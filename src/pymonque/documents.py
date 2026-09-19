"""Typed documents in MongoDB: the Document base, the collection engine every storage kind is built
on, and the conventions stored documents share."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, ClassVar, Generic, Iterable, Literal, Mapping, Self, Sequence, TypeVar

from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer, PrivateAttr
from pymongo import IndexModel
from pymongo.collection import Collection

from .exceptions import UnboundDocument


logger = logging.getLogger("pymonque")

# how far the database server's clock is ahead of this host's; kept by syncClock()
_serverOffset = timedelta(0)

# an offset this large says this host's clock is wrong, and is logged
CLOCK_WARN_AFTER = timedelta(seconds=1)


def utc_now() -> datetime:
    """Now, as naive UTC — what MongoDB stores and hands back — by the database server's clock.

    Every process compares leases, deadlines and heartbeats written by every other process, so they
    must share one clock: the server's. A host whose own clock runs ahead would otherwise take over
    work another still holds. The offset is measured by syncClock(); until then it is this host's own.
    """

    return _localNow() + _serverOffset


def _localNow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def syncClock(db: Any) -> timedelta | None:
    """Measure how far the database server's clock is from this host's, and have utc_now() follow the
    server. Returns the offset, or None if the server would not say (it is then left as it was).

    Reads `hello`'s localTime, corrected by half the round trip, so it is off by at most a few
    milliseconds. One offset per process: an app's processes share one database.
    """

    global _serverOffset

    before = _localNow()

    try:
        reply = db.command("hello")
    except Exception as e:
        logger.debug("could not read the database server's clock, so this host's is used: %r", e)
        return None

    after = _localNow()
    serverTime = reply.get("localTime") if isinstance(reply, Mapping) else None

    if not isinstance(serverTime, datetime):
        logger.debug("the database server did not report its clock, so this host's is used")
        return None

    if serverTime.tzinfo is not None:
        serverTime = serverTime.astimezone(timezone.utc).replace(tzinfo=None)

    offset = serverTime - (before + (after - before) / 2)

    if abs(offset) >= CLOCK_WARN_AFTER and abs(offset - _serverOffset) >= CLOCK_WARN_AFTER:
        logger.warning(
            "this host's clock is %.1fs %s the database server's; pymonque keeps time by the server's",
            abs(offset.total_seconds()), "behind" if offset > timedelta(0) else "ahead of"
        )

    _serverOffset = offset

    return offset


def uuid4str() -> str:
    return str(uuid.uuid4())


def _naiveUtc(value: datetime) -> datetime:
    # MongoDB hands datetimes back naive, so an aware one kept in memory could not even be
    # compared with one that was stored
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    return value

UtcDatetime = Annotated[datetime, AfterValidator(_naiveUtc)]


def _seconds(value: Any) -> Any:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return timedelta(seconds=value)

    if isinstance(value, str):
        try:
            return timedelta(seconds=float(value))
        except ValueError:
            return value    # not a number of seconds; pydantic says what is wrong with it

    return value

# a duration, stored as seconds so it can be queried and summed
Duration = Annotated[
    timedelta,
    BeforeValidator(_seconds),
    PlainSerializer(lambda value: value.total_seconds(), return_type=float),
]

# the statuses tasks and pile items share; tasks add their own
WorkStatus = Literal["pending", "running", "done", "failed", "canceled"]

# aliases are how a model matches an existing schema, so documents are stored under them
MONGO_CONFIG = ConfigDict(serialize_by_alias=True, validate_by_name=True, validate_by_alias=True)


class Document(BaseModel):
    """A model stored in a collection.

    A document handed back by an engine remembers the engine and the key it was stored under, so it
    can save, delete and reload itself — and changing its key renames it rather than leaving a copy
    behind. One built by hand is unbound until it is stored.
    """

    model_config = MONGO_CONFIG

    # the fields the engine storing this keeps for itself: never given, updated or saved through the
    # default API, only written by the engine's own work. None for a plain document; a task, a
    # scheduler and a pile item name theirs, and a subclass inherits them
    _kept: ClassVar[frozenset[str]] = frozenset()

    uid: str = Field(default_factory=uuid4str)

    _engine: Any = PrivateAttr(default=None)
    _storedKey: Any = PrivateAttr(default=None)

    def _bind(self, engine: CollectionEngine, stored: bool) -> Self:
        self._engine = engine

        if stored:
            self._storedKey = getattr(self, engine.key)

        return self

    @property
    def bound(self) -> bool:
        return self._engine is not None

    @property
    def storedKey(self) -> Any:
        """The key this document was last written under, or None if it has not been."""

        return self._storedKey

    def _requireEngine(self) -> CollectionEngine:
        if self._engine is None:
            raise UnboundDocument(
                f"{type(self).__name__} is not attached to a collection — insert it through an "
                f"engine, or fetch it from one, before saving, deleting or reloading it"
            )

        return self._engine

    def save(self) -> Self:
        return self._requireEngine().save(self)

    def delete(self) -> bool:
        engine = self._requireEngine()
        deleted = engine.delete(engine._rowKey(self))
        self._storedKey = None      # gone: saving it again stores it afresh

        return deleted

    def reload(self) -> Self | None:
        engine = self._requireEngine()

        return engine.get(engine._rowKey(self))


M = TypeVar("M", bound=Document)


class CollectionEngine(Generic[M]):
    """Typed storage for one collection of documents.

    The base every other engine is built on: it knows a model, a collection and the key documents
    are stored under, and nothing about tasks, schedules or work.
    """

    def __init__(
            self,
            collection:     Collection,
            model:          type[M],
            *,
            name:           str,
            key:            str = "uid",
            extraIndexes:   Sequence[IndexModel] | None = None
        ):

        self.collection = collection
        self.model: type[M] = model
        self.name = name
        self.key = key
        self.extraIndexes: list[IndexModel] = list(extraIndexes or ())

        # queries match the name the key is stored under, which an alias changes
        field = model.model_fields.get(key)
        byAlias = field is not None and model.model_config.get("serialize_by_alias")
        self.keyField: str = (field.serialization_alias or field.alias or key) if byAlias else key

        self.createIndexes()

    def createIndexes(self):
        self.collection.create_index([(self.keyField, 1)], unique=True)

        if self.extraIndexes:
            self.collection.create_indexes(self.extraIndexes)

    # --- reading ---

    def _load(self, raw: Mapping[str, Any]) -> M:
        # MongoDB's own _id is not part of the model, and a model that forbids extras would refuse it
        fields = {name: value for name, value in raw.items() if name != "_id"}

        return self.model.model_validate(fields)._bind(self, stored=True)

    def get(self, key: Any) -> M | None:
        raw = self.collection.find_one({self.keyField: key})

        return self._load(raw) if raw else None

    def findOne(self, where: Mapping[str, Any] | None = None) -> M | None:
        raw = self.collection.find_one(dict(where or {}))

        return self._load(raw) if raw else None

    def find(
            self,
            where:  Mapping[str, Any] | None = None,
            sort:   Sequence[tuple[str, int]] | None = None,
            limit:  int | None = None
        ) -> list[M]:

        cursor = self.collection.find(dict(where or {}))

        if sort:
            cursor = cursor.sort(list(sort))

        if limit:
            cursor = cursor.limit(limit)

        return [self._load(raw) for raw in cursor]

    def count(self, where: Mapping[str, Any] | None = None) -> int:
        return self.collection.count_documents(dict(where or {}))

    def exists(self, key: Any) -> bool:
        return self.collection.count_documents({self.keyField: key}, limit=1) > 0

    # --- the fields the engine keeps ---

    # why a kept field is refused, where a better word than "the engine keeps it" helps
    _keptHints: ClassVar[dict[str, str]] = {}

    def _refuseKept(self, fields: Mapping[str, Any]):
        """Refuse a field the model says the engine keeps, by its name or its alias."""

        labels = {
            label: name
            for name in self.model._kept
            for label in (name, getattr(self.model.model_fields.get(name), "alias", None)) if label
        }

        for given in fields:
            name = labels.get(given)

            if name is not None:
                raise TypeError(self._keptHints.get(name, f"{name} is not set through {self.name}'s API; the engine keeps it"))

    def _keepStored(self, document: M, stored: M) -> M:
        """Put the stored values of the kept fields back on a document about to be saved over them."""

        for name in self.model._kept:
            setattr(document, name, getattr(stored, name))

        return document

    def _keepFresh(self, document: M) -> M:
        """Give a document saved for the first time the kept fields a new one starts with — its key
        aside — whatever this copy carries, as if it had been built with none of them."""

        fresh = self.model.model_validate(document.model_dump(exclude=self.model._kept - {self.key}))

        for name in self.model._kept - {self.key}:
            setattr(document, name, getattr(fresh, name))

        return document

    # --- writing ---

    def _prepare(self, document: M) -> M:
        """Keep derived fields in step before a document is written. Nothing to do for plain storage."""

        return document

    def _rowKey(self, document: M) -> Any:
        """The key of the row a document stands for: the one it was stored under here, or its own."""

        if document._engine is self and document.storedKey is not None:
            return document.storedKey

        return getattr(document, self.key)

    def build(self, **fields: Any) -> M:
        """A document of this engine's model, bound to it but not stored. The fields the engine keeps
        are refused."""

        self._refuseKept(fields)

        return self.model(**fields)._bind(self, stored=False)

    def create(self, **fields: Any) -> M:
        """Build a document, store it, and hand it back bound. The fields the engine keeps are refused."""

        self._refuseKept(fields)

        return self.insert(self.model(**fields))

    def insert(self, document: M) -> M:
        self.collection.insert_one(self._prepare(document).model_dump())

        return document._bind(self, stored=True)

    def insertMany(self, documents: Iterable[M]) -> list[M]:
        documents = list(documents)

        if documents:
            self.collection.insert_many([self._prepare(d).model_dump() for d in documents])

        return [d._bind(self, stored=True) for d in documents]

    def save(self, document: M) -> M:
        """Store the document as it is now, creating it if it is not there yet.

        A document that came from here is written over the row it came from, so changing its key
        renames it rather than leaving a copy behind. Over a stored document, the fields the engine
        keeps stay as stored, whatever this copy carries: a copy read before a worker finished cannot
        put the work back, or wipe the claim holding it.
        """

        if self.model._kept:
            stored = self.collection.find_one({self.keyField: self._rowKey(document)})

            if stored is not None:
                self._keepStored(document, self._load(stored))
            else:
                self._keepFresh(document)

        self.collection.replace_one(
            {self.keyField: self._rowKey(document)},
            self._prepare(document).model_dump(),
            upsert=True
        )

        return document._bind(self, stored=True)

    def _assign(self, document: M, fields: Mapping[str, Any]) -> M:
        """Set fields on a document, each validated against the model, so nothing invalid is written."""

        for name, value in fields.items():
            document.__pydantic_validator__.validate_assignment(document, name, value)

        return document

    def update(self, key: Any, **fields: Any) -> M | None:
        """Merge fields into a stored document and return it, or None if there is no such document.

        The fields are validated against the model first, so nothing invalid is written, and only
        what changed is written: the fields given, and anything kept in step with them. Changing the
        key renames the document. The fields the engine keeps are refused.
        """

        self._refuseKept(fields)
        document = self.get(key)

        if document is None:
            return None

        before = document.model_dump()
        after = self._prepare(self._assign(document, fields)).model_dump()
        changed = {name: value for name, value in after.items() if name not in before or before[name] != value}

        if changed:
            self.collection.update_one({self.keyField: key}, {"$set": changed})

        return self.get(after[self.keyField])

    def delete(self, key: Any) -> bool:
        return self.collection.delete_one({self.keyField: key}).deleted_count > 0

    def deleteMany(self, where: Mapping[str, Any]) -> int:
        return self.collection.delete_many(dict(where)).deleted_count

    def __repr__(self) -> str:
        return f"{type(self).__name__} {self.name} ({self.collection.name})"
