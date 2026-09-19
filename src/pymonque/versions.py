"""One version of the code at a time: the fingerprint of an app's shared behaviour, and the registry of
worker processes that checks it.

Two processes whose fingerprints differ are running different code, and must not work the same
collections. The fingerprint cannot see a function's body — nothing can, reliably — so it is a guard,
not a proof: deploy one version at a time.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import re
import socket
from datetime import timedelta
from typing import Any, Callable, Iterable, Mapping

from pydantic import BaseModel
from pymongo.collection import Collection

from .claims import durable
from .documents import utc_now
from .exceptions import VersionMismatch


logger = logging.getLogger("pymonque")

HOSTNAME = socket.gethostname()

HEARTBEAT_INTERVAL = 15     # seconds between a worker process checking in
WORKER_STALE_AFTER = 60     # after this long without checking in, a worker process is gone

# a default like object() prints its address, which differs per process
_ADDRESS = re.compile(r" at 0x[0-9a-fA-F]+")


def signatureOf(name: str, func: Callable) -> str:
    """A function's name and signature, defaults included: a call stores only the arguments given, so a
    changed default changes what runs as much as a changed type does."""

    return _ADDRESS.sub("", f"{name}{inspect.signature(func)}")


def schemaOf(model: type[BaseModel] | None) -> str:
    """What a stored document looks like to a model, so a changed field refuses a mismatched worker."""

    if model is None:
        return "any dict"

    try:
        return json.dumps(model.model_json_schema(), sort_keys=True, default=str)
    except Exception:
        # a field of a type with no JSON schema: its annotation and default still say what it takes
        fields = (f"{name}: {field.annotation!r} = {field.default!r}" for name, field in model.model_fields.items())

        return _ADDRESS.sub("", f"{model.__name__}({'; '.join(fields)})")


def fingerprint(parts: Iterable[str]) -> str:
    return hashlib.sha1("\n".join(parts).encode()).hexdigest()[:12]


class Registry:
    """Worker processes that checked in, in one collection every process of an app shares.

    Every process that starts workers checks in with its fingerprint and keeps checking in until its
    work in flight has drained, so another process can count its workers and refuse to run beside it
    if their versions differ. A process that stops checking in goes stale, and stops counting.
    """

    def __init__(self, collection: Collection, *, uid: str, fingerprint: str, staleAfter: float = WORKER_STALE_AFTER):
        self.collection = durable(collection)     # a version check must not read a stale registry
        self.uid = uid
        self.fingerprint = fingerprint
        self.staleAfter = staleAfter

        # constructing an app writes nothing but indexes
        self.collection.create_index([("uid", 1)], unique=True)
        self.collection.create_index([("lastSeen", 1)])

    def live(self) -> list[dict[str, Any]]:
        """Worker processes that have checked in recently."""

        return list(self.collection.find(
            {"lastSeen": {"$gte": utc_now() - timedelta(seconds=self.staleAfter)}},
            {"_id": 0},
        ))

    def verify(self):
        """Raise VersionMismatch if a live worker process is running a different version."""

        conflict = next((w for w in self.live() if w.get("fingerprint") != self.fingerprint), None)

        if conflict is not None:
            raise VersionMismatch(
                f"a live worker is running a different version of this app "
                f"({conflict.get('fingerprint')} on {conflict.get('host')}:{conflict.get('pid')}, "
                f"this process is {self.fingerprint}). Only one version may run at a time — "
                f"stop the old workers before starting these."
            )

    def register(self, report: Mapping[str, Any]):
        """Check this process in, with what it reports about itself."""

        now = utc_now()

        self.collection.update_one(
            {"uid": self.uid},
            {"$set": {
                "uid":          self.uid,
                "fingerprint":  self.fingerprint,
                "host":         HOSTNAME,
                "pid":          os.getpid(),
                "startedAt":    now,
                "lastSeen":     now,
                **report,
            }},
            upsert=True,
        )

    def beat(self, report: Mapping[str, Any]):
        """Check in again, updating what this process reports."""

        self.collection.update_one({"uid": self.uid}, {"$set": {"lastSeen": utc_now(), **report}})

    def forgetGone(self, olderThan: timedelta) -> int:
        """Delete the records of worker processes that stopped checking in more than `olderThan` ago —
        killed ones; a process that stops cleanly removes its own. Returns how many."""

        return self.collection.delete_many({"lastSeen": {"$lt": utc_now() - olderThan}}).deleted_count

    def deregister(self):
        """Free this process's slot at once, rather than waiting for it to go stale."""

        try:
            self.collection.delete_one({"uid": self.uid})
        except Exception:
            logger.exception("could not deregister worker process %s", self.uid)
