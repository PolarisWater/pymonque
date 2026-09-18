"""Upgrading a database a 2.0 app ran against.

Run once, by an operator, with every 2.0 process stopped — workers, and processes that only enqueue:

    from pymonque import upgradeFrom2

    print(upgradeFrom2(App(db)))

It is explicit rather than part of starting workers, because it renames collections: a 2.0 process
that only enqueues never registers, so nothing could tell it was still writing to the old ones. It is
safe to run again; a second run changes nothing.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .exceptions import VersionMismatch

if TYPE_CHECKING:
    from .app import BaseApp


logger = logging.getLogger("pymonque")

# 2.0's words for what this version calls done and running
TASK_STATUSES = {"success": "done", "processing": "running"}
ITEM_STATUSES = {"claimed": "running"}


def upgradeFrom2(app: BaseApp, *, tasks: str = "pymonque_tasks", schedulers: str = "pymonque_schedulers") -> dict[str, int]:
    """Bring the documents of a 2.0 app up to this version. Returns how many of each were changed.

    - `pymonque_tasks` and `pymonque_schedulers` (or the names given, for collections 2.0 was told to
      use) are renamed to the collections of the default engines, `app.task` and `app.scheduler`, and
      a declared scheduler engine's `pymonque_schedulers_<name>` to its `pymonque_scheduler_<name>`. A
      target that already holds documents is refused: merging two queues is not an upgrade.
    - Task statuses `success` and `processing` become `done` and `running`, on every task engine; a
      task left `processing` by a stopped worker is then written off by the next claim, as a task
      whose worker died. Tasks lose 2.0's `attempts`: tasks are not retried. A task 2.0 held back
      for a retry is still pending, and runs once more.
    - Item status `claimed` becomes `running`, on every pile.

    Refused while any worker process is registered live, of any version.
    """

    live = app.registry.live()

    if live:
        where = ", ".join(f"{w.get('host')}:{w.get('pid')} ({w.get('fingerprint')})" for w in live)

        raise VersionMismatch(f"stop every worker process before upgrading; still live: {where}")

    renamed = _rename(app, schedulers, app.scheduler)

    # a declared scheduler engine's 2.0 default was pymonque_schedulers_<name>; one given a collection
    # of its own keeps it
    for name, engine in app.schedulerEngines.items():
        if name != "scheduler" and engine.collection.name == f"pymonque_scheduler_{name}":
            renamed += _rename(app, f"pymonque_schedulers_{name}", engine)

    changed = {
        "tasks renamed":        _rename(app, tasks, app.task),
        "schedulers renamed":   renamed,
        "task statuses":        0,
        "task attempts":        0,
        "item statuses":        0,
    }

    for engine in app.taskEngines.values():
        for old, new in TASK_STATUSES.items():
            changed["task statuses"] += engine.collection.update_many({"status": old}, {"$set": {"status": new}}).modified_count

        changed["task attempts"] += engine.collection.update_many({"attempts": {"$exists": True}}, {"$unset": {"attempts": ""}}).modified_count

    for engine in app.piles.values():
        for old, new in ITEM_STATUSES.items():
            changed["item statuses"] += engine.collection.update_many({"status": old}, {"$set": {"status": new}}).modified_count

    logger.warning("upgraded from 2.0: %s", changed)

    return changed


def _rename(app: BaseApp, old: str, engine) -> int:
    """Rename a 2.0 collection to an engine's, and give it the engine's indexes. Returns how many
    documents moved: none if there was nothing to move."""

    if old == engine.collection.name or old not in app.db.list_collection_names():
        return 0

    moving = app.db[old].count_documents({})
    waiting = engine.collection.count_documents({})

    if waiting:
        raise ValueError(
            f"{old} cannot become {engine.collection.name}, which already holds {waiting} document(s); "
            f"move them, or upgrade before this version has written anything"
        )

    # the target exists only with the indexes constructing the app made, so it is dropped with the rename
    app.db[old].rename(engine.collection.name, dropTarget=True)
    engine.createIndexes()

    return moving
