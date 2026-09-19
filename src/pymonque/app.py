"""BaseApp: an app's documents and work, declared in one class, and the processes that run it.

    class App(BaseApp):
        distributions = MyDistributions

        groups  = collection(Group, key="groupId")
        heavy   = tasks(RenderTask, leaseSeconds=900)
        nightly = schedulers(emitsInto=heavy, missed="skip")
        outbox  = pile(Email, maxAttempts=3)

        @task(timeout=60)
        def sync(self, accountId: int): ...

    app = App(db)                   # any process: enqueue, query — writes nothing but indexes
    app.run(taskWorkers=4)          # a worker process: housekeeping, workers, until a signal

Shared behaviour — declarations, defaults, the distribution registry — lives on the class, so every
process that imports it agrees; per-process pacing is passed to the constructor.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from datetime import timedelta
from typing import Annotated, Any, Callable, ClassVar, Mapping, Sequence

from pydantic import Field
from pymongo.database import Database

from .calls import nearestAttributes
from .declarations import Declaration, collection, declaredOn, pile, schedulers, task, tasks
from .declarations import task as _task     # `task` is the default task engine inside BaseApp's body
from .distributions import BaseDistributions, DistributionEngine
from .documents import CollectionEngine, syncClock, uuid4str
from .piles import PileEngine
from .schedulers import SchedulerEngine
from .settings import LEASE_SECONDS, UNSET, AppDefaults, orDefault
from .tasks import TaskEngine, taskFunctions
from .versions import HEARTBEAT_INTERVAL, WORKER_STALE_AFTER, Registry, fingerprint, schemaOf, signatureOf


logger = logging.getLogger("pymonque")

BACKLOG_WARN_AFTER = 60     # seconds work may sit due before the app says nobody is free to take it
BACKLOG_INTERVAL = 30       # seconds between those checks
SHORT_LEASE = 10            # seconds; a shorter lease is lost to one slow write or stall, and is logged

WorkerCounts = int | Mapping[str, int]


class BaseApp:
    """An app's documents and work. Subclass it and declare them; construct it in every process.

    Constructing an app never writes to shared state beyond creating indexes, so any process may do it
    at any time. Housekeeping belongs to a process that starts workers: `startWorkers()` or `run()`.
    """

    # the interval functions schedulers draw from; a BaseDistributions subclass adds your own
    distributions: ClassVar[type[BaseDistributions]] = BaseDistributions

    # the default engines, named after their attributes; a declaration of the same name replaces one and
    # takes over its collection. Declared here, so replacing one is what replacing any declaration is.
    task = tasks()
    scheduler = schedulers()

    # Defaults, for every declaration that leaves the setting out. Declared in code like what they
    # apply to: every process must agree on them, so they are part of the fingerprint.
    taskTimeout:            float | None    = None          # seconds a call may run; None: no limit
    taskSkipAfter:          float | None    = None          # seconds past its deadline still worth running
    taskLeaseSeconds:       float           = LEASE_SECONDS
    schedulerMissed:        str             = "once"        # skip, once or replay
    schedulerLeaseSeconds:  float           = LEASE_SECONDS
    pileMaxAttempts:        int             = 1
    pileLeaseSeconds:       float           = LEASE_SECONDS

    # set by __init__, so a declaration under one of these names would be shadowed
    _INSTANCE_ATTRIBUTES: ClassVar[frozenset[str]] = frozenset({
        "db", "defaults", "distribution", "functions", "limits", "fingerprint",
        "collections", "piles", "taskEngines", "schedulerEngines",
        "enforceVersion", "heartbeatInterval", "backlogWarnAfter", "backlogInterval", "retireAfter",
        "workerUid", "registry", "retired",
    })

    # the default engines, the one sanctioned replacement: each by a declaration of its own kind
    _REPLACEABLE: ClassVar[dict[str, type]] = {"task": tasks, "scheduler": schedulers}

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)

        cls._refuseReserved()

        # checked where they are written, like every declaration
        AppDefaults.of(cls)

        registry = cls.distributions

        if not (isinstance(registry, type) and issubclass(registry, BaseDistributions)):
            raise TypeError(f"{cls.__name__}.distributions must be a BaseDistributions subclass, not {registry!r}")

        cls._refuseLostTargets()

    @classmethod
    def _refuseReserved(cls):
        """Refuse a declaration that would replace part of the app itself: a pile named `backlog` would
        break the backlog check, a task named `init` the startup."""

        reserved = (set(dir(BaseApp)) | BaseApp._INSTANCE_ATTRIBUTES) - {"__doc__", "__module__"}

        for name, value in cls.__dict__.items():
            if not isinstance(value, (Declaration, task)) or name not in reserved:
                continue

            if isinstance(value, cls._REPLACEABLE.get(name, ())):
                continue

            what = f"a {cls._REPLACEABLE[name].__name__}(...) declaration" if name in cls._REPLACEABLE else "another name"

            raise TypeError(f"{cls.__name__}.{name} would replace BaseApp.{name}; declare it as {what}")

    @classmethod
    def _refuseLostTargets(cls):
        """A scheduler engine emits into the task engine its reference names, followed by name: a
        subclass that replaces that engine replaces it for the schedulers too. Replaced by something
        that is not a task engine, it is refused here rather than when the app is built."""

        engines = declaredOn(cls, tasks)

        for name, declared in declaredOn(cls, schedulers).items():
            target = cls._emitsInto(declared)

            if target not in engines:
                raise TypeError(
                    f"{cls.__name__}.{name} emits into {target}, which {cls.__name__} replaced with "
                    f"{nearestAttributes(cls).get(target)!r}, not a task engine"
                )

    @staticmethod
    def _emitsInto(declared: schedulers) -> str:
        return "task" if declared.emitsInto is UNSET else declared.emitsInto.name

    def __init__(
            self,
            db:                     Database,
            *,
            taskPollInterval:       float = 1.0,
            schedulerPollInterval:  float = 1.0,
            enforceVersion:         bool = True,
            heartbeatInterval:      float = HEARTBEAT_INTERVAL,
            workerStaleAfter:       float = WORKER_STALE_AFTER,
            backlogWarnAfter:       float | None = BACKLOG_WARN_AFTER,
            backlogInterval:        float = BACKLOG_INTERVAL,
            retireAfter:            int | None = None
        ):

        """Build every part the class declares, against `db`.

        Everything here is per-process pacing and diagnostics, which processes may set differently:
        poll intervals (a declaration's own `pollInterval` wins), the heartbeat, the backlog warning
        (None turns it off), and `retireAfter` — the number of abandoned threads at which a worker
        process stops claiming, drains, and leaves `run()` for its supervisor to restart it.
        """

        cls = type(self)

        if retireAfter is not None and retireAfter < 1:
            raise ValueError(f"retireAfter is a number of abandoned threads, at least 1, or None; not {retireAfter!r}")

        self.db = db

        # every process keeps time by the database server's clock, so leases compare across hosts; a
        # read, like creating indexes, and safe from any process
        syncClock(db)

        self.defaults = AppDefaults.of(cls)
        self.distribution = DistributionEngine(cls.distributions)

        declaredTasks = declaredOn(cls, task)

        # tasks belong to no engine: every task engine runs any of them, bound to this app
        self.functions = taskFunctions({name: getattr(self, name) for name in declaredTasks})
        self.limits = {name: declared.limitsWith(self.defaults) for name, declared in declaredTasks.items()}

        # storage first, so a task reaches a collection or a pile through its app
        self.collections: dict[str, CollectionEngine] = {
            name: CollectionEngine(declared.collectionIn(db), declared.model, name=name, key=declared.key, extraIndexes=declared.extraIndexes)
            for name, declared in declaredOn(cls, collection).items()
        }

        self.piles: dict[str, PileEngine] = {
            name: PileEngine(
                declared.collectionIn(db), declared.model, name=name,
                settings=declared.settingsWith(self.defaults), extraIndexes=declared.extraIndexes,
            )
            for name, declared in declaredOn(cls, pile).items()
        }

        self.taskEngines: dict[str, TaskEngine] = {
            name: TaskEngine(
                declared.collectionIn(db), declared.model, name=name,
                functions=self.functions,
                settings=declared.settingsWith(self.defaults),
                limits=self.limits,
                distributions=self.distribution,
                extraIndexes=declared.extraIndexes,
                pollInterval=orDefault(declared.pollInterval, taskPollInterval),
            )
            for name, declared in declaredOn(cls, tasks).items()
        }

        self.schedulerEngines: dict[str, SchedulerEngine] = {
            name: SchedulerEngine(
                declared.collectionIn(db), declared.model, name=name,
                settings=declared.settingsWith(self.defaults),
                tasks=self.taskEngines[cls._emitsInto(declared)],
                extraIndexes=declared.extraIndexes,
                pollInterval=orDefault(declared.pollInterval, schedulerPollInterval),
            )
            for name, declared in declaredOn(cls, schedulers).items()
        }

        # each part under the name it was declared as; the class keeps the declaration
        for parts in (self.collections, self.piles, self.taskEngines, self.schedulerEngines):
            for name, part in parts.items():
                setattr(self, name, part)

        self.enforceVersion = enforceVersion
        self.heartbeatInterval = heartbeatInterval
        self.backlogWarnAfter = backlogWarnAfter
        self.backlogInterval = backlogInterval
        self.retireAfter = retireAfter
        self.retired = False

        self._warnShortLeases()

        self.fingerprint = fingerprint(self._surface())
        self.workerUid = uuid4str()
        self.registry = Registry(db["pymonque_workers"], uid=self.workerUid, fingerprint=self.fingerprint, staleAfter=workerStaleAfter)

        for engine in self.taskEngines.values():
            engine.onAbandoned = self._onAbandoned

        self._stopping = False
        self._quit = threading.Event()      # what the monitor threads wait on
        self._monitors: dict[str, threading.Thread] = {}
        self._previousHandlers: dict[int, Any] = {}
        self._retiring = threading.Lock()

        # NB: init() is deliberately not called here. Constructing an app must be safe from any process
        # at any time; housekeeping belongs to a process that is taking over as a worker.

    def _warnShortLeases(self):
        """Say so when a lease is short enough to be lost without anyone dying.

        A lease is renewed every third of its length, so a lease of L seconds is taken over after a
        stall of about 2L/3 — a slow write, a paused VM, a thread held by a long C call. At a few
        seconds that happens in ordinary operation, and live work is written off or worked twice.
        """

        parts = {**self.taskEngines, **self.schedulerEngines, **self.piles}

        for name, part in parts.items():
            lease = part.settings.leaseSeconds

            if lease < SHORT_LEASE:
                logger.warning(
                    "%s has a lease of %gs: a stall of about %.1fs — one slow write, a paused VM — hands its "
                    "live work to another worker. Leases under %ds are for tests.",
                    name, lease, lease * 2 / 3, SHORT_LEASE
                )

    # --- one version at a time ---

    def _surface(self) -> list[str]:
        """What every process running this app must agree on: each task's signature and resolved limits,
        each distribution's signature, and each part's collection, model and shared settings.

        Settings are in here because they are shared behaviour: one process giving up an item another
        would claim again, or taking over work another still holds, is a split brain over the same
        documents, not two harmless local choices.
        """

        parts = [
            f"task {signatureOf(name, func)} {self.limits[name].model_dump()}"
            for name, func in sorted(self.functions.functions.items())
        ]

        parts += [f"distribution {signatureOf(name, func)}" for name, func in sorted(self.distribution.functions.functions.items())]

        parts += [
            f"tasks {name} {engine.collection.name} {engine.settings.model_dump()} {schemaOf(engine.model)}"
            for name, engine in sorted(self.taskEngines.items())
        ]

        parts += [
            f"schedulers {name} {engine.collection.name} emitsInto={engine.tasks.name} {engine.settings.model_dump()} {schemaOf(engine.model)}"
            for name, engine in sorted(self.schedulerEngines.items())
        ]

        parts += [
            f"pile {name} {engine.collection.name} {engine.settings.model_dump()} {schemaOf(engine.payload)}"
            for name, engine in sorted(self.piles.items())
        ]

        parts += [
            f"collection {name} {engine.collection.name} key={engine.key} {schemaOf(engine.model)}"
            for name, engine in sorted(self.collections.items())
        ]

        return parts

    def liveWorkers(self) -> list[dict[str, Any]]:
        """Worker processes of this app that have checked in recently, whatever their version."""

        return self.registry.live()

    def taskWorkers(self) -> dict[str, int]:
        """Task worker threads on each task engine, across every process that has checked in.

        A process running no task workers of its own must not report that nobody is working.
        """

        counts = dict.fromkeys(self.taskEngines, 0)

        for worker in self.liveWorkers():
            byEngine = worker.get("taskWorkers")

            # a 2.0 process reports one number, for an engine this version cannot name
            if isinstance(byEngine, dict):
                for name, count in byEngine.items():
                    counts[name] = counts.get(name, 0) + count

        return counts

    def init(self):
        """Housekeeping, before this process starts workers: on every task engine — whether or not this
        process works it, so an engine nobody works is still cleaned — waiting tasks whose function is
        gone are flagged incompatible, and tasks a dead worker left running whose function is gone are
        written off, since no claim would ever take them.

        It decides from this process's tasks, so it is refused while a live worker runs another version.
        """

        if self.enforceVersion:
            self.registry.verify()

        for engine in self.taskEngines.values():
            flagged = engine.flagIncompatible()
            stuck = engine.writeOffStuck()

            if flagged or stuck:
                logger.warning(
                    "%r: %d waiting task(s) flagged incompatible and %d left running written off, "
                    "their function gone from this app", engine, flagged, stuck
                )

    # --- keeping the history in bounds ---

    @_task(timeout=None, skipAfter=None)     # its own limits: an app's short taskTimeout must not cut a big cleanup short
    def cleanupFinished(self, days: Annotated[float, Field(ge=0)] = 30) -> dict[str, int]:
        """Delete what finished more than `days` ago: tasks on every task engine, items on every pile,
        and the records of worker processes gone that long. Returns how many of each.

        Finished work stays until then, as the history of what ran. A task like any other, so it runs
        on a scheduler — once a day is plenty:

            app.scheduler.ensure("cleanup", App.cleanupFinished(days=30), app.distribution("constant", dailyFrequency=1))
        """

        age = timedelta(days=days)

        return {
            "tasks":    sum(engine.purge(age) for engine in self.taskEngines.values()),
            "items":    sum(engine.purge(("done", "failed", "canceled"), olderThan=age) for engine in self.piles.values()),
            "workers":  self.registry.forgetGone(max(age, timedelta(seconds=self.registry.staleAfter))),
        }

    # --- workers ---

    @property
    def engines(self) -> list[TaskEngine | SchedulerEngine]:
        """Every engine that runs workers."""

        return [*self.taskEngines.values(), *self.schedulerEngines.values()]

    @staticmethod
    def _counts(setting: str, given: WorkerCounts, engines: Mapping[str, Any]) -> dict[str, int]:
        """An int is that many threads on every engine of the kind; a dict sets engines by name."""

        if isinstance(given, bool) or not isinstance(given, (int, Mapping)):
            raise TypeError(f"{setting} is a number of threads per engine, or a dict of engine name to one; not {given!r}")

        counts = dict.fromkeys(engines, given) if isinstance(given, int) else dict(given)
        unknown = sorted(set(counts) - set(engines))

        if unknown:
            raise ValueError(f"{setting} names {', '.join(unknown)}, which this app has no engine of; it has {', '.join(engines)}")

        for name, count in counts.items():
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ValueError(f"{setting}[{name!r}] is a number of threads, not {count!r}")

        return counts

    def startWorkers(self, taskWorkers: WorkerCounts = 0, schedulerWorkers: WorkerCounts = 0):
        """Start worker threads: `taskWorkers` on the task engines and `schedulerWorkers` on the scheduler
        engines, each an int for every engine of its kind or a dict of engine name to count.

        Runs housekeeping first, and refuses if a live worker process runs another version.
        """

        taskCounts = self._counts("taskWorkers", taskWorkers, self.taskEngines)
        schedulerCounts = self._counts("schedulerWorkers", schedulerWorkers, self.schedulerEngines)

        self._stopping = False
        self._quit.clear()

        self.init()     # refuses here if a live worker is running other code
        self.registry.register(self._report())
        self._monitor("heartbeat", self._heartbeat)

        for name, count in taskCounts.items():
            self.taskEngines[name].workers.start(count)

        for name, count in schedulerCounts.items():
            self.schedulerEngines[name].workers.start(count)

        self.registry.beat(self._report())

        if self.backlogWarnAfter is not None:
            self._checkBacklog()    # say it now if the backlog is already old
            self._monitor("backlog", self._watchBacklog)

    def _report(self) -> dict[str, Any]:
        """What this process tells the others on its heartbeat."""

        return {
            "taskWorkers":      {name: engine.workers.count for name, engine in self.taskEngines.items()},
            "schedulerWorkers": {name: engine.workers.count for name, engine in self.schedulerEngines.items()},
            "abandonedThreads": sum(len(engine.abandoned()) for engine in self.taskEngines.values()),
        }

    def _monitor(self, name: str, target: Callable[[], None]):
        """Start one background loop, once: a second startWorkers() must not leave a second one behind."""

        running = self._monitors.get(name)

        if running is not None and running.is_alive():
            return

        thread = threading.Thread(target=target, name=f"pymonque-{name}", daemon=True)
        self._monitors[name] = thread
        thread.start()

    def _heartbeat(self):
        while not self._quit.wait(self.heartbeatInterval):
            syncClock(self.db)      # clocks drift; a long-running worker keeps following the server's

            try:
                self.registry.beat(self._report())
            except Exception:
                logger.exception("worker heartbeat failed")

    # --- is anyone keeping up ---

    def backlog(self) -> dict[str, tuple[int, float]]:
        """For each task engine: tasks due that nobody has picked up, and how long the oldest has waited."""

        return {name: engine.backlog() for name, engine in self.taskEngines.items()}

    def _checkBacklog(self):
        """Say plainly, engine by engine, when work is due and nobody is free to take it."""

        try:
            backlog = self.backlog()
            workers = self.taskWorkers()
        except Exception:
            logger.exception("backlog check failed")
            return

        for name, (due, waiting) in backlog.items():
            if not due or waiting < self.backlogWarnAfter:
                continue

            if workers.get(name):
                logger.warning(
                    "%s: %d task(s) due, oldest waiting %.0fs, %d worker(s) on it across processes — "
                    "not enough workers, or they are all on long tasks", name, due, waiting, workers[name]
                )
            else:
                logger.warning(
                    "%s: %d task(s) due, oldest waiting %.0fs, and no process runs workers on it",
                    name, due, waiting
                )

    def _watchBacklog(self):
        while not self._quit.wait(self.backlogInterval):
            self._checkBacklog()

    # --- threads a timeout could not stop ---

    def abandoned(self) -> list:
        """Calls that outlived their timeout and would not stop, on every task engine, oldest first."""

        return sorted((each for engine in self.taskEngines.values() for each in engine.abandoned()), key=lambda each: each.since)

    def _onAbandoned(self, engine: TaskEngine):
        """A thread was abandoned: publish the count, and retire the process if it has too many."""

        try:
            self.registry.beat(self._report())
        except Exception:
            logger.exception("worker heartbeat failed")

        abandoned = self.abandoned()

        if self.retireAfter is None or len(abandoned) < self.retireAfter:
            return

        with self._retiring:
            if self.retired:
                return

            self.retired = True

        stuck = "\n".join(f"{each!r}, stuck at:\n{each.where()}" for each in abandoned)

        logger.error(
            "%d abandoned thread(s), at the limit of %d, so this process retires. Abandoned:\n%s",
            len(abandoned), self.retireAfter, stuck
        )
        logger.error("no longer claiming, draining %d", sum(engine.workers.busy for engine in self.engines))

        self.requestStop()

    # --- shutting down ---

    @property
    def running(self) -> bool:
        return any(engine.workers.running for engine in self.engines)

    @property
    def stopping(self) -> bool:
        return self._stopping

    def requestStop(self):
        """Stop claiming new work, without waiting for what is in flight.

        The heartbeat goes on: work still finishing belongs to a live worker, and the version check must
        go on seeing it. stopWorkers() ends it.
        """

        self._stopping = True

        for engine in self.engines:
            engine.workers.requestStop()

    def stopWorkers(self, timeout: float | None = 30) -> bool:
        """Stop claiming, let the work already claimed finish, then return.

        Returns False if anything was still busy when `timeout` ran out. The threads are daemons, so
        leaving the process then abandons them, as a kill would: their leases lapse, and the next worker
        writes their tasks off.
        """

        self.requestStop()

        deadline = None if timeout is None else time.monotonic() + timeout
        drained = True

        for engine in self.engines:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())

            if not engine.workers.stop(remaining):
                drained = False

        self._quit.set()

        # joined, so a startWorkers() straight after finds them gone and starts fresh ones, rather than
        # trusting a heartbeat on its way out
        for thread in self._monitors.values():
            thread.join(1)

        self.registry.deregister()

        if not drained:
            logger.warning("shutdown timed out with work still in flight")

        return drained

    def joinWorkers(self, timeout: float | None = None) -> bool:
        """Block until the workers stop. Returns whether they have."""

        deadline = None if timeout is None else time.monotonic() + timeout

        for engine in self.engines:
            for thread in engine.workers.threads:
                thread.join(None if deadline is None else max(0.0, deadline - time.monotonic()))

        return not self.running

    def handleSignals(self, signals: Sequence[int] = (signal.SIGINT, signal.SIGTERM)):
        """Turn SIGINT and SIGTERM into a graceful shutdown: the first stops claiming and lets the work in
        flight finish; a second exits at once.

        Opt-in, because a host framework may want to own these. SIGKILL cannot be caught: the process
        dies with its work in flight, and its leases lapse for the next worker to deal with.
        """

        def onSignal(signum, frame):
            if self._stopping:
                logger.warning("second signal (%s), exiting now", signum)
                os._exit(128 + signum)

            logger.info("signal %s: finishing the work in flight, not claiming more", signum)
            self.requestStop()

        for each in signals:
            # a second call must not record our own handler as the one to restore
            self._previousHandlers.setdefault(each, signal.getsignal(each))
            signal.signal(each, onSignal)

    def restoreSignals(self):
        for each, handler in self._previousHandlers.items():
            signal.signal(each, handler)

        self._previousHandlers.clear()

    def run(self, taskWorkers: WorkerCounts = 0, schedulerWorkers: WorkerCounts = 0, timeout: float | None = 30) -> bool:
        """Start workers, handle signals, and block until a signal — or retiring — stops them. Returns
        whether the work in flight drained in `timeout`.

            if __name__ == "__main__":
                drained = app.run(taskWorkers=4, schedulerWorkers=1)
                sys.exit(3 if app.retired else 0 if drained else 1)

        `app.retired` says the process stopped itself for having too many abandoned threads, so its
        supervisor can restart it. A run that would start no worker at all is refused, rather than
        returning at once.
        """

        taskCounts = self._counts("taskWorkers", taskWorkers, self.taskEngines)
        schedulerCounts = self._counts("schedulerWorkers", schedulerWorkers, self.schedulerEngines)

        if not any(taskCounts.values()) and not any(schedulerCounts.values()):
            raise ValueError(
                "run() starts no workers: give taskWorkers and/or schedulerWorkers, e.g. "
                "app.run(taskWorkers=4, schedulerWorkers=1). A process that only enqueues needs no run()."
            )

        self.handleSignals()
        self.startWorkers(taskWorkers, schedulerWorkers)

        try:
            self.joinWorkers()
        finally:
            drained = self.stopWorkers(timeout)
            self.restoreSignals()

        if self.retired:
            logger.error("retired with %d abandoned thread(s): exiting, for the supervisor to restart this process", len(self.abandoned()))

        return drained

    def __repr__(self) -> str:
        # reached while the app is still being built, through a bound task's repr
        return f"{type(self).__name__} ({self.db.name}, {getattr(self, 'fingerprint', 'being built')})"
