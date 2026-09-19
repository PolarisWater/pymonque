# Reference

- [BaseApp](#baseapp) · [Declarations](#declarations) · [Running and shutting down](#running-and-shutting-down)
- [Collections](#collections) · [Tasks](#tasks) · [Limits](#limits) · [Schedulers](#schedulers) ·
  [Distributions](#distributions) · [Piles](#piles)
- [Leases and claims](#leases-and-claims) · [One version at a time](#one-version-at-a-time) ·
  [Startup housekeeping](#startup-housekeeping) · [Not enough workers](#not-enough-workers)
- [Exceptions](#exceptions) · [Upgrading from 2.0](#upgrading-from-20) ·
  [Known limitations](#known-limitations) · [Notes](#notes)

## BaseApp

```python
class App(BaseApp):
    distributions   = MyDistributions           # the interval registry; left out: BaseDistributions

    taskTimeout     = 300                       # defaults are declared, never passed
    pileMaxAttempts = 3

    groups  = collection(Group, key="groupId")
    heavy   = tasks(RenderTask, leaseSeconds=900)
    nightly = schedulers(emitsInto=heavy, missed="skip")
    outbox  = pile(Email)

    @task(timeout=60)
    def sync(self, accountId: int): ...

app = App(
    db,                                         # a pymongo Database
    taskPollInterval        = 1,                # seconds an idle worker waits between polls
    schedulerPollInterval   = 1,
    enforceVersion          = True,             # see One version at a time
    heartbeatInterval       = 15,
    workerStaleAfter        = 60,
    backlogWarnAfter        = 60,               # see Not enough workers; None turns it off
    backlogInterval         = 30,
    retireAfter             = None,             # see Timeouts
)
```

**Shared behaviour lives on the class, pacing on the constructor.** Everything every process must
agree on — the parts, each task's limits, the defaults, the distribution registry — is declared in
code, is part of the [fingerprint](#one-version-at-a-time), and is never a constructor argument. What
a process may set for itself — how often it polls, how often it checks in, what it warns about — is.

**Constructing an app writes nothing but indexes**, so any process may do it at any time: an API
enqueues, a script reads, a worker container runs. Housekeeping belongs to a process that starts
workers.

### Defaults

| Default | Value | Fills in |
|---|---|---|
| `taskTimeout` | `None` | `@task(timeout=)` — seconds a call may run; `None`: no limit |
| `taskSkipAfter` | `None` | `@task(skipAfter=)` — seconds past its deadline still worth running; `None`: however late |
| `taskLeaseSeconds` | `300` | `tasks(leaseSeconds=)` |
| `schedulerMissed` | `"once"` | `schedulers(missed=)` |
| `schedulerLeaseSeconds` | `300` | `schedulers(leaseSeconds=)` |
| `pileMaxAttempts` | `1` | `pile(maxAttempts=)` |
| `pileLeaseSeconds` | `300` | `pile(leaseSeconds=)` |

A bad default fails as the class is defined, naming itself. 2.0's `taskMaxAttempts`,
`taskRetryDelay`, `itemMaxAttempts` and `itemRetryDelay` are refused, saying what became of them.

### What an app holds

| Attribute | What |
|---|---|
| `app.<name>` | each declared part, as its engine |
| `app.task` / `app.scheduler` | the default task and scheduler engines |
| `app.taskEngines` / `app.schedulerEngines` | `dict[str, TaskEngine]` / `dict[str, SchedulerEngine]`, the defaults included |
| `app.piles` / `app.collections` | `dict[str, PileEngine]` / `dict[str, CollectionEngine]` |
| `app.engines` | every engine that runs workers |
| `app.distribution` | the `DistributionEngine` |
| `app.functions` / `app.limits` | the app's tasks, and the `TaskLimits` each resolved to |
| `app.defaults` | the `AppDefaults` the class declares |
| `app.fingerprint` | see [One version at a time](#one-version-at-a-time) |
| `app.db` | the database |

Reached through the class, a part is its declaration: `App.outbox` is the `pile(...)`, `app.outbox`
the `PileEngine`.

## Declarations

Every storage declaration takes the same leading arguments — the model, then the collection — and
after them, by keyword, only what its kind needs:

```python
kind(Model,                     # the document, task, scheduler or payload model
     collection = <left out>,   # a Collection, or its name; left out: a default name
     extraIndexes = None,       # more IndexModels to create
     **settings)                # by keyword
```

| Declaration | Model | Default collection | Settings |
|---|---|---|---|
| `collection(Model, …)` | a `Document` subclass | the attribute name | `key="uid"` |
| `tasks(Model=Task, …)` | a `Task` subclass | `pymonque_task_<name>` | `leaseSeconds`, `pollInterval` |
| `schedulers(Model=Scheduler, …)` | a `Scheduler` subclass | `pymonque_scheduler_<name>` | `emitsInto`, `missed`, `leaseSeconds`, `pollInterval` |
| `pile(Payload=None, …)` | any pydantic model; `None`: any dict | `pymonque_pile_<name>` | `maxAttempts`, `leaseSeconds` |
| `@task` / `@task(…)` | — | — | `timeout`, `skipAfter` |

**Checked where it is written.** A bad value raises pydantic's `ValidationError` naming the setting,
as the class is defined; a setting that does not exist raises `TypeError` naming it, and one that
2.0 had says what became of it. **Leaving a setting out is the only way to take the default**: `None`
is refused everywhere except `timeout` and `skipAfter`, where it means no limit. `pollInterval` left
out takes the constructor's.

**The default engines.** `BaseApp` declares `task = tasks()` and `scheduler = schedulers()`, stored in
`pymonque_task` and `pymonque_scheduler`. A declaration of the same name replaces one and takes over
its collection:

```python
class App(BaseApp):
    @task
    def sync(self, accountId: int): ...

    task = tasks(AccountTask)       # below the tasks: from here on, `task` is no longer the decorator
```

`task = tasks(...)` hides the `@task` decorator for the rest of that class body, so declare it below
the tasks; a declaration used as a decorator raises a `TypeError` saying so.

**Reserved names.** Any other declaration or task under a name `BaseApp` uses — a pile called
`backlog`, a task called `init`, a collection called `db` — would replace part of the app, and is
refused with a `TypeError` as the class is defined. `task` and `scheduler` may only be replaced by a
declaration of their own kind.

**Subclasses.** Declarations are inherited, and a subclass's redefinition of a name replaces the
parent's — a pile with another pile, a task with a plain method (which is then no task).

## Running and shutting down

```python
if __name__ == "__main__":
    drained = app.run(taskWorkers=4, schedulerWorkers=1, timeout=30)   # start, handle signals, block
    sys.exit(3 if app.retired else 0 if drained else 1)
```

That is the whole of a worker process. `run()` refuses to start with no workers at all — a process
that only enqueues needs no `run()`. For finer control:

| | |
|---|---|
| `startWorkers(taskWorkers=0, schedulerWorkers=0)` | Run [housekeeping](#startup-housekeeping), check in, and start worker threads. |
| `requestStop()` | Stop claiming new work. Returns at once. |
| `stopWorkers(timeout=30)` | Stop claiming, wait for the work in flight, check out. `False` if the timeout ran out. |
| `joinWorkers(timeout=None)` | Block until the workers stop. `True` if they have. |
| `handleSignals(signals=(SIGINT, SIGTERM))` / `restoreSignals()` | Install, and remove, graceful-shutdown handlers. |
| `app.running` / `app.stopping` / `app.retired` | |
| `engine.workers` | Each engine's `WorkerLoop`: `running`, `stopping`, `count`, `busy`, `threads`, `start(n)`, `stop(timeout)`. |

**Worker counts.** An int is that many threads on **every** engine of the kind, the defaults
included; a dict sets engines by name, and those it leaves out get none:

```python
app.startWorkers(taskWorkers=2)                                  # 2 on task, 2 on heavy
app.startWorkers(taskWorkers={"heavy": 4}, schedulerWorkers=1)   # 4 on heavy only; 1 per scheduler engine
```

A name the app has no engine of is refused. Both default to none, so a process that only enqueues
never starts a worker by accident.

A worker sleeps only when it found nothing to do, so a backlog drains at full speed; it waits on the
stop event rather than sleeping, so a shutdown never sits through a poll interval. An iteration that
raises — the database unreachable, a stored document that no longer fits its model — is logged
(`task-heavy worker iteration failed`) and the worker carries on.

**Stopping.** A stop is checked between pieces of work, never inside one, so work already claimed
runs to its end; anything not yet claimed waits for the next process. While work drains, its leases
are still renewed and the process still checks in, so nothing it holds is taken over and the
version check still sees it. `stopWorkers()` then checks the process out, freeing its version slot at
once. Workers can be started again after a stop.

### Signals

`handleSignals()` is opt-in, because a host framework may want to own them; `run()` installs it and
restores the previous handlers when it returns.

| | |
|---|---|
| First SIGINT/SIGTERM | Stop claiming, finish the work in flight, exit |
| Second | `os._exit(128 + signum)` at once |
| SIGKILL | Cannot be caught. The process dies with its work in flight, and its leases lapse for the next worker. |

## Collections

```python
class Group(Document):              # Document gives it a uid
    name: str = ""

class App(BaseApp):
    groups   = collection(Group)                         # -> the "groups" collection
    accounts = collection(Account, key="accountId")      # keyed by a field of your own
    legacy   = collection(Group, "old_groups")           # an existing collection, by name or object
```

The key is uniquely indexed, and must be a field of the model; an aliased key is stored and queried
under its alias.

### CollectionEngine

| Method | What |
|---|---|
| `create(**fields)` | Build, store and return it, bound. |
| `build(**fields)` | Build it, bound but not stored. |
| `insert(doc)` / `insertMany(docs)` | Store what you built. `insertMany([])` does nothing. |
| `save(doc)` | Replace the stored document, creating it if absent. |
| `update(key, **fields)` | Merge fields in, validated against the model before anything is written; only what is given is written. `None` if there is no such document. |
| `get(key)` / `findOne(where=None)` | One document, or `None`. |
| `find(where=None, sort=None, limit=None)` | A list. |
| `count(where=None)` / `exists(key)` | |
| `delete(key)` / `deleteMany(where)` | `True` / how many. |

Task engines, scheduler engines and piles are collection engines too, so `app.task.find(...)`,
`app.scheduler.count(...)` and `app.outbox.get(...)` work the same way.

### Bound documents

A document that came from an engine remembers it:

```python
group = app.groups.create(name="beta")
group.name = "gamma"
group.save()
group.delete()
fresh = group.reload()
```

One built by hand is unbound until stored: `save()` on it raises `UnboundDocument` rather than guess
where it belongs. `bound` says which it is; the binding is never written to MongoDB.

A bound document remembers the key it was stored under, so **changing the key renames it** rather
than leaving a copy behind; `delete()` and `reload()` use that key too, whatever the field now holds
in memory. `storedKey` is the key it was last written under, `None` until it is stored.

### Fields the engine keeps

A task, a scheduler and a pile item carry fields their engine writes and nobody else should: their
identity, state, claim and outcome. Each model names them in `_kept`, and a subclass inherits them:

| Model | Kept by the engine |
|---|---|
| `Task` | `uid` `status` `claimId` `leaseUntil` `claimedAt` `finishedAt` `executionTime` `result` `error` |
| `Item` | `uid` `status` `claimId` `leaseUntil` `claimedAt` `finishedAt` `attempts` `result` `error` |
| `Scheduler` | `uid` `status` `claimId` `leaseUntil` |
| `Document` | none: a plain collection's fields are all yours, its key included |

The default API never writes them. `update()`, `build()` and `create()` refuse them by name — so do
`schedule()` and a scheduler's `add()`, `ensure()` and `update()` — saying what changes them instead:
`cancel()` for a task, `done()` / `fail()` / `release()` / `cancel()` for an item, `enabled=` for a
scheduler. `save()` leaves them as stored, whatever the copy in hand carries, so **a copy read before
a worker finished cannot put the work back**, rerun a task, or wipe the claim of the worker holding it;
the rest of the document is written as given. A document saved for the first time starts with the
kept fields a new one would have, its key aside.

Moving a deadline is yours: on a waiting task or a scheduler it moves the lease with it, and on a held
scheduler it releases the claim.

## Tasks

```python
class App(BaseApp):
    @task
    @staticmethod
    def send(to: str, subject: str = ""): ...

    @task
    def withSelf(self): ...         # an instance task gets the app, and reaches its parts

    @task
    @classmethod
    def withClass(cls): ...

    @task(timeout=60, skipAfter=None)
    def withLimits(self): ...       # see Limits
```

| Access | Gives |
|---|---|
| `App.send` | a `FuncSpec`; call it to build a `CallSpec` |
| `App.send(to="a@b.c")` | a `CallSpec`, `{functionName, kwargs}` |
| `app.send(to="a@b.c")` | the function itself, run now |
| `app.task("send", to="a@b.c")` | a `CallSpec` by name, the arguments given checked |

**Signatures.** A task is called with keyword arguments only, since a `CallSpec` has nowhere to put a
positional one: `*args` and positional-only parameters are refused where the task is declared.
`**kwargs` is fine. An instance task without `self` is refused too.

A call is **validated when it is scheduled**, against the signature, so a bad one is never stored,
and **again when it runs**, so the function receives what its annotations promise: a model argument
arrives as the model rather than the dict it was stored as, and `"3"` for an `int` arrives as `3`.
Arguments left out stay left out — the function's own defaults apply when it runs. Calling an engine
(`app.task("send", …)`) checks only the arguments given, since a `Task` subclass may supply the rest.

### Task engines

Tasks belong to no engine: every task engine runs any of the app's tasks, and only chooses where
tasks wait and who works them. Declare more than the default to keep slow work from holding up
quick work:

```python
class App(BaseApp):
    heavy = tasks(leaseSeconds=900)             # -> pymonque_task_heavy

app.heavy.schedule(App.render(videoId=7))
app.run(taskWorkers={"task": 4, "heavy": 1})
```

| Method | What |
|---|---|
| `schedule(work, deadline=None, factory=None, **fields)` | Store a task, due at `deadline` or now, checked first. `fields` are the ones the engine's model adds. |
| `scheduleFromDistribution(work, distribution, factory=None, **fields)` | The same, due one interval from now. |
| `__call__(functionName, **kwargs)` | A `CallSpec`, the arguments given checked. |
| `work()` | Claim the most overdue task and see it through; the task as it ended, or `None`. What a worker loops on. |
| `purge(olderThan, statuses=<every finished status>)` | Delete finished tasks that ended more than `olderThan` (seconds or a timedelta) ago; how many. Never a waiting or running task. |
| `wait(task, timeout=None, interval=0.1)` | Block until a task has ended, and return it. From any process. `TimeoutError` if it doesn't in time, `TaskNotFound` if there is no such task. |
| `cancel(uid)` / `cancelMany(where=None)` | Cancel tasks that have not started. |
| `limits` / `limitsFor(task)` | Every task's resolved `TaskLimits`, and the ones a stored task runs under. |
| `backlog()` | See [Not enough workers](#not-enough-workers). |
| `abandoned()` | See [Timeouts](#timeouts). |
| `flagIncompatible()` / `writeOffStuck()` | See [Startup housekeeping](#startup-housekeeping). |

**Custom tasks.** A declaration may take a `Task` subclass: its fields are stored on every task of
that engine — context to query and index by — and given where the task is created. `runWork()` is
the one place they reach the call:

```python
class AccountTask(Task):
    accountId: int

    def runWork(self) -> CallSpec:
        return self.work.bind(accountId=self.accountId)

class App(BaseApp):
    accountTasks = tasks(AccountTask, extraIndexes=[IndexModel("accountId")])

    @task
    def sync(self, accountId: int): ...

app.accountTasks.schedule(App.sync(), accountId=42)     # runs sync(accountId=42)
```

`schedule()` checks the call as `runWork()` returns it. It refuses a limit (`timeout=…` belongs on
`@task`), a field `Task` itself keeps (`status`, `uid`, …), and a field the model does not have. The
fields are data only: they do not change what is claimed first. `CallSpec.bind(**kwargs)` returns a
copy with the arguments merged in.

**Who scheduled it.** `factory` tags a task with its source, a `TaskFactory(name=…)`; a scheduler tags
the tasks it emits with itself. Left out, it is `TaskFactory(name="default")`.

```python
app.task.schedule(work, factory=TaskFactory(name="web-api"))
app.task.find({"factory.name": "web-api"})
```

### Task document

| Field | Type |
|---|---|
| `uid` | `str` |
| `status` | `pending` `running` `done` `failed` `canceled` `timeout` `outdated` `incompatible` |
| `work` | `CallSpec` |
| `deadline` | `datetime` (naive UTC) |
| `factory` | `TaskFactory` — `{uid, name}` |
| `createdAt` / `claimedAt` / `finishedAt` | `datetime`; `finishedAt` is set once the status is final, including `canceled`, `outdated` and `incompatible` |
| `leaseUntil` / `claimId` | see [Leases and claims](#leases-and-claims) |
| `executionTime` | `timedelta`, stored as seconds |
| `result` | anything BSON can encode |
| `error` | the traceback, or why it ended as it did |

```
pending ─→ running ─→ done
               ├────→ failed      the call raised — final, never retried
               ├────→ failed      its worker died: written off by the next claim, not rerun
               └────→ timeout     the call outlived its limit

pending ─→ outdated       claimed past skipAfter, never run
pending ─→ canceled       cancel() before it started
pending ─→ incompatible   its function no longer exists on the app
```

**A task runs at most once.** A task that raises is `failed`, and nothing retries it: rerunning a side
effect nobody asked to rerun — an upload, a payment — is worse than a failure you can see. A task
that must succeed retries inside its own code. A task left `running` by a worker that died is
written off as `failed` by the next claim, keeping when it was started and when its lease ran out,
with an error saying so — it may have done part of its work. A result the driver cannot encode is a
failure too, and does not leave the task claimed. `sys.exit()` in a task is a failure, not a dead
worker.

**Cancelling.** `cancel(uid)` cancels a task that has not started — waiting, or held by a worker whose
lease lapsed — and returns whether it did. A running task cannot be interrupted, only waited out.
`cancelMany(where)` cancels every such task matching `where`, and returns how many.

## Limits

What a task may do is part of declaring it, because it describes the function — how long it may run,
how late it is still worth running — not one call of it:

```python
class App(BaseApp):
    taskTimeout = 300                       # the default for every task

    @task(timeout=900)                      # this task's own
    def sync(self, accountId: int): ...

    @task(skipAfter=None)                   # None: no limit, whatever the default
    @staticmethod
    def charge(orderId: str): ...
```

| Limit | App default | What |
|---|---|---|
| `timeout` | `taskTimeout = None` | seconds a call may run before the task is written `timeout`; `None`: no limit |
| `skipAfter` | `taskSkipAfter = None` | seconds past its deadline a task is still worth running; `None`: however late |

A limit left off `@task(...)` takes the app's default. A scheduled call carries no limits, and
neither does a scheduler, so an emitted task runs under the same limits as any other call of that
task. To run a function under other limits, declare a second task. `app.limits` holds what every
task resolved to, and all of it is in the [fingerprint](#one-version-at-a-time).

| | Declared on | App default | Stored on documents |
|---|---|---|---|
| Task limits: `timeout`, `skipAfter` | `@task(...)` | `task*` | no |
| Item tries: `maxAttempts` | `pile(...)` | `pileMaxAttempts` | no |
| Lease length: `leaseSeconds` | `tasks(...)`, `schedulers(...)`, `pile(...)` | `taskLeaseSeconds`, `schedulerLeaseSeconds`, `pileLeaseSeconds` | no |
| Missed beats: `missed` | `schedulers(...)` | `schedulerMissed` | no |

### Timeouts

A task with a timeout runs its call in a thread of its own; one without runs on the worker. When the
timeout passes:

1. the task is written `timeout`, with where the call was in its `error`, and the worker is freed for
   the next task — a timed-out task is never run again;
2. a warning names the task, its uid, the limit and the call's stack;
3. a pile item the call holds stops being renewed, so its lease lapses (a spent try) and another
   worker can take it;
4. `TaskStopped` is raised inside the call, to stop it.

`TaskStopped` derives from `BaseException`, so `except Exception:` does not swallow it — a bare
`except:` does. **It stops Python code only**: a call blocked in C, I/O or a sleep sees it only once
that call returns. A few seconds later (`engine.stopGrace`, 3 by default) the log says either that the
call stopped, or — with its stack again — that it is still running, blocked outside Python, and its
thread is **abandoned**. `app.abandoned()` lists abandoned calls still running, with how long each
has run and `where()` it is stuck; their count is on the process's heartbeat, as `abandonedThreads`.

**Retiring.** `App(db, retireAfter=N)` makes a worker process that has abandoned N threads stop for
good: it logs every abandoned call and where it is stuck, then "no longer claiming, draining", stops
claiming, lets its work in flight finish, sets `app.retired` and returns from `run()`, so the process
can exit for its supervisor (Docker, systemd) to restart it. Left out, a process never retires.

### skipAfter

A task claimed more than `skipAfter` seconds past its deadline is written `outdated`, with how late
it was, and never run. It is the one rule for stale work, whatever made it stale: the app was down,
or nobody kept up.

## Schedulers

A scheduler stores a call and a distribution. When its deadline passes, it emits the call as a task
dated to that deadline, then moves its deadline on by a fresh interval, measured from the old
deadline so the rhythm does not drift.

### Scheduler engines

```python
class App(BaseApp):
    heavy      = tasks()
    globalOps  = schedulers()                                       # -> pymonque_scheduler_globalOps
    accountOps = schedulers(AccountScheduler, "account_ops")
    nightly    = schedulers(emitsInto=heavy, missed="skip")
```

A scheduler engine emits into the default task engine, or into the one `emitsInto` names — a
reference to a `tasks(...)` declared above it on the same class, or inherited
(`emitsInto=Parent.heavy`). A subclass that replaces that engine replaces it for the scheduler engine
too.

| Method | What |
|---|---|
| `add(work, distribution, **fields)` | Store a new scheduler, first beat one interval from now. Every call makes another. |
| `ensure(name, work, distribution, enabled=None, **fields)` | Declare one by name. Idempotent. |
| `build(work, distribution, deadline=None, **fields)` | A scheduler of the engine's model, not stored. |
| `upsert(scheduler)` / `scheduler.save()` | Store one whole, creating or replacing it. |
| `update(uid, work=None, distribution=None, enabled=None, **fields)` | Change parts of one; `None` if there is no such uid. |
| `byName(name)` / `removeNamed(name)` | The one `ensure()` declared under that name; delete it. |
| `validateScheduler(scheduler)` | Raise unless it would run. |
| `work()` | Claim the most overdue scheduler, emit its beat, move it on; `None` if none was due. |
| `get`, `find`, `count`, `delete`, `deleteMany` | As on any collection. |

`**fields` are the scheduler model's own, such as `name` or your subclass's. What the engine keeps
— `uid`, `claimId`, `leaseUntil` and `status` — is refused; enable and disable with `enabled=`. A
`deadline` may be moved by hand with `update()`, and the lease moves with it.

**Checked as it will run.** `add`, `ensure`, `upsert` and `update` build the task the scheduler will
emit — in the target engine's `Task` model, with the scheduler's `taskFields()` — and check its call as
that task's `runWork()` gives it. So a bad call, an unknown task or distribution, or context the target
engine cannot take is refused before anything is stored.

A new distribution restarts the rhythm from now; new work or fields keep it. A `disabled` scheduler
still walks its deadline, emitting nothing, so its distribution keeps its shape for when it is
enabled again.

**A beat the app cannot run is skipped**, with a warning naming the scheduler: its task is gone, or
its stored call or fields no longer fit a model that changed since. The scheduler stays `enabled` —
disabling is the operator's word, and would outlast the fix.

### ensure

The name is hashed into the scheduler's uid, which is uniquely indexed, so processes starting
together make one scheduler, not one each.

| You change | What happens |
|---|---|
| the work, or fields | updated in place; rhythm untouched |
| the distribution | updated; next deadline recomputed from now |
| nothing | nothing; a restart never resets the deadline |

`enabled` is left as the database has it unless given, so a scheduler disabled in production stays
disabled across a deploy. Dropping the `ensure()` call does not delete the scheduler — use
`removeNamed()`. The name lands on every emitted task as `factory.name`.

### Context on emitted tasks

A scheduler never changes the call it emits: context reaches the call only through the task, by
`Task.runWork()`. A `Scheduler` subclass hands its fields to the tasks it emits with `taskFields()`:

```python
class AccountScheduler(Scheduler):
    accountId: int

    def taskFields(self) -> dict[str, Any]:
        return {"accountId": self.accountId}

class App(BaseApp):
    accountTasks = tasks(AccountTask)                   # AccountTask.runWork() stamps accountId
    accountOps   = schedulers(AccountScheduler, emitsInto=accountTasks)

app.accountOps.add(App.sync(), dist, accountId=42)      # stored work: sync(); runs sync(accountId=42)
```

The stored work stays context-free, so one declaration serves every account. A scheduler with
context must emit into an engine whose `Task` has those fields; one that cannot is refused when the
scheduler is added.

### Missed beats

What a scheduler owes for beats that went by unworked, applied every time one is claimed — a
scheduler falls behind while an app is running as easily as while it is down.

| `missed` | when a whole beat went by unworked |
|---|---|
| `"once"` | emit one task, then resume from now (default) |
| `"replay"` | emit every missed beat, one per claim, keeping the original rhythm |
| `"skip"` | emit nothing, resume from now |

A scheduler that is merely due emits normally under all three. `"replay"` is the only one that can
stay behind for good: a scheduler set faster than its workers serve keeps a backlog forever. Each
missed beat is logged as `missed a beat`.

### Scheduler document

| Field | Type |
|---|---|
| `uid` | `str` — derived from the name when created by `ensure()` |
| `name` | `str`, default `"Scheduler"` |
| `status` | `enabled` `disabled` |
| `work` | `CallSpec` emitted on each beat |
| `distribution` | `CallSpec` producing the interval |
| `deadline` | `datetime` — the next beat |
| `leaseUntil` / `claimId` | see [Leases and claims](#leases-and-claims) |

A scheduler carries no limits: what it emits runs under its task's. Being worked is a claim, not a
status.

## Distributions

Every built-in takes `dailyFrequency` — average beats per day — and returns a `timedelta`. It must be
positive, and every distribution, yours too, must return a positive interval, or a scheduler would
never move on: `DistributionValidationError` either way.

| Name | Extra argument |
|---|---|
| `constant` | — |
| `normal` | `stdFraction` (≥ 0) |
| `lognormal` | `sigma` (≥ 0) |
| `exponential` | — |

```python
dist = app.distribution("normal", dailyFrequency=10, stdFraction=0.2)
```

Add your own with a registry, declared on the class:

```python
class MyDistributions(BaseDistributions):
    @staticmethod
    def workHours(dailyFrequency: float) -> timedelta:
        return timedelta(seconds=28800 / dailyFrequency)

class App(BaseApp):
    distributions = MyDistributions
```

Staticmethods only, not starting with `_`; the built-ins stay available. A distribution call is
always checked in full, since nothing adds arguments to it.

## Piles

A pile holds work that has to happen even if the process holding it dies: the item holds the work, a
task drains it. Piles run no workers of their own — drive one from a task, and fire that task from a
scheduler.

```python
class App(BaseApp):
    outbox = pile(Email)                        # data validated against Email
    scraps = pile()                             # data is any dict
    tried  = pile(Email, maxAttempts=3)         # left out: the app's pileMaxAttempts
```

### The work() block

```python
@task
def sendOne(self):
    with self.outbox.work() as w:
        if w is None:
            return                  # the pile is empty
        if rateLimited():
            w.release()             # ends the block here: back on the pile, try returned
        if not w.data.to:
            w.fail("no recipient")  # ends the block here: failed, final
        if alreadySent(w.data):
            w.done("duplicate")     # ends the block here: done
        send(w.data)
    # reaching the end of the block: done; an exception: failed, and re-raised
```

`work(where=None)` claims one item — the one that has waited longest, matching `where` — and holds it
for as long as the block runs. It yields a `Work` (`w.data`, `w.uid`, `w.attempts`, `w.item`), or
`None` on an empty pile. The block ends in exactly one outcome:

| | |
|---|---|
| the block reaches its end | `done` |
| the block raises | `failed`, with the traceback; the exception is re-raised |
| `w.done(result=None)` | `done`, and the block ends there |
| `w.fail(error=None)` | `failed` — final — and the block ends there |
| `w.release(delay=None)` | back on the pile with its try returned — claimable at once, or after `delay` — and the block ends there |
| the claim was lost meanwhile | nothing is written, and it is logged |
| `w.confirm()` finds the claim lost | nothing is written, it is logged, and the block ends there |

**What the block does for you:** the lease is renewed in the background for as long as it runs;
every outcome is written only while this claim still holds the item, so a holder whose lease lapsed
cannot overwrite the worker that took over, or a cancel; the end of the block is `done`, an exception
is `failed`; `sys.exit()` in the block is a failure too. Nested blocks end the right item: an outer
block's early end, passing through an inner one, puts the inner item straight back with its try
spent — nobody said its work did not happen.

`w.done()`, `w.fail()` and `w.release()` **write the outcome first**, then leave the block by raising a
private exception derived from `BaseException`, which `work()` catches, so the code after the `with`
carries on and nothing outside sees an exception. `except Exception:` in the block does not catch it.

**`w.confirm()` — still mine?** A holder frozen for longer than its lease — a paused VM, a suspended
laptop, the database out of reach — cannot tell that another worker has taken its item over. Call
`w.confirm()` just before a side effect that must not happen twice: it asks the database now, and if
the claim still holds the item it renews the lease and the block goes on; if not — taken over,
cancelled, given up — the block ends there and nothing is written.

```python
with self.outbox.work() as w:
    if w is None:
        return
    body = render(w.data)           # slow, safe to redo
    w.confirm()                     # still mine? if not, the block ends here
    send(w.data.to, body, idempotencyKey=w.uid)
```

It narrows the window to the moment between the call and the side effect; a freeze in exactly that
moment still does it twice. Only the receiving system can close it, by refusing a duplicate — `w.uid`
is the key to give it.

> **Caveat.** Something that swallows `BaseException` inside the block — a bare `except:`, `except
> BaseException:`, `contextlib.suppress(BaseException)`, or `return` / `break` / `continue` in a
> `finally` — keeps the block running after `w.release()` (or `done` / `fail`). The outcome already
> stored stands and nothing more is written, and a warning says the block kept running; but the code
> that ran after it has run, and after a release or a fail another worker may already hold the item.

**Draining several items in one task.** Put the block in a loop, one claim per pass; `range` caps how
many one run takes:

```python
@task
def sendBatch(self):
    for _ in range(100):
        with self.outbox.work() as w:
            if w is None:
                break                   # pile empty: stop early
            if not ready(w.data):
                w.release(delay=60)     # this pass ends; the loop goes on to the next item
            send(w.data)
```

Each pass ends in its own outcome, and `w.done()`, `w.fail()` and `w.release()` end only that pass.
An exception fails that item and, re-raised, ends the loop. Three things to know:

- **`continue`, `break` or `return` with an item in hand marks it `done`** — leaving a `with` block
  that way is reaching its end. To skip an item, say what became of it: `w.release(...)`,
  `w.fail(...)` or `w.done(...)`. The guard's `break` is safe only because there is no item then.
- **`w.release()` without a delay spins in a loop:** the item goes back to the front, and the next pass
  takes it again. In a loop, release with a delay.
- **`while True:` stops only when the pile is empty,** items added meanwhile included; cap it with
  `range` if one task should not run for long.

### Tries

Items have tries because the task holding one can die before it reports back. **A claim uses a
try.** An item whose holder stopped renewing its lease goes back to be claimed with that try spent;
out of tries, the next claim gives it up as `failed`, saying its tries' outcomes never came back.
`fail()` is final. `release()` hands an item back unfinished and **returns the try**, since the
holder says the work did not happen (shutting down, rate limited, not ready yet). Items have no retry
delay after a failure. A released item is claimable at once, in its own place in the pile — or, with
`release(delay=…)` (seconds or a timedelta), only once the delay has passed, queued by that time. Use
a delay for an item that is not ready yet: released without one, it goes back to the front and is
taken and handed back by every claim, so nothing behind it is reached. The default of one try
means an item whose holder died is given up rather than handed on — nobody knows how far it got.

### PileEngine

| Method | What |
|---|---|
| `add(data=None, **kwargs)` | Add one item, from a payload model, a dict, or keyword arguments. |
| `addMany(data)` | Add many in one write, every payload checked first. |
| `work(where=None)` | The block above. |
| `claim(where=None)` | Take the item that has waited longest, or `None`. |
| `done(item, result=None)` / `fail(item, error=None)` / `release(item, delay=None)` | Record an outcome outside a block. `True` if it was written. |
| `renewLease(item)` | Hold an item for another lease. |
| `cancel(uid)` / `cancelMany(where=None)` | Cancel items nobody is working. |
| `count(where=None, status=None)` / `counts()` | `counts()` gives every status. |
| `purge(status="done", olderThan=None)` | Delete finished items of a status, or of several — all of them, or only those that ended more than `olderThan` (seconds or a timedelta) ago; how many. Never a waiting or held item. |

`item` may be an `Item` or a uid. **An `Item` stands for the claim that handed it out**: the outcome is
written only while that claim holds it, and one that never came from `claim()` raises `ValueError`.
**A uid is an operator's verdict**, whatever holds the item — but only on unfinished work: an item
already `done`, `failed` or `canceled` is left as it ended. A running item cannot be cancelled, only
waited out.

### Item document

| Field | Type |
|---|---|
| `uid` | `str` |
| `status` | `pending` `running` `done` `failed` `canceled` |
| `data` | the payload |
| `createdAt` / `claimedAt` / `finishedAt` | `datetime` |
| `attempts` | `int` — tries used: each claim uses one, `release()` returns one |
| `leaseUntil` / `claimId` | see [Leases and claims](#leases-and-claims) |
| `result` / `error` | |

```
pending ─→ running ─→ done
              ├────→ failed      fail(), an exception, or out of tries
              ├────→ pending     release(), try returned
              └────→ running     lease lapsed; another worker claimed it, try spent
pending ─→ canceled
```

## Leases and claims

Every task, scheduler and item carries `leaseUntil` — **the one field that says when it is
claimable.** While it waits, that is when it is due: a task's deadline, a scheduler's next beat, an
item's `createdAt`. While it is held, it is the end of its holder's lease. So a claim is one
comparison, `leaseUntil <= now`, and one atomic `find_one_and_update`; two workers can never take
the same document. The most overdue goes first.

**Every claim gives the document a new `claimId`**, and only a write that still matches it lands. An
outcome clears it, so a claim gets one outcome and nothing more; a cancel clears it too. A holder
whose lease lapsed therefore cannot overwrite a cancel, or the outcome of the worker that took over —
its write matches nothing, and is logged.

**Leases are renewed** every `leaseSeconds / 3` while the work runs, whoever ends it, and through a
graceful shutdown until the work is done. If the holder dies, nothing renews, the lease lapses, and
the next worker to poll claims it — no restart, no sweep, no coordinator:

| it claimed | it |
|---|---|
| a task | writes it off as `failed`, never runs it again |
| a scheduler | emits the beat if the dead worker had not — each beat's task has a fixed uid, so it is never emitted twice |
| a pile item | hands it out again with the try spent, or gives it up if none is left |

A scheduler is held only while it emits, milliseconds, and is not renewed. Moving a waiting task's or
a scheduler's deadline by hand — `save()` or `update()` — moves its lease with it, and releases a
scheduler's claim.

Because a live lease is visible in the document, **constructing an app never disturbs work in
flight**.

**One clock: the database server's.** A lease written on one host is compared on another, so a host
whose clock ran ahead would take over work another still holds. Every process therefore keeps time
by the server: constructing an app reads the server's clock (`hello`'s `localTime`, corrected by half
the round trip), and a worker process reads it again on every heartbeat, so drift is followed.
`utc_now()` is this host's clock plus that offset, and a host more than a second off the server is
logged. Build deadlines from `utc_now()`, not `datetime.now()`, so they share it. `syncClock(db)`
does the same for a process that builds no app. A server that will not say — mongomock, in tests —
leaves the host's own clock in use.

**Keep leases long.** A lease of L seconds is renewed every L/3, so a stall of about 2L/3 — one slow
write, a paused VM, a thread held by a long C call — hands live work to another worker. The default
is 300 s; a lease under 10 s is logged where the app is built, as one for tests.

## One version at a time

**Only one version of your code may run workers against a database at once.** Deploy all-or-nothing:
stop the old workers, then start the new ones.

A task document names a function, and nothing more. Two deployments can agree on every name while
one does something else entirely, so the library does not try to reconcile versions: it refuses to
let them run together. `app.fingerprint` is a short hash of what every process must agree on:

- every task's name and signature, defaults included, and the limits it resolved to;
- every distribution's name and signature;
- every task engine, scheduler engine, pile and collection: its name, its collection's name, its
  model's schema, and its settings — lease length, `missed`, `emitsInto`, tries, key.

`startWorkers()` checks the process in to `pymonque_workers` and refuses to start if a worker process
that has checked in recently reports a different fingerprint:

```
VersionMismatch: a live worker is running a different version of this app
(a41f0b93c2e1 on box-2:4471, this process is 3f9c1a02b7de). Only one version may run
at a time — stop the old workers before starting these.
```

| | |
|---|---|
| `app.fingerprint` | the hash for this process |
| `app.liveWorkers()` | worker processes that have checked in recently: fingerprint, host, pid, `taskWorkers` and `schedulerWorkers` by engine, `abandonedThreads` |
| `enforceVersion=False` | skip the check; the process still checks in, so it is still counted |

Workers check in every `heartbeatInterval` seconds, until their work has drained, and count as gone
after `workerStaleAfter`. A stopped process checks out at once; a crashed one frees its slot within
`workerStaleAfter`.

**What it cannot see:** a changed function body. That would need hashing every dependency; the
fingerprint is a guard against the obvious mistake, not a proof. Constructing an app is never
refused — only starting workers and housekeeping are — so any process may enqueue and read whatever
its version.

## Startup housekeeping

```python
app = App(db)           # safe from any process: changes nothing
app.startWorkers(...)   # runs init(), then starts working
app.init()              # or call it yourself
```

`init()` is run by `startWorkers()`, never by the constructor. On **every** task engine — whether or
not this process works it, so an engine nobody works is still cleaned:

- waiting tasks whose function is gone from the app are marked `incompatible`
  (`flagIncompatible()`);
- tasks a dead worker left `running` whose function is gone are written off as `failed`
  (`writeOffStuck()`) — a claim only takes tasks it can run, so none would ever find them.

Everything else is resolved where it is observed, at the claim: a lapsed lease, a task gone stale, an
item out of tries, a scheduler behind or whose task is gone. The set of tasks is the one thing that
can only change at a restart — the fingerprint sees to that — so it is the one thing `init()` decides.

`init()` is **version-checked** like `startWorkers()`: it decides what is runnable from this process's
tasks, so a process with other tasks — a script importing half the app — must not run it beside a
live deployment.

## Cleaning up

Finished tasks and items **stay**: they are the history of what ran, with its result, error and times,
queryable like any document. Nothing deletes them on its own. Every app has one task for keeping that
history in bounds:

```python
app.scheduler.ensure(
    "cleanup",
    App.cleanupFinished(days=30),
    app.distribution("constant", dailyFrequency=1),
)
```

`cleanupFinished(days=30)` deletes what finished more than `days` ago — tasks on every task engine,
items on every pile — and the records of worker processes gone that long, and returns how many of
each. It is a task like any other, so it runs on a scheduler, once a day being plenty, or at once as
`app.cleanupFinished(days=30)`. It carries its own limits (no timeout, no skipAfter), so a short
`taskTimeout` on the app does not cut a big cleanup short. Being part of `BaseApp`, its name is
reserved.

For finer choices, per engine and per status: `purge(olderThan, statuses=…)` on a task engine and
`purge(status, olderThan=…)` on a pile. A task or item that ended before 3.0 recorded `finishedAt`
counts by when it was created.

## Not enough workers

A free worker claims within one poll interval, so work that has waited much longer means every
worker on its engine is busy.

```python
app.backlog()       # {"task": (0, 0.0), "heavy": (4, 31.2)} -- four due on heavy, oldest waiting 31s
app.taskWorkers()   # {"task": 4, "heavy": 1} -- task workers on each engine, across every process
```

Once the oldest has waited `backlogWarnAfter` seconds, the app says so, engine by engine, at
`startWorkers()` and every `backlogInterval` after:

```
heavy: 4 task(s) due, oldest waiting 31s, 1 worker(s) on it across processes -- not enough
workers, or they are all on long tasks

heavy: 4 task(s) due, oldest waiting 31s, and no process runs workers on it
```

The count spans processes: a process running no workers of its own sees the workers elsewhere.
`backlogWarnAfter=None` turns the warning off.

## Exceptions

`from pymonque.exceptions import ...`

| | Raised when |
|---|---|
| `TaskNotFound` | no such task on the app, or no such task to `wait()` on |
| `TaskValidationError` | a call's arguments do not fit its task |
| `DistributionNotFound` | no such distribution in the registry |
| `DistributionValidationError` | bad arguments, or it did not return a positive `timedelta` |
| `TaskTimeout` | a task outlived its limit; recorded as status `timeout` |
| `TaskStopped` | raised *inside* a timed-out call to stop it; a `BaseException` |
| `VersionMismatch` | `startWorkers()`, `init()` or `upgradeFrom2()` found a live worker it cannot run beside |
| `UnboundDocument` | `save()` / `delete()` / `reload()` on a document with no engine |

A bad declaration raises pydantic's `ValidationError`, or `TypeError` for a setting that does not
exist.

## Upgrading from 2.0

3.0 keeps no compatibility code, so an upgrade is two steps: change the code, then upgrade the data
**once, with every 2.0 process stopped** — workers, and processes that only enqueue.

### Code

| 2.0 | 3.0 |
|---|---|
| `@task(maxAttempts=…, retryDelay=…)`; `taskMaxAttempts`, `taskRetryDelay` | gone: tasks are not retried; a task that must succeed retries inside its own code. Work that has to happen belongs on a pile |
| `pile(Email, itemsCollection="x", retryDelay=…)`; `itemMaxAttempts`, `itemRetryDelay` | `pile(Email, "x")`; `pileMaxAttempts`; no retry delay |
| `schedulers(schedulerModel=…, schedulersCollection=…)` | `schedulers(Model, collection)` — every declaration is `kind(Model, collection, extraIndexes=, …)` |
| `pile(payload=Email)` | `pile(Email)` |
| `App(db, distributionsRegistry=Mine)` | `distributions = Mine` on the class |
| `App(db, leaseSeconds=…)` | `taskLeaseSeconds`, `schedulerLeaseSeconds`, `pileLeaseSeconds` on the class, or `leaseSeconds=` on a declaration — shared, so fingerprinted |
| one task engine | `app.task`, plus any `tasks(...)` you declare; `schedulers(emitsInto=heavy)` picks one |
| `Scheduler.emitWork()` | gone: `Scheduler.taskFields()` hands fields to the task, and a `Task` subclass's `runWork()` stamps them — see [Context on emitted tasks](#context-on-emitted-tasks) |
| `startWorkers(taskWorkers=4, schedulerWorkers=1)` | the same, and each may be a dict by engine name; an int now means that many on **every** engine of the kind |
| `with pile.work() as item:` … `item.data` | `with pile.work() as w:` … `w.data`; `w.done()`, `w.fail()` and `w.release()` end the block |
| `fail(item)` with attempts left put it back | `fail()` is final; `release()` puts it back, returning the try |
| `done(uid)` / `fail(uid)` on any item | only on an unfinished one |
| `app.task.tasksCollection`, `app.tasksCollection` | `app.task.collection` |
| `app.defaultFactory` | `app.task.factory` |
| `engine.execute(task)`, `engine.validate(work)`, `engine.renewLeases()` | `engine.work()`; `schedule()` checks; leases renew themselves |
| `app.backlog()` → `(due, waiting)` | a dict of that by engine name; `app.taskWorkers()` likewise |
| a timeout only freed the worker | it also stops the call where it can, and can retire the process (`retireAfter=`) |

### Statuses and documents

| | 2.0 | 3.0 |
|---|---|---|
| tasks | `pending` `processing` `success` `failed` `timeout` `canceled` `outdated` `incompatible` | `pending` `running` `done` `failed` `timeout` `canceled` `outdated` `incompatible` |
| items | `pending` `claimed` `done` `failed` | `pending` `running` `done` `failed` `canceled` |
| collections | `pymonque_tasks`, `pymonque_schedulers` | `pymonque_task`, `pymonque_scheduler`; declared engines `pymonque_task_<name>`, `pymonque_scheduler_<name>` |
| fields | tasks carry `attempts` | tasks carry `claimId`, not `attempts`; held schedulers carry `claimId` |

A scheduler declared by name keeps its uid across the upgrade.

### Data

Stop every 2.0 process, then run once:

```python
from pymonque import upgradeFrom2

print(upgradeFrom2(App(db)))
# {"tasks renamed": 1200, "schedulers renamed": 14, "task statuses": 1180, "task attempts": 1200, "item statuses": 2}
```

It renames `pymonque_tasks` and `pymonque_schedulers` to `app.task`'s and `app.scheduler`'s
collections, and each declared scheduler engine's `pymonque_schedulers_<name>` to its
`pymonque_scheduler_<name>`; maps the task and item statuses above on every task engine and pile; and
drops the tasks' `attempts`. Collections 2.0 was told to use by name are kept, or, for the default
engines, named: `upgradeFrom2(app, tasks="my_tasks", schedulers="my_schedulers")`.

- It is refused while any worker process is registered live, of either version, and will not rename
  onto a collection that already holds documents.
- It is safe to run again: a second run changes nothing.
- A task 2.0 left `processing` becomes `running`, and the next claim writes it off as `failed` — its
  worker is gone. A task 2.0 held back for a retry is still `pending`, and runs once more. A claimed
  item becomes `running`, and the next claim deals with it by its tries.

Coming from 0.x: upgrade to 2.0 first, following "Upgrading from 0.x" in the 2.0.0 reference in this
repository's history.

## Known limitations

Deliberate trade-offs and edges not handled, so none comes as a surprise:

- **Two versions starting at the same instant can both pass the version check.** It reads the live
  workers, then checks in. Deploy one version at a time rather than relying on the check to settle a
  race.
- **The fingerprint sees signatures and schemas, not function bodies.**
- **A timeout stops Python code, not a call blocked outside it.** Such a call runs on in an abandoned
  daemon thread until it returns or the process exits; `retireAfter` bounds how many a process keeps.
  An abandoned call that later finishes can still record a pile item nobody has claimed since.
- **A process cut off from the database, or frozen — a paused VM, a suspended laptop, `SIGSTOP`, a
  long GC pause — for longer than its lease can have its work taken over while its own run carries
  on.** A lease cannot tell a frozen holder from a dead one. Only the current claim's outcome is
  recorded, and the other is logged. For a task that means it is written off as `failed` while it
  still runs; for a pile item, that it is **worked twice**. `w.confirm()` just before the side effect
  narrows this to the moment between the two; to close it, keep such work idempotent — an item's
  `uid` is a natural idempotency key to give the system it writes to.
- **A task whose worker died stays `running` until a worker of its engine claims next.** Only a claim
  writes it off, so with no workers on that engine, or all of them busy, it shows `running` long after
  its worker is gone, and `wait()` goes on waiting. It counts in the backlog, so the backlog warning
  says nobody is taking it; `cancel()` works on it, since its lease has lapsed.
- **`stopWorkers()` that times out still checks the process out,** while its unfinished threads may
  still be running. Exit the process afterwards.
- **`requestStop()` alone leaves the heartbeat running.** Call `stopWorkers()`, or use `run()`.
- **Limits are per task, not per call, and tries per pile, not per item.** They describe the work. To
  run one function under two sets of limits, declare two tasks.
- **A scheduler whose task is gone warns on every beat** until you remove it, disable it, or put the
  task back. One whose stored document no longer fits its model fails its claim after every lease,
  logged by the worker, until you migrate it — as any collection's documents would after a schema
  change.
- **Swallowing `BaseException` in a `work()` block** keeps it running after its outcome — see the
  caveat under [The work() block](#the-work-block).
- **`work()` treats `SystemExit` as a failure, like a task does, but not `KeyboardInterrupt`.** A Ctrl-C
  in the main thread leaves a claimed item for its lease to lapse.

## Notes

- Times are naive UTC (`utc_now()`); an aware datetime given to a task, scheduler or item is turned
  into naive UTC. MongoDB keeps millisecond precision.
- `model_dump()` is pydantic's own. Documents serialize with field aliases by default
  (`serialize_by_alias`), so a model can match an existing schema. `None` is stored as null, so a
  field can be cleared. MongoDB's `_id` is dropped on load.
- Worker threads are daemons. `stopWorkers()` lets work in flight finish; a process killed outright
  leaves its leases to lapse, and the next worker deals with its work ([Leases and claims](#leases-and-claims)).
