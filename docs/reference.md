# Reference

## BaseApp

```python
app = App(
    db,                                         # pymongo Database
    distributionsRegistry   = BaseDistributions,
    taskPoolInterval        = 1,                # seconds between polls
    schedulerPoolInterval   = 1,
    overdueTaskPolicy       = "execute now",
    overdueSchedulersPolicy = "execute once",
    staleItemsPolicy        = "retry",
)
```

| Attribute | What |
|---|---|
| `app.task` | `TaskEngine` |
| `app.scheduler` | `SchedulerEngine` — the default one |
| `app.schedulerEngines` | `dict[str, SchedulerEngine]`, also reachable as `app.<name>` |
| `app.distribution` | `DistributionEngine` |
| `app.piles` | `dict[str, PileEngine]`, also reachable as `app.<name>` |
| `app.defaultFactory` | `TaskFactory(name="default")` |
| `app.tasksCollection` | `pymonque_tasks` |
| `app.schedulersCollection` | `pymonque_schedulers` |

Constructor also takes `enforceVersion=True`, `heartbeatInterval=15`, `workerStaleAfter=60` —
see [One version at a time](#one-version-at-a-time).

## Running and shutting down

```python
app.run(taskWorkers=4, schedulerWorkers=1, timeout=30)   # start, handle signals, block
```

That is the whole of a worker process. For finer control:

| | |
|---|---|
| `startWorkers(taskWorkers=None, schedulerWorkers=None)` | Start threads on the task engine and every scheduler engine. |
| `requestStop()` | Stop claiming new work. Returns at once. |
| `stopWorkers(timeout=30)` | Stop claiming, wait for in-flight work, deregister. `False` if the timeout ran out. |
| `joinWorkers(timeout=None)` | Block until the workers stop. |
| `handleSignals(timeout=30, signals=(SIGINT, SIGTERM))` | Install graceful-shutdown handlers. |
| `restoreSignals()` | Put the previous handlers back. |
| `app.running` / `app.stopping` | Also on each engine. |

A stop is checked **between** iterations, never inside one, so work already claimed always runs
to completion. Anything not yet claimed stays `pending` for the next process. Workers wait on the
stop event rather than sleeping, so a shutdown doesn't sit through a poll interval.

`stopWorkers` deregisters the process, which frees its version slot immediately — the replacement
deployment can start without waiting out `workerStaleAfter`.

### Signals

`handleSignals()` is opt-in, because a host framework may want to own them — call it from a
process pymonque is running, not from inside someone else's server.

| | |
|---|---|
| First SIGINT/SIGTERM | Stop claiming, finish in-flight work, exit |
| Second | `os._exit(128 + signum)` immediately |
| SIGKILL | Cannot be caught. The process dies with work in flight. |

After a SIGKILL, the leases of whatever was in flight simply lapse and the next worker to poll
reclaims it. No boot is required, and no other worker is disturbed.

Collections and indexes are created on construction. Startup cleanup is not — it belongs to
`startWorkers()`. See [Startup housekeeping](#startup-housekeeping).

## Collections

Everything in pymonque is a typed collection of documents. `collection(Model)` gives you one
directly, with no task-queue behaviour attached.

```python
class Group(Document):          # Document gives it a uid
    name: str = ""

class App(BaseApp):
    groups   = collection(Group)                            # -> collection "groups"
    accounts = collection(Account, key="accountId")         # keyed by your own field
    legacy   = collection(Group, collection="old_groups")   # an existing collection
```

```python
collection(model: type[Document],
           collection: Collection | str | None = None,   # default: the attribute name
           key: str = "uid",
           extraIndexes: Sequence[IndexModel] | None = None)
```

They land in `app.collections` and are reachable as `app.<name>`. The key is uniquely indexed.

### CollectionEngine

| Method | What |
|---|---|
| `create(**fields)` | Build, store and return it, bound. |
| `build(**fields)` | Build it without storing. |
| `insert(doc)` / `insertMany(docs)` | Store what you built. |
| `save(doc)` | Replace the stored document, creating it if absent. |
| `update(key, **fields)` | Merge fields in without reading first. |
| `get(key)` / `findOne(where)` | One document, or `None`. |
| `find(where=None, sort=None, limit=None)` | `list[Model]`. |
| `count(where=None)` / `exists(key)` | |
| `delete(key)` / `deleteMany(where)` | |

`TaskEngine`, `SchedulerEngine` and `PileEngine` all inherit this, so `app.task.find(...)`,
`app.scheduler.count(...)` and `app.outbox.findOne(...)` work the same way everywhere. All three
document types subclass `Document`, so they can save and delete themselves too.

### Bound documents

A document that came from an engine remembers it:

```python
group = app.groups.create(name="beta")
group.name = "gamma"
group.save()            # no engine argument needed
group.delete()
fresh = group.reload()
```

One you built by hand is unbound until you store it — `save()` on an unbound document raises
`UnboundDocument` rather than guessing where it belongs. `bound` tells you which it is. The
binding is never written to MongoDB.

## Tasks

Declare with `@task`, above `@staticmethod` if you use one:

```python
class App(BaseApp):
    @task
    @staticmethod
    def send(to: str, subject: str = ""): ...

    @task
    def with_self(self): ...       # instance methods get the app
```

| Access | Returns |
|---|---|
| `App.send` | `FuncSpec`; call it to build a `CallSpec` |
| `app.send(...)` | the real function, executed now |
| `app.task("send", ...)` | a validated `CallSpec`, by name |

### TaskEngine

| Method | What |
|---|---|
| `schedule(work, deadline=None, factory=None)` | Store a task. Deadline defaults to now. |
| `scheduleFromDistribution(work, distribution, factory=None)` | Deadline is now + one interval. |
| `__call__(functionName, **kwargs)` | Build and validate a `CallSpec`. |
| `validate(work)` | Raises `TaskNotFound` / `TaskValidationError`. |
| `execute(task)` | Run one task, return it filled in. |
| `startWorkers(n)` | Poll, claim, execute. |

Claiming is `find_one_and_update` on `{status: "pending", deadline: {$lte: now}}` sorted by
deadline, so the oldest due task goes first and no task runs twice.

### Task document

| Field | Type |
|---|---|
| `uid` | `str` |
| `status` | `pending` `processing` `success` `failed` `canceled` `outdated` `incompatible` |
| `work` | `CallSpec` |
| `deadline` | `datetime` (naive UTC) |
| `factory` | `TaskFactory` — `{uid, name}` |
| `executionTime` | `timedelta`, stored as seconds |
| `result` | anything BSON can encode |
| `error` | traceback, on failure |

```
pending ─→ processing ─→ success
                ├──────→ failed
                └──────→ processing    lease lapsed; another worker claimed it

pending ─→ outdated       overdue, under the "skip" policy
pending ─→ incompatible   the function no longer exists on the app
```

`canceled` is no longer written — an abandoned task is recovered by its lease rather than being
cancelled at startup. It stays in the type so older documents still validate.

A result the driver cannot encode is stored as a `failed` task, not left claimed.

### TaskFactory

Tags who emitted a task, so you can query by source.

```python
web = TaskFactory(name="web-api")
app.task.schedule(work, factory=web)

app.task.tasksCollection.find({"factory.name": "web-api"})
```

## Schedulers

### Declaring engines

An app can have any number of scheduler engines, each with its own collection and model, all
emitting into the one task engine.

```python
class App(BaseApp):
    globalOps  = schedulers()                                    # pymonque_schedulers_globalOps
    accountOps = schedulers(AccountScheduler, "account_ops")
    groupOps   = schedulers(GroupScheduler, Groups_Collection, policy="skip")
```

```python
schedulers(schedulerModel: type[Scheduler] = Scheduler,
           schedulersCollection: Collection | str | None = None,
           policy: OVERDUE_SCHEDULES_POLICY | None = None,   # default: the app's
           poolInterval: float | None = None,                # default: the app's
           taskEngine: TaskEngine | None = None,             # default: app.task
           extraIndexes: Sequence[IndexModel] | None = None)
```

They land in `app.schedulerEngines` and are reachable as `app.<name>`. `init()` runs on all of
them at construction and `app.startWorkers()` starts all of them. Declaring one named
`scheduler` replaces the default engine instead of adding to it.

### SchedulerEngine

| Method | What |
|---|---|
| `add(work, distribution, **fields)` | Create a new scheduler. Every call creates another one. |
| `ensure(name, work, distribution, enabled=None, **fields)` | Declare one by name. Idempotent. |
| `build(work, distribution, deadline=None, **fields)` | An unsaved scheduler of this engine's model. |
| `upsert(scheduler)` | Store one under its own uid, creating or replacing. |
| `update(uid, work=None, distribution=None, enabled=None, **fields)` | Change parts of one; `None` if no such uid. |
| `get(uid)` / `byUid(uid)` | `Scheduler` or `None`. |
| `byName(name)` | The one declared under that name by `ensure()`. |
| `find(where=None)` / `count(where=None)` | Query the collection. |
| `delete(uid)` / `removeNamed(name)` | `True` if one was deleted. |
| `deleteMany(where)` | How many were deleted. |
| `validate(work, distribution)` / `validateScheduler(scheduler)` | Raise if it wouldn't run. |
| `startWorkers(n)` | Poll and fire. |

`**fields` are the extra fields of your `Scheduler` subclass. A new distribution passed to
`update` restarts the rhythm from now; anything else leaves the deadline alone.

Everything that writes validates the scheduler *as it will be emitted* — `emitWork()`, not
`work` — so context fields are checked against the task signature.

On each fire the scheduler emits a task dated to its own deadline, then advances that
deadline by a fresh interval — measured from the old deadline, so the rhythm does not drift.
A `disabled` scheduler advances without emitting.

### ensure

The name is hashed into the scheduler's `uid`, which is uniquely indexed. Creation is an
upsert, so processes booting together produce one scheduler, not one each.

| You change | What happens |
|---|---|
| the work | updated in place, rhythm untouched |
| the distribution | updated, next deadline recomputed from now |
| nothing | nothing; restarts never reset the deadline |

`enabled` is left as the database has it unless you pass it, so disabling a scheduler in
production survives a deploy. Dropping the `ensure()` call does not delete the scheduler —
use `remove()`.

The name also lands on every emitted task as `factory.name`.

### Context on emitted tasks

`Scheduler.emitWork()` returns the call the scheduler emits. Override it in a subclass to
stamp context onto every task, instead of baking it into the stored `work`:

```python
class AccountScheduler(Scheduler):
    accountId: int

    def emitWork(self) -> CallSpec:
        return self.work.bind(accountId=self.accountId)
```

`CallSpec.bind(**kwargs)` returns a copy with the kwargs merged in; the original is untouched.

Point an engine at the subclass with `schedulerModel`, and it reads, writes, validates and
emits with it:

```python
accountOps = schedulers(AccountScheduler, "account_ops")

app.accountOps.add(App.sync(), dist, accountId=42)   # stored work: sync()
                                                        # emitted task: sync(accountId=42)
```

The stored `work` stays context-free, so one declaration serves every account, and validation
happens against the real emitted call rather than a placeholder.

### Scheduler document

| Field | Type |
|---|---|
| `uid` | `str` — derived from the name when created by `ensure` |
| `name` | `str`, default `"Scheduler"` |
| `status` | `enabled` `disabled` `processing` |
| `work` | `CallSpec` emitted on each fire |
| `distribution` | `CallSpec` producing the interval |
| `deadline` | `datetime` |

A scheduler's `status` is only ever `enabled` or `disabled`; being worked is a live lease, not a
status. `processing` is no longer written and remains only so older documents validate.

## Distributions

All take `dailyFrequency` — average runs per day — and return a `timedelta`.

| Name | Extra argument |
|---|---|
| `constant` | — |
| `normal` | `stdFraction` |
| `lognormal` | `sigma` |
| `exponential` | — |

```python
dist = app.distribution("normal", dailyFrequency=10, stdFraction=0.2)
```

Add your own by subclassing and passing the registry:

```python
class MyDistributions(BaseDistributions):
    @staticmethod
    def work_hours(dailyFrequency: float) -> timedelta:
        return timedelta(seconds=28800 / dailyFrequency)

app = App(db, distributionsRegistry=MyDistributions)
```

Staticmethods only, not starting with `_`. Built-ins stay available.

## Piles

```python
class App(BaseApp):
    outbox = pile(Email)                          # payload validated against Email
    scraps = pile()                               # payload is any dict
    other  = pile(Email, itemsCollection="x")     # explicit collection
    strict = pile(Email, policy="fail")           # override staleItemsPolicy
```

Each pile gets `pymonque_pile_<name>` unless told otherwise.

### PileEngine

| Method | What |
|---|---|
| `add(model \| dict \| **kwargs)` | Insert one item. |
| `addMany(iterable)` | Insert many; validates all before inserting any. |
| `claim(where=None)` | Atomically take the oldest pending item, or `None`. |
| `work(where=None)` | Context manager: claim, then done, or failed if the block raises. |
| `done(item, result=None)` | |
| `fail(item, error=None)` | |
| `release(item)` | Put it back as pending. |
| `count(where=None, status=None)` / `counts()` | |
| `find(where=None)` | `list[Item]`. |
| `purge(status="done")` | Delete, return how many. |

`item` may be an `Item` or a uid.

```python
with app.outbox.work() as item:
    if item is None:
        return
    send(item.data.to)
```

Piles run no workers of their own. Drive one from a task, and fire that task from a
scheduler.

### Item document

| Field | Type |
|---|---|
| `uid` | `str` |
| `status` | `pending` `claimed` `done` `failed` |
| `data` | the payload |
| `createdAt` / `claimedAt` / `finishedAt` | `datetime` |
| `attempts` | `int`, bumped on every claim |
| `result` / `error` | |

```
pending ─→ claimed ─→ done
              ├────→ failed
              └────→ pending    release(), or a restart under "retry"
```

## One version at a time

**Only one version of your code may run workers against a database at once.** Deploy all-or-nothing:
stop the old workers, then start the new ones.

The reason is that a task document names a function, and nothing more. Two deployments can agree
on every name and signature while one of them does something entirely different — there is no way
to tell from inside Python, since a one-line task can call into a module that changed completely.
So the library does not try to reconcile versions; it refuses to let them run together.

`app.fingerprint` is a short hash of the app's executable surface — every task and distribution
name with its signature:

```python
app.fingerprint          # "3f9c1a02b7de"
```

`startWorkers()` registers the process in `pymonque_workers` and refuses to start if a worker that
has checked in recently reports a different fingerprint:

```
VersionMismatch: a live worker is running a different version of this app
(a41f0b93c2e1 on box-2:4471, this process is 3f9c1a02b7de). Only one version may run
at a time — stop the old workers before starting these.
```

| | |
|---|---|
| `app.fingerprint` | the hash for this process |
| `app.liveWorkers()` | worker processes that have checked in recently |
| `enforceVersion=False` | skip the check entirely |

Workers check in every `heartbeatInterval` seconds and are considered gone after
`workerStaleAfter`. So a stopped deployment stops blocking the next one on its own — no manual
cleanup — and a crashed process frees its slot within a minute.

**What the fingerprint can and cannot see.** It sees added, removed, or re-signatured tasks and
distributions. It does **not** see a changed function body, and cannot: that would require hashing
every transitive dependency. It is a guard against the obvious mistake, not a proof of identity.
The rule is the guarantee; the hash only enforces the part of it that is mechanically checkable.

Constructing an app is never refused — only starting workers is. Any process may enqueue tasks
and query results regardless of version.

## Leases

Every task, scheduler and item carries `leaseUntil` — **the one field that says when it is
claimable.** For something waiting, it equals the deadline (or `createdAt` for a pile item). For
something being worked, it is the end of the holder's lease.

So a claim is one comparison, and it covers both cases at once:

```python
{"status": {"$in": ["pending", "processing"]}, "leaseUntil": {"$lte": now}}
```

While a worker holds something it renews the lease in the background, every `leaseSeconds / 3`.
If that worker dies, nothing renews, the lease lapses, and the next worker claims it — with no
restart, no sweep, and no coordination. `leaseSeconds` defaults to 300 and is settable on the
app or per engine.

This is **at-least-once**: a worker that hangs long enough for its lease to lapse can have its
work picked up while it is still running. Make tasks idempotent, or set a lease longer than the
longest task.

| | |
|---|---|
| `engine.renewLeases()` | Extend the lease on everything this process holds. |
| `pile.renewLease(item)` | Same, for one item. |

Because a live lease is visible in the document, **constructing an app never disturbs work in
flight** — any process may connect, enqueue and read at any time.

## Startup housekeeping

`init()` backfills leases onto documents written by an older version, flags tasks whose
function is gone, and applies the task and pile policies. It is called by `startWorkers()` —
a process that is actually taking over as a worker — and **never by the constructor**.

```python
app = App(db)      # safe from any process, changes nothing
app.startWorkers(4)     # runs init(), then starts working
app.init()              # or call it yourself
```

That split is what keeps an API process, a CLI script and a worker container able to share one
database. Everything `init()` still does is scoped to documents nobody holds, so it cannot take work
away from a running worker.

## Policies

`overdueTaskPolicy` and `staleItemsPolicy` are applied by `init()`, to whatever the last run
left behind. `overdueSchedulersPolicy` is applied every time a scheduler is claimed, because a
scheduler falls behind while an app is running just as easily as while it is down.

| `overdueTaskPolicy` | |
|---|---|
| `"execute now"` | run overdue tasks as normal (default) |
| `"skip"` | mark overdue pending tasks `outdated` |

| `overdueSchedulersPolicy` | when a whole beat has gone by unworked |
|---|---|
| `"execute once"` | emit one task, then resume from now (default) |
| `"execute reconstructed"` | replay the backlog beat by beat, one per poll |
| `"skip"` | emit nothing, resume from now |

A scheduler that is merely due — its deadline has passed but the next one has not — emits
normally under all three. The policy only decides what is owed for beats that were missed.
`"execute reconstructed"` is the only one that can stay permanently behind: a scheduler set
faster than its workers can serve it will keep a backlog forever. It logs `missed a beat` on
every claim so you can see that happening.

| `staleItemsPolicy` | what happens to an item whose holder stopped renewing |
|---|---|
| `"retry"` | it becomes claimable again on its own (default) — no sweep needed |
| `"fail"` | it is never retried, and `init()` marks it failed |

`init()` also flags pending tasks whose function is gone as `incompatible`, disables schedulers
that emit a missing task, and backfills `leaseUntil` onto pre-lease documents.

A worker never claims a task it cannot run — the claim filters on the function names it has — so
an unrecognised task waits rather than failing.

## Reading results

Documents are plain MongoDB. Deserialize with pydantic:

```python
from pymonque import Task

for raw in app.task.tasksCollection.find({"status": "failed"}):
    t = Task.model_validate(raw)
    print(t.uid, t.error, t.executionTime)
```

## Exceptions

`from pymonque.exceptions import ...`

| | Raised when |
|---|---|
| `TaskNotFound` | no such task on the app |
| `TaskValidationError` | kwargs don't match the signature |
| `DistributionNotFound` | no such distribution in the registry |
| `DistributionValidationError` | bad kwargs, or it didn't return a `timedelta` |
| `VersionMismatch` | `startWorkers()` found a live worker on a different fingerprint |
| `UnboundDocument` | `save()`/`delete()`/`reload()` on a document with no engine |

## Notes

- Times are naive UTC (`utc_now()`). Mongo keeps millisecond precision.
- `model_dump()` is pydantic's own. Documents serialize with field aliases by default
  (`serialize_by_alias`), so a model can match an existing schema; pass `by_alias=False`
  or `mode="json"` when you want something else. `None` is stored as null, so a field
  can be cleared.
- Workers are daemon threads. `stopWorkers()` lets work in flight finish; a process killed
  outright leaves its leases to lapse, and the next worker reclaims them.
- Nothing caps retries. A pile item that kills its worker is retried on every restart;
  `attempts` is there to build a cap on.
