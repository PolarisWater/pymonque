# Reference

## BaseApp

```python
class App(BaseApp):
    schedulerMissed         = "once"            # defaults are declared, not passed
    taskTimeout             = None              # for every task; @task(...) sets a task's own
    taskSkipAfter           = None
    taskMaxAttempts         = 1
    taskRetryDelay          = 60
    itemMaxAttempts         = 1                 # for every pile; pile(...) sets a pile's own
    itemRetryDelay          = 60

app = App(
    db,                                         # pymongo Database
    distributionsRegistry   = BaseDistributions,
    taskPollInterval        = 1,                # seconds between polls
    schedulerPollInterval   = 1,
)
```

Defaults are class attributes, never constructor arguments, and what overrides them is declared in
code too — [limits](#limits) on `@task(...)` and `pile(...)`, [missed beats](#missed-beats) on
`schedulers(...)`. Every process that imports the app has to agree on all of it, or one would retry
a failure another treats as final, so it is part of the [fingerprint](#one-version-at-a-time) and two
that disagree cannot both run workers. Pacing and lease lengths are per-process and stay arguments.

**Declarations are checked where they are written.** A bad setting on `@task(...)`, `pile(...)`,
`schedulers(...)` or `collection(...)`, or a bad default on the class, raises pydantic's
`ValidationError` naming the setting, as the class is defined. Leaving a setting out is the only way
to take the default: `None` is refused everywhere except `timeout` and `skipAfter`, where it means no
limit.

A declaration can't reuse a name `BaseApp` already has — a pile called `backlog`, a task called
`init` or `run` — because it would replace that part of the app; the class raises `TypeError` when
it is defined. The one exception is `scheduler = schedulers(...)`, which replaces the default engine.

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
| `handleSignals(signals=(SIGINT, SIGTERM))` | Install graceful-shutdown handlers. |
| `restoreSignals()` | Put the previous handlers back. |
| `app.running` / `app.stopping` | Also on each engine. |

A stop is checked **between** iterations, never inside one, so work already claimed always runs
to completion. Anything not yet claimed stays `pending` for the next process. Workers wait on the
stop event rather than sleeping, so a shutdown doesn't sit through a poll interval.

While work drains, the process keeps renewing its leases and checking in, so nothing it is still
running gets picked up elsewhere and the version check still sees it. `stopWorkers` then
deregisters the process, which frees its version slot immediately — the replacement deployment can
start without waiting out `workerStaleAfter`.

### Signals

`handleSignals()` is opt-in, because a host framework may want to own them — call it from a
process pymonque is running, not from inside someone else's server.

| | |
|---|---|
| First SIGINT/SIGTERM | Stop claiming, finish in-flight work, exit |
| Second | `os._exit(128 + signum)` immediately |
| SIGKILL | Cannot be caught. The process dies with work in flight. |

After a SIGKILL, the leases of whatever was in flight simply lapse and the next worker to poll
picks it up — no boot is required, and no other worker is disturbed. What it then does depends on
what it picked up; see [Leases](#leases).

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
           collection: Collection | str,                 # left out: the attribute name
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
| `update(key, **fields)` | Merge fields in, validated against the model before anything is written. Only what changed is written. `None` if there is no such document. |
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

A bound document also remembers the key it was stored under, so **changing the key renames it**
rather than leaving the old row behind as a copy:

```python
account = app.accounts.create(accountId=1)   # collection(Account, key="accountId")
account.accountId = 2
account.save()          # one row, now keyed 2 -- not two rows

account.storedKey       # 2, the key it was last written under
```

`delete()` and `reload()` use the stored key too, so they act on the row the document came from
whatever you have since done to the field in memory.

## Tasks

Declare with `@task`, above `@staticmethod` or `@classmethod` if you use one:

```python
class App(BaseApp):
    @task
    @staticmethod
    def send(to: str, subject: str = ""): ...

    @task
    def with_self(self): ...       # instance methods get the app

    @task
    @classmethod
    def with_class(cls): ...       # class methods get the app's class

    @task(timeout=60, maxAttempts=3)
    def with_limits(self): ...     # see Limits
```

A subclass that redefines a task's name as something else — a plain method, a pile — replaces the
task; it is not left registered underneath.

| Access | Returns |
|---|---|
| `App.send` | `FuncSpec`; call it to build a `CallSpec` |
| `app.send(...)` | the real function, executed now |
| `app.task("send", ...)` | a validated `CallSpec`, by name |

**Signatures.** A task is always called with keyword arguments, because a `CallSpec` is
`{functionName, kwargs}` and has nowhere to put a positional one. So `*args` and positional-only
parameters raise a `TypeError` when the app class is built, rather than failing every call later.
`**kwargs` is fine — the named parameters are still checked and anything else is passed through.

Arguments are validated against the signature when you **schedule**, not when a worker picks the
task up. Unannotated parameters accept anything; annotated ones are checked by pydantic, so a
constraint declared on the parameter is enforced at schedule time:

```python
@task
@staticmethod
def retain(days: Annotated[int, Field(gt=0)]): ...

app.task.schedule(App.retain(days=0))    # TaskValidationError, before it is ever stored
```

They are validated again when the call runs, so the function receives what its annotations
promise: a model argument arrives as the model, not the dict it was stored as, and a value
pydantic coerced (`"3"` for an `int`) arrives coerced. Distributions are called the same way.

### TaskEngine

| Method | What |
|---|---|
| `schedule(work, deadline=None, factory=None)` | Store a task, and return it. Deadline defaults to now. |
| `scheduleFromDistribution(work, distribution, ...)` | Same, deadline is now + one interval. |
| `__call__(functionName, **kwargs)` | Build and validate a `CallSpec`. |
| `validate(work)` | Raises `TaskNotFound` / `TaskValidationError`. |
| `execute(task)` | Run one task, return it filled in. |
| `limits` | `dict[str, TaskLimits]`: what each task resolved to, from its `@task(...)` and the app's defaults. |
| `limitsFor(task)` | The `TaskLimits` a stored task runs under — its function's. |
| `wait(task, timeout=None, interval=0.1)` | Block until it finishes and return it. `TimeoutError` if it doesn't in time. From any process. |
| `backlog()` | `(due, secondsTheOldestHasWaited)` — on `BaseApp`. |
| `startWorkers(n)` | Poll, claim, execute. |

Claiming is one `find_one_and_update` on `{status: {$in: ["pending", "processing"]}, leaseUntil:
{$lte: now}}`, sorted by `leaseUntil`, so the longest-waiting task goes first and no task runs
twice. See [Leases](#leases) for why one field covers both due and abandoned.

### Task document

| Field | Type |
|---|---|
| `uid` | `str` |
| `status` | `pending` `processing` `success` `failed` `timeout` `canceled` `outdated` `incompatible` |
| `work` | `CallSpec` |
| `deadline` | `datetime` (naive UTC) |
| `factory` | `TaskFactory` — `{uid, name}` |
| `createdAt` / `claimedAt` / `finishedAt` | `datetime` — the same three moments an item records. `claimedAt` is cleared while a task waits for a retry; `finishedAt` is set once the status is final, including `canceled`, `outdated` and `incompatible` |
| `executionTime` | `timedelta`, stored as seconds |
| `result` | anything BSON can encode |
| `error` | traceback of the last failure |
| `attempts` | `int`, bumped on every claim — a crashed run counts |

A task stores no limits: it runs under the ones its function declares.

```
pending ─→ processing ─→ success
                ├──────→ pending       the call raised, attempts are left
                ├──────→ failed        the call raised on the last attempt
                ├──────→ timeout       the call outlived its limit (never retried)
                └──────→ processing    lease lapsed; another worker claimed it
                                       (or failed, if that was the last attempt)

pending ─→ outdated       claimed past skipAfter
pending ─→ canceled       cancel() before it started
pending ─→ incompatible   the function no longer exists on the app
```

A result the driver cannot encode is stored as a `failed` task, not left claimed.

## Limits

What a task may do is part of declaring it, because it describes the function — how long it may
run, whether running it again is safe — not one call of it:

```python
class App(BaseApp):
    taskTimeout = 300                                   # the default for every task

    @task(timeout=900, maxAttempts=3, retryDelay=30)    # this task's own
    def sync(self, accountId: int): ...

    @task(skipAfter=None)                               # None: no limit, whatever the default
    @staticmethod
    def charge(orderId: str): ...
```

| Limit | App default | What |
|---|---|---|
| `timeout` | `taskTimeout = None` | seconds a call may run before it is written `timeout`; `None`: no limit |
| `skipAfter` | `taskSkipAfter = None` | seconds past its deadline a task is still worth running; `None`: however late |
| `maxAttempts` | `taskMaxAttempts = 1` | runs before a failure is final |
| `retryDelay` | `taskRetryDelay = 60` | seconds a failed task waits before it is claimable again |

A limit left off `@task(...)` takes the app's default, and that is all: a scheduled call carries no
limits of its own, and neither does a scheduler, so a task emitted by one runs under the same limits
as any other call of that task. To run a function under different limits, declare a second task.

Limits are checked where they are written — a bad one on `@task(...)`, or a bad default, fails as
the class is defined. `app.task.limits` holds what every task resolved to, and all of it
is in the [fingerprint](#one-version-at-a-time): whether a task ends up `timeout`, `outdated` or
retried must not depend on which process picked it up.

### Timeouts

When `timeout` passes, the task is written `timeout` with the reason in `error`, the worker is
freed, and a warning is logged. **The call itself is not interrupted** — Python cannot do that —
so it runs on in a daemon thread until it returns. What a timeout guarantees is that the worker
and the document stop waiting on it, which is what every other process can see. A task that must
actually stop needs to check something itself.

Without a timeout, the call runs on the worker thread; the extra thread only appears when a limit
applies. A timed-out task is **not retried**: its call may still be running, and a second attempt
would run it twice at once.

### skipAfter

If a worker claims a task more than `skipAfter` seconds past its deadline, it is written
`outdated` with how late it was, and never run. It is the one rule for stale work, whatever made
it stale: the app was down, or nobody kept up.

A retry keeps its original deadline, so a task that keeps failing can age past `skipAfter` and be
outdated between attempts.

### Retries

**Off by default.** A task runs once, and a failure is final: rerunning a side effect nobody asked
to rerun — an upload, a payment — is worse than a failure you can see. Opt in with `maxAttempts`
where a task is safe to run again.

A task that raises with attempts left is put back as `pending`, claimable after `retryDelay`
seconds. `error` holds the last failure, and is cleared if a later attempt succeeds. `wait()`
treats a task waiting for a retry as unfinished.

**A crash counts as an attempt.** A worker that dies mid-task leaves it `processing` with a lapsing
lease, and the next worker reclaims it — but if that claim would exceed `maxAttempts`, the task is
written `failed` instead of run. So a task that takes its worker down with it (out of memory, a
segfault, a killed container) is given up on rather than taking every worker down in turn. Under
the default of one attempt, that means a hard-killed task is failed, not rerun — nobody knows how
far it got. A graceful shutdown lets work in flight finish, so only a kill leaves one behind.

## Not enough workers

`BaseApp` watches how long work sits due. A free worker claims within one poll interval, so
anything waiting much longer means every worker is busy.

```python
app.backlog()       # (4, 31.2) -- four due, oldest waiting 31 seconds
app.taskWorkers()   # task workers across every process that has checked in
```

Once the oldest has waited `backlogWarnAfter` seconds, it says so, at `startWorkers()` and every
`backlogInterval` after:

```
4 task(s) due, oldest waiting 31s, 2 task worker(s) running -- not enough workers,
or they are all on long tasks

4 task(s) due, oldest waiting 31s, and no process is running task workers
```

| | |
|---|---|
| `backlogWarnAfter=60` | seconds due work may wait before warning. `None` turns it off |
| `backlogInterval=30` | seconds between checks |

The count spans processes — a scheduler-only box reports the workers on the worker boxes, not
zero. Every worker process registers, whether or not it enforces the version.

Schedulers have their own version of this: an enabled one that cannot keep its cadence logs
`missed a beat`.

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
    groupOps   = schedulers(GroupScheduler, Groups_Collection, missed="skip")
```

```python
schedulers(schedulerModel: type[Scheduler] = Scheduler,
           schedulersCollection: Collection | str,          # left out: pymonque_schedulers_<name>
           missed: "skip" | "once" | "replay",              # left out: the app's schedulerMissed
           pollInterval: float,                             # left out: the app's schedulerPollInterval
           extraIndexes: Sequence[IndexModel] | None = None,
           leaseSeconds: float)                             # left out: the app's leaseSeconds
```

They land in `app.schedulerEngines` and are reachable as `app.<name>`. `app.startWorkers()` starts
all of them. Declaring one named
`scheduler` replaces the default engine instead of adding to it.

### SchedulerEngine

| Method | What |
|---|---|
| `add(work, distribution, **fields)` | Create a new scheduler. Every call creates another one. |
| `ensure(name, work, distribution, enabled=None, **fields)` | Declare one by name. Idempotent. |
| `build(work, distribution, deadline=None, **fields)` | An unsaved scheduler of this engine's model. |
| `upsert(scheduler)` / `scheduler.save()` | Store one under its own uid, creating or replacing. The lease follows the deadline, so a deadline changed by hand takes effect. |
| `update(uid, work=None, distribution=None, enabled=None, **fields)` | Change parts of one; `None` if no such uid. |
| `get(uid)` | `Scheduler` or `None`. |
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
A `disabled` scheduler advances without emitting. So does one whose task the app no longer has,
logging a warning each beat — it stays `enabled`, so putting the task back resumes it.

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
use `removeNamed()`.

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
| `status` | `enabled` `disabled` |
| `work` | `CallSpec` emitted on each fire |
| `distribution` | `CallSpec` producing the interval |
| `deadline` | `datetime` |

A scheduler carries no limits: what it emits runs under its task's.

Being worked is a live lease, not a status. A disabled scheduler is still claimed and still walks
its deadline, so its distribution keeps its shape for when it is enabled again — it just emits
nothing, and has nothing to warn about.

## Distributions

All take `dailyFrequency` — average runs per day — and return a `timedelta`. It must be
positive: zero divides, and a negative interval walks a scheduler backwards, which no missed-beats
rule can stop. `DistributionValidationError` either way, and `gen()` rejects a non-positive
interval from a custom distribution too.

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

Staticmethods only, not starting with `_`. Built-ins stay available. Yours must return a
positive `timedelta`.

## Piles

```python
class App(BaseApp):
    outbox = pile(Email)                          # payload validated against Email
    scraps = pile()                               # payload is any dict
    other  = pile(Email, itemsCollection="x")     # explicit collection
    retried = pile(Email, maxAttempts=5, retryDelay=10)   # override itemMaxAttempts / itemRetryDelay
```

```python
pile(payload: type[BaseModel] | None = None,        # None: items carry any dict
     itemsCollection: Collection | str,             # left out: pymonque_pile_<name>
     maxAttempts: int,                              # left out: the app's itemMaxAttempts
     retryDelay: float,                             # left out: the app's itemRetryDelay
     leaseSeconds: float,                           # left out: the app's leaseSeconds
     extraIndexes: Sequence[IndexModel] | None = None)
```

Each pile gets `pymonque_pile_<name>` unless told otherwise.

### PileEngine

| Method | What |
|---|---|
| `add(model \| dict \| **kwargs)` | Insert one item. |
| `addMany(iterable)` | Insert many; validates all before inserting any. |
| `claim(where=None)` | Atomically take the oldest claimable item, or `None`. One whose worker died on its last attempt is given up on instead. |
| `work(where=None)` | Context manager: claim, then `done`, or `fail` if the block raises. |
| `done(item, result=None)` | `True`, or `False` if nothing matched: no such item, or — given the claimed `Item` — its claim has since passed to another worker. A bare uid acts whatever the claim. |
| `fail(item, error=None)` | Same. Given the claimed `Item` with attempts left, puts it back to retry; a bare uid is final. |
| `release(item)` | Put a claimed item back as pending, without using up an attempt. Same. |
| `count(where=None, status=None)` / `counts()` | |
| `find(where=None)` | `list[Item]`. |
| `purge(status="done")` | Delete, return how many. |

`item` may be an `Item` or a uid. An `Item` stands for the claim that handed it out, so passing one
that never came from `claim()` or `work()` — the return value of `add()`, say — raises `ValueError`;
pass its uid to act on it by hand.

```python
with app.outbox.work() as item:
    if item is None:
        return
    send(item.data.to)
```

Piles run no workers of their own. Drive one from a task, and fire that task from a
scheduler.

### Retries

The same rule as [tasks](#retries), one level shorter: the pile's `maxAttempts` / `retryDelay`,
else the app's `itemMaxAttempts` / `itemRetryDelay`. Off by default. A failed item with attempts
left goes back as `pending`, claimable after `retryDelay`. A crash counts: an item whose worker died
on its last attempt is written `failed` at the next claim rather than handed out again.

### Item document

| Field | Type |
|---|---|
| `uid` | `str` |
| `status` | `pending` `claimed` `done` `failed` |
| `data` | the payload |
| `createdAt` / `claimedAt` / `finishedAt` | `datetime` |
| `attempts` | `int`, bumped on every claim — a crashed run counts, a `release()` gives it back |
| `claimId` | `str`, new on every claim. `done`/`fail`/`release`/`renewLease` given an `Item` only apply while it still matches, so a worker whose lease lapsed cannot overwrite the worker that took over; `work()` logs when that happens |
| `result` / `error` | |

```
pending ─→ claimed ─→ done
              ├────→ failed     out of attempts, or failed by uid
              ├────→ pending    fail() with attempts left, or release()
              └────→ claimed    lease lapsed; another worker claimed it
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
| `enforceVersion=False` | skip the check; the process still registers, so it is still counted |

Workers check in every `heartbeatInterval` seconds and are considered gone after
`workerStaleAfter`. So a stopped deployment stops blocking the next one on its own — no manual
cleanup — and a crashed process frees its slot within a minute.

**What the fingerprint can and cannot see.** It sees added, removed, or re-signatured tasks and
distributions, every task's and pile's limits, and each scheduler engine's `missed`. It does **not** see a changed function body, and cannot: that would require hashing
every transitive dependency. It is a guard against the obvious mistake, not a proof of identity.
The rule is the guarantee; the hash only enforces the part of it that is mechanically checkable.

Constructing an app is never refused — only starting workers and running housekeeping are. Any
process may enqueue tasks and query results regardless of version.

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
app or per engine. What the next worker does with it:

| it claimed | it |
|---|---|
| a task | counts the dead run as an attempt: reruns it if attempts are left, else writes it `failed` — so under the default of one, a killed task is failed, not rerun ([Retries](#retries)) |
| a scheduler | emits the beat if the dead worker hadn't — each beat's task has a fixed uid, so it is never emitted twice |
| a pile item | the same as a task ([pile retries](#retries-1)) |

A renewal only stops when the process does, or loses the database. A worker cut off for longer
than its lease can have its work picked up while its own run carries on, so the same call can run
twice at once. **Only the current claim writes an outcome**: every claim bumps a task's `attempts`
and gives an item a new `claimId`, and a write from an older claim matches nothing and is logged —
which also keeps a late finish from overwriting a `cancel()`.

| | |
|---|---|
| `engine.renewLeases()` | Extend the lease on everything this process holds. |
| `pile.renewLease(item)` | Same, for one item. |

Because a live lease is visible in the document, **constructing an app never disturbs work in
flight** — any process may connect, enqueue and read at any time.

## Startup housekeeping

```python
app = App(db)           # safe from any process, changes nothing
app.startWorkers(4)     # runs init(), then starts working
app.init()              # or call it yourself
```

`init()` is called by `startWorkers()` — a process that is actually taking over as a worker —
and **never by the constructor**. That split is what keeps an API process, a CLI script and a
worker container able to share one database.

Almost nothing is left in it, because a condition that can change while an app is running is
resolved where it is observed rather than at boot:

| condition | when it can change | resolved |
|---|---|---|
| a scheduler fell behind | continuously | at the claim |
| a worker died holding a pile item | continuously | at the claim |
| a lease lapsed | continuously | at the claim |
| a task went stale (`skipAfter`) | continuously | at the claim |
| a task or item ran out of attempts | continuously | at the claim |
| a scheduler's task is gone | only at boot | at the claim: the beat is skipped, the status left alone |
| **the set of tasks that exist** | **only at boot** | **`init()`** |

What `init()` still does: **flags pending tasks whose function is gone** as `incompatible`.

That belongs at boot rather than at a claim, and not as a compromise: the fingerprint forces
a full restart to change an app's task list, so boot is the only moment that set can change.

`init()` is **version-checked**, like `startWorkers()`. It decides what is runnable from *this*
process's task list, so a process holding a different one must not run it — otherwise a script
importing half the app could write off a live deployment's tasks. Everything it does is
otherwise scoped to documents nobody holds, so it cannot take work from a running worker.

## Missed beats

What a scheduler owes for beats that went by unworked. Declared per scheduler engine,
`schedulers(..., missed="skip")`, with the app's `schedulerMissed` for the engines that don't, and
applied every time a scheduler is claimed — a scheduler falls behind while an app is running just as
easily as while it is down.

| `missed` | when a whole beat has gone by unworked |
|---|---|
| `"once"` | emit one task, then resume from now (default) |
| `"replay"` | emit every missed beat, one per poll, keeping the original rhythm |
| `"skip"` | emit nothing, resume from now |

A scheduler that is merely due — its deadline has passed but the next one has not — emits
normally under all three; `missed` only decides what is owed for beats that were missed.
`"replay"` is the only one that can stay permanently behind: a scheduler set faster than its
workers can serve it will keep a backlog forever. It logs `missed a beat` on every claim so you
can see that happening. An unknown value is refused where it is written.

Nothing else is a policy: a late task is governed by its [`skipAfter`](#skipafter), and a failing
task or item by its [`maxAttempts`](#retries).

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
| `TaskTimeout` | raised inside `execute()` when a task outlives its limit; recorded as status `timeout` |
| `DistributionNotFound` | no such distribution in the registry |
| `DistributionValidationError` | bad kwargs, or it didn't return a `timedelta` |
| `VersionMismatch` | `startWorkers()` or `init()` found a live worker on a different fingerprint |
| `UnboundDocument` | `save()`/`delete()`/`reload()` on a document with no engine |

## Upgrading from 0.x

2.0 keeps no compatibility code, so an upgrade is two steps: change the code, then migrate the
data **once, with every 0.x worker stopped**. Skipping the data step fails quietly — a 0.x
document has no `leaseUntil`, and 2.0 never claims one without it.

### Code

| 0.x | 2.0 |
|---|---|
| `class Queue(BaseQueue)` | `class App(BaseApp)` |
| `super().__init__(db, overdueSchedulersPolicy="skip")` | `schedulerMissed = "skip"` on the class, or `missed=` on one `schedulers(...)`; `"execute once"` is now `"once"`, `"execute reconstructed"` is `"replay"` |
| `overdueTaskPolicy` | gone — `skipAfter` on `@task(...)`, or `taskSkipAfter` for every task, covers work that went stale for any reason |
| `SchedulerEngine(self, schedulersCollection=..., policy=...)` in `__init__`, then `engine.init()` | `accountOps = schedulers(AccountScheduler, "accountOperations")` on the class |
| `taskPoolInterval`, `schedulerPoolInterval`, `engine.poolInterval` | `taskPollInterval`, `schedulerPollInterval`, `engine.pollInterval` |
| constructing the queue ran `init()` | only `startWorkers()` runs it — constructing is safe from any process |
| `engine.startWorkers(n)` on each engine | `app.startWorkers(taskWorkers=n, schedulerWorkers=n)`, or `app.run(...)` |
| context merged into `work.kwargs` by hand | `emitWork()` on a `Scheduler` subclass — see [Schedulers](#schedulers) |
| `pymonque.mongo.MongoModel` | gone. `model_dump()` is pydantic's, and `None` is stored as null |

Two defaults to check: a `SchedulerEngine` built by hand in 0.x defaulted to `"execute reconstructed"`
— `"replay"` — while a declared one takes the app's `schedulerMissed` (`"once"`). And tasks are still
not retried unless a task declares `maxAttempts`, or the app sets `taskMaxAttempts`.

### Data

Run once against the database, before starting any 2.0 worker. List every scheduler collection,
including those of engines you built yourself:

```python
from pymongo import MongoClient

db = MongoClient("mongodb://...")["your_db"]
schedulerCollections = ["pymonque_schedulers"]      # + your own, e.g. "accountOperations"
collections = ["pymonque_tasks", *schedulerCollections]

# 1. uids must be unique: 2.0 builds unique indexes, and construction fails on duplicates
for name in collections:
    dupes = list(db[name].aggregate([
        {"$group": {"_id": "$uid", "n": {"$sum": 1}}},
        {"$match": {"n": {"$gt": 1}}},
    ]))
    assert not dupes, f"{name} has duplicate uids, resolve these first: {dupes[:5]}"

# 2. tasks 0.x left processing: it cancelled these at its next start, so do the same
#    (otherwise 2.0 would claim and run them again)
db["pymonque_tasks"].update_many({"status": "processing"}, {"$set": {"status": "canceled"}})

# 3. give everything a lease, claimable at its deadline
for name in collections:
    db[name].update_many({"leaseUntil": None}, [{"$set": {"leaseUntil": "$deadline"}}])

# 4. schedulers no longer have a processing status — being worked is a lease
for name in schedulerCollections:
    db[name].update_many({"status": "processing"}, {"$set": {"status": "enabled"}})
```

It is idempotent, so running it twice does no harm. Tasks 0.x marked `outdated` or `canceled` stay
as they are. The update in step 3 is a pipeline, which needs MongoDB 4.2 or later.

## Known limitations

Deliberate trade-offs and edges that are not handled, so none of them comes as a surprise:

- **Two versions starting at the same instant can both pass the version check.** It reads the live
  workers, then registers. Deploy one version at a time rather than relying on the check to settle a
  race.
- **The fingerprint sees signatures, not function bodies.** See
  [One version at a time](#one-version-at-a-time).
- **A timeout frees the worker, not the call.** Python can't interrupt a running call, so a timed-out
  task keeps running on its own daemon thread until it returns or the process exits.
- **A process cut off from the database for longer than its lease can have its work run twice at
  once.** Only the current claim's outcome is recorded, and the other is logged, but the side effects
  of both runs happen. Keep tasks idempotent where that matters.
- **`stopWorkers()` that times out still deregisters the process,** while the abandoned threads may
  still be running. Exit the process afterwards; a new version could otherwise start alongside them.
- **`requestStop()` alone leaves the heartbeat running.** Call `stopWorkers()`, or use `run()`, to
  finish shutting down.
- **`skipAfter` counts from the original deadline,** so the time a task spends waiting for retries
  counts against it.
- **`save()` on a task waiting for a retry keeps the retry time,** even if you change its deadline.
  Set `leaseUntil` yourself to move it. A task waiting for its first run, or a scheduler, moves its
  lease with its deadline.
- **Limits are per task, not per call, and per pile, not per item.** They describe the work, so
  they are declared with it. To run one function under two sets of limits, declare two tasks.
- **A scheduler whose task is gone warns on every beat** until you remove it, disable it, or put
  the task back.
- **`work()` treats `SystemExit` as a failure, like a task does, but not `KeyboardInterrupt`.** A
  Ctrl-C in the main thread leaves a claimed item for its lease to lapse.

## Notes

- Times are naive UTC (`utc_now()`). Mongo keeps millisecond precision.
- `model_dump()` is pydantic's own. Documents serialize with field aliases by default
  (`serialize_by_alias`), so a model can match an existing schema; pass `by_alias=False`
  or `mode="json"` when you want something else. `None` is stored as null, so a field
  can be cleared.
- Workers are daemon threads. `stopWorkers()` lets work in flight finish; a process killed
  outright leaves its leases to lapse, and the next worker picks them up ([Leases](#leases)).
