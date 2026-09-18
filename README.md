# pymonque

A typed MongoDB layer for an application, with work on top. You declare pydantic models against
collections and get storage, scheduled calls, recurring schedules and work piles over the same
documents — no broker, no coordinator. The documents *are* the queue, so any number of processes —
an API, a script, worker containers — can construct the same app against one database and share it.

An app declares its parts in one class:

- **collections** — pydantic models stored as documents, with typed CRUD
- **task engines** — calls due at a time, run by worker threads
- **scheduler engines** — recurring schedules, each emitting a task on its rhythm
- **piles** — items waiting to be claimed, drained by tasks

## Install

```bash
uv add https://github.com/PolarisWater/pymonque.git
```

Python ≥ 3.13, pydantic ≥ 2.13, pymongo ≥ 4.17.

## Example

```python
# app.py
from pydantic import BaseModel
from pymonque import BaseApp, Document, collection, pile, task

class Email(BaseModel):
    to:      str
    subject: str

class Group(Document):
    name:     str = ""
    colorHex: str = "#FFFFFF"

class App(BaseApp):
    groups = collection(Group)          # -> the "groups" collection
    outbox = pile(Email, maxAttempts=3) # -> pymonque_pile_outbox

    @task(timeout=600)
    @staticmethod
    def cleanupLogs(retentionDays: int): ...

    @task
    def sendOne(self):
        with self.outbox.work() as w:
            if w is None:
                return                  # the pile is empty
            if rateLimited():
                w.release()             # back on the pile, try returned; the block ends here
            send(w.data.to, w.data.subject)
        # reaching the end of the block: done; an exception: failed, and re-raised
```

```python
# main.py
from pymongo import MongoClient
from app import App

app = App(MongoClient()["my_app"])      # safe from any process: writes nothing but indexes

app.task.schedule(App.cleanupLogs(retentionDays=30))       # run once, now

app.scheduler.ensure(                   # run every day; declared once however often this runs
    "nightly-cleanup",
    App.cleanupLogs(retentionDays=30),
    app.distribution("constant", dailyFrequency=1),
)

group = app.groups.create(name="beta")  # typed documents that save themselves
group.colorHex = "#FF0000"
group.save()

app.outbox.add(to="user@example.com", subject="Welcome")   # fill the pile from anywhere

app.run(taskWorkers=4, schedulerWorkers=1)   # a worker process: blocks; Ctrl-C drains and exits
```

## How it works

**Declarations.** Everything shared — parts, limits, defaults, the distribution registry — is
declared on the class, and checked where it is written: a bad setting fails as the class is defined,
naming itself. Every storage declaration has one shape, `kind(Model, collection, extraIndexes=, …)`.
Leaving a setting out takes the app's default (`taskTimeout`, `pileMaxAttempts`, …); `None` only
means "no limit". Per-process pacing — poll intervals, worker counts — goes to the constructor.

**Collections.** `collection(Model)` gives a model its own collection and a typed engine: `create`,
`insert`, `save`, `update`, `get`, `find`, `count`, `delete`. A document handed back remembers where
it came from, so `doc.save()` and `doc.delete()` just work. Task engines, scheduler engines and piles
are collection engines too, so they query the same way.

**Tasks.** `@task` marks a method as runnable. Reached through the class it builds a call to store;
through an app, it is the function:

```python
App.cleanupLogs(retentionDays=30)       # -> CallSpec, to schedule
app.cleanupLogs(retentionDays=30)       # -> runs it now
```

A call is checked against the signature when it is scheduled, so a bad one is never stored, and again
when it runs, so a model argument arrives as the model. **A task runs at most once**: a failure is
final, and a task whose worker died is written off as `failed`, never run again — it may have done
part of its work. A task that must succeed retries inside its own code.

An app has a default task engine, `app.task`, and may declare more — `heavy = tasks(leaseSeconds=900)`
— each with its own collection and workers. Any engine runs any task. A `Task` subclass on a
declaration adds fields of your own to every task in it, given to `schedule(…)`, and its `runWork()`
is the one place they reach the call.

**Schedulers.** A scheduler stores a call and a distribution. When its deadline passes it emits the
call as a task and moves its deadline on by a fresh interval. Each beat's task has a fixed uid, so a
beat is never emitted twice. `ensure(name, …)` declares one by name, idempotently; `missed` says what
is owed for beats that went by unworked: `skip`, `once` or `replay`. A scheduler engine emits into
the default task engine, or the one named by `schedulers(emitsInto=heavy)`.

**Piles.** A pile holds items that must be worked even if a process dies holding one: a claim uses a
try, a lapsed lease leaves it spent, and an item out of tries is given up. `with pile.work() as w:`
claims one item and holds it for as long as the block runs. `w.done()`, `w.fail()` and `w.release()`
record their outcome and end the block there; `release()` gives the try back.

**Leases.** Everything claimable has a `leaseUntil`: when it is due while it waits, the end of its
holder's lease while it is held. A claim is one atomic `find_one_and_update` on it, and gives the
document a new `claimId`; only the claim still holding it can write an outcome. Leases are renewed
while the work runs — through a graceful shutdown too — and a dead process's leases simply lapse:
no restart, no sweep.

**Timeouts.** `@task(timeout=60)` frees the worker and writes the task off as `timeout`, logging where
the call was. The call is then asked to stop: that stops pure Python, not a call blocked in C, I/O or
a sleep, which is logged and counted as abandoned. `App(db, retireAfter=3)` makes a process with three
abandoned threads stop claiming, drain, and leave `run()` for its supervisor to restart it.

**One version at a time.** A task names a function and nothing more, so two deployments that
disagree about that function cannot share a database. Starting workers checks a fingerprint of the
app — task signatures, limits, models, collections, settings — against every live worker process,
and refuses on a mismatch. Deploy all-or-nothing: stop the old workers, then start the new ones.

## Docs

[docs/reference.md](docs/reference.md) — every object, method, field, status and setting.

Coming from 2.0? 3.0 changes the declarations, the statuses and the stored documents: read
[Upgrading from 2.0](docs/reference.md#upgrading-from-20) before starting a 3.0 worker.

## Tests

```bash
uv run pytest
```
