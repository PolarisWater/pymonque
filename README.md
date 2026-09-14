# pymonque

A typed MongoDB layer for an application, with a task queue on top. You declare pydantic
models against collections and get storage, scheduling and workers over the same documents —
no reloading, no hand-written to-JSON, no broker, no daemon. The collections *are* the queue,
so any number of processes can share one.

Four things live in an app, all built on the same typed-collection engine:

- **collections** — pydantic models stored as objects, with CRUD
- **tasks** — a function call to run at a deadline
- **schedulers** — a recurring schedule that emits a task on a rhythm
- **piles** — a backlog of items that workers claim one at a time

## Install

```bash
uv add https://github.com/PolarisWater/pymonque.git
```

Python ≥ 3.13, pydantic ≥ 2.13, pymongo ≥ 4.17.

## Example

```python
# app.py
from pymonque import BaseApp, Document, task, pile, collection
from pydantic import BaseModel

class Email(BaseModel):
    to: str
    subject: str

class Group(Document):
    name:     str = ""
    colorHex: str = "#FFFFFF"

class App(BaseApp):
    groups = collection(Group)              # -> the "groups" collection
    outbox = pile(Email)                    # -> pymonque_pile_outbox

    @task
    @staticmethod
    def cleanup_logs(retention_days: int):
        ...

    @task
    def send_one(self):
        with self.outbox.work() as item:
            if item is None:
                return
            send(item.data.to, item.data.subject)
```

```python
# main.py
from pymongo import MongoClient
from app import App

app = App(MongoClient()["my_app"])

# run once, now
app.task.schedule(App.cleanup_logs(retention_days=30))

# run every day, declared once no matter how often this file runs
app.scheduler.ensure(
    "nightly-cleanup",
    App.cleanup_logs(retention_days=30),
    app.distribution("constant", dailyFrequency=1),
)

# store objects
group = app.groups.create(name="beta")
group.colorHex = "#FF0000"
group.save()
app.groups.find({"name": "beta"})

# fill the pile from anywhere
app.outbox.add(to="user@example.com", subject="Welcome")

app.run(taskWorkers=4, schedulerWorkers=1)   # blocks; Ctrl-C drains and exits
```

## How it works

**Collections.** `collection(Model)` gives a model its own collection and a typed CRUD engine:
`create`, `insert`, `save`, `update`, `get`, `find`, `count`, `delete`. Documents handed back
remember where they came from, so `obj.save()` and `obj.delete()` work directly.

This is the base everything else is built on — tasks, schedulers and piles are all collection
engines with extra behaviour, so they share the same querying and the same `model=` override.

**Tasks.** `@task` marks a method as runnable. It's a descriptor, so it behaves differently
depending on where you reach it from:

```python
App.cleanup_logs(retention_days=30)     # -> CallSpec, for scheduling
app.cleanup_logs(retention_days=30)     # -> runs it now
```

A `CallSpec` is just `{functionName, kwargs}`. Kwargs are checked against the function
signature when you schedule the task, not when a worker picks it up.

**Schedulers.** A scheduler stores a `CallSpec` and an interval. When its deadline passes it
emits a task and advances its own deadline by a fresh interval. Intervals come from
distributions — `constant`, `normal`, `lognormal`, `exponential` — all expressed as
`dailyFrequency`, how many times per day on average.

Use `add()` for a one-off schedule and `ensure(name, ...)` for the ones an app should always
have: `ensure` is keyed by name, so calling it on every boot creates it once.

An app can have several scheduler engines, each with its own collection and its own
`Scheduler` subclass, all feeding the one task engine. A subclass carries context and stamps
it onto every task it emits:

```python
class AccountScheduler(Scheduler):
    accountId: int

    def emitWork(self) -> CallSpec:
        return self.work.bind(accountId=self.accountId)

class App(BaseApp):
    accountOps = schedulers(AccountScheduler, "account_operations")

app.accountOps.add(App.sync(), dist, accountId=42)   # emits sync(accountId=42)
```

**Piles.** A pile is a backlog in its own collection. Items carry no function and no deadline;
something claims them. `claim()` is a single `find_one_and_update`, so two workers never get
the same item. `work()` wraps that: claim, mark done, or mark failed if the block raises — and
items retry by the same rule as tasks (`itemMaxAttempts`, or per pile).

**Workers.** `startWorkers()` starts daemon threads that poll, claim atomically, run, and write
results back. A worker sleeps only when it finds nothing to do, so a backlog drains at full
speed. A task runs once unless you opt in to retries (`taskMaxAttempts`, `retryDelay`); a crash
counts as an attempt, so a task that kills its worker can't take every worker down in turn.
`app.task.wait(task)` blocks until one finishes, from any process.

**Leases.** A claim is held for `leaseSeconds` and renewed while the work runs. If a worker dies,
its lease lapses and the next worker picks the work up — no restart, no sweep: a scheduler's beat
is emitted once, and a task or pile item counts the dead run as an attempt. Only the current claim can write an
outcome, so a worker that lost its claim cannot overwrite the one that took over. Because a live
lease is visible in the document, constructing an app never disturbs work in flight, so an API
process, a script and a worker container can all share one database safely.

**Shutting down.** `run()` installs SIGINT/SIGTERM handlers and blocks. The first signal stops
claiming new work and lets what's in flight finish; a second exits immediately. Use
`startWorkers()` / `stopWorkers()` instead if something else owns the process lifecycle.

**One version at a time.** A task document names a function and nothing more, so two deployments
that disagree about what that function does cannot safely share a database. `startWorkers()` hashes
the app's task and distribution signatures, its policies and its task limits, and refuses to start
if a live worker reports a different hash. Stop the old workers before starting the new ones.

## Docs

[docs/reference.md](docs/reference.md) — every object, method, field, status, and policy.

Coming from 0.x? 2.0 changes the code and the stored documents — read
[Upgrading from 0.x](docs/reference.md#upgrading-from-0x) before starting a 2.0 worker.

## Tests

```bash
uv run pytest
```
