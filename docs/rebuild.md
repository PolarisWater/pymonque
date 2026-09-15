# Rebuild

The library grew one kind at a time — tasks, then schedulers, then piles, then plain collections —
and each fix since has been fitted to the shape that was already there. This is the whole picture,
written before any code: what the rebuild keeps, how the pieces fit, what is decided, and what has
to be decided first.

Status of each part: **Keep** (proven, carry over as-is), **Decided**, **Proposed** (a starting
point, not agreed), **Open** (needs a decision before building). Finding ids (B1, P3, N2…) refer
to the engine anatomy review; `docs/design-notes.md` has the history behind the decisions.

## 1. What it is for

A typed MongoDB layer for an application, with work on top. An app declares its documents and its
work in one class. Any number of processes — an API, a script, worker containers — construct the
same app against one database and share it safely, with no broker and no coordinator: the
documents are the queue.

## 2. Invariants — Keep

Each of these was paid for with a bug. The rebuild keeps all of them and tests each one directly.

**Claiming**
- One field, `leaseUntil`, says when anything is claimable: its due time while waiting, the end of
  the holder's lease while held. A claim is one atomic `find_one_and_update` on it.
- A lease is renewed in the background until the work finishes — including through a graceful
  shutdown, until the worker threads have actually stopped.
- A lapsed lease makes the work claimable by anyone. No restart, sweep or coordinator.
- **Only the current claim writes an outcome.** A worker whose lease lapsed cannot overwrite a
  cancel, or the worker that took over. (Today this is three mechanisms — B6; see §5.)
- A renewal never overwrites an outcome.

**Failure — Decided: no task retries**
- Schedulers are assumed never to fail; tasks may fail, and nothing retries them. A task that must
  succeed retries inside its own code; one that may fail just fails. Rerunning a side effect nobody
  asked to rerun is worse than a failure you can see.
- A task runs at most once. If its worker dies (the lease lapses), the task is written off as failed,
  not handed to the next worker — it may have done half its work.
- A timeout frees the worker and writes the task off, since the call may still be running.
- Work that has to happen survives a dead process through a pile: the item holds it, a task drains
  it. A claim uses a try; if the lease lapses the item goes back to be claimed with that try spent,
  and running out of tries gives it up. `release()` hands an item back unfinished and returns the
  try, since the holder says the work did not happen. `fail()` is final — the item is marked failed
  and never claimed again. Items have no retry delay.
- Inside `with pile.work() as w:`, `w.release()` ends just that block and releases the item —
  **Decided**, details in `docs/design-notes.md`.
- Tasks and items that have not started can be cancelled (`cancel`, `cancelMany`); running work
  cannot be interrupted, only waited out.
- `sys.exit()` in work is a failure, not a dead worker thread.
- A result the driver cannot encode is checked before writing, and recorded as a failure.

**Schedulers**
- A beat's task has a fixed uid, so a worker dying between emitting and moving the deadline on
  cannot emit it twice.
- A deadline write only lands if nobody moved the deadline meanwhile.
- A deadline moved by hand moves the lease with it.
- A scheduler whose task is gone skips the beat and stays enabled; disabling is the operator's word.

**Processes and versions**
- Constructing an app never writes to shared state beyond creating indexes. Housekeeping belongs to
  a process that starts workers.
- Only one version of the code runs workers at a time: a fingerprint of the shared behaviour, a
  worker registry with heartbeats, refusal on mismatch. The heartbeat runs until work in flight has
  drained.
- Housekeeping that decides from this process's task list is version-checked too.

**Declarations**
- Shared behaviour is declared in code on the app class; per-process pacing is passed to the
  constructor. (Where exactly the line runs is P1/P3 — §5.)
- Every declaration is checked where it is written, and the error names the setting.
- Leaving a setting out is the only way to take a default. `None` only means "no limit".
- Limits describe the work, not a call: declared once, never stored on documents.
- A declaration cannot replace part of the app, and a subclass's redefinition replaces the parent's.

**Calls**
- A call's arguments are validated against the signature when it is queued, and validated again
  when it runs, so the function receives what its annotations promise.
- A task is called with keyword arguments only; a signature that can't be is refused at declaration.

**Documents**
- Writes are validated against the model before anything is written; `update` writes only what
  changed.
- A bound document remembers the key it was stored under, so changing its key renames it.
- Times are naive UTC.

## 3. The shape

### Kinds

| Kind | Holds | Runs | Declared with |
|---|---|---|---|
| Collection | your documents | nothing | `collection(Model, …)` |
| Task engine | calls due at a time | worker threads | `tasks(Model, …)` — **Decided**, several per app |
| Scheduler engine | recurring schedules | worker threads | `schedulers(Model, …)` |
| Pile | items waiting to be claimed | nothing; drained by tasks | `pile(Payload, …)` |
| Distributions | interval functions | — | a registry class |

Every storage kind is a typed collection engine underneath, so `get`, `find`, `count`, `update`,
`delete` and bound documents work the same everywhere.

### One declaration shape — Proposed

Every storage declaration takes the same leading arguments, spelled the same way, and adds only
what its kind needs:

```python
kind(Model,                         # positional: the document or payload model
     collection = <left out>,       # a Collection or a name; left out: a default name
     extraIndexes = None,
     **kind-specific settings)
```

This retires N1 (`itemsCollection`, `schedulersCollection`, `tasksCollection`, `payload`,
`schedulerModel`, `taskModel`).

```python
class App(BaseApp):
    groups  = collection(Group, key="groupId")
    heavy   = tasks(RenderTask, leaseSeconds=900)       # beside the default engine, `task`
    nightly = schedulers(emitsInto="heavy", missed="skip")
    outbox  = pile(Email, maxAttempts=3)

    @task(timeout=60, maxAttempts=3)
    def sync(self, accountId: int): ...
```

### Several task engines — Decided

- Tasks belong to no engine: any task engine can run any of the app's tasks.
- A scheduler engine emits into the task engine selected on its declaration. Everywhere else, code
  schedules on whichever task engine it wants.
- Worker counts: `taskWorkers` and `schedulerWorkers` take an int — that many threads on every
  engine of the kind — or a dict of engine name to count; engines a dict leaves out get none.

### Custom tasks — Proposed

A task engine declaration takes a `Task` subclass, the way `schedulers()` takes a `Scheduler`
subclass: extra fields stored on every task in that engine, given where the task is created and
validated by pydantic. `Task.runWork()` returns the call that runs; `Scheduler.taskFields()` hands
a scheduler's context to the tasks it emits. Details in `docs/design-notes.md`.

### Rules, per kind

| | Declared on | App default | Stored on documents |
|---|---|---|---|
| Task limits: `timeout`, `skipAfter` | `@task(...)` | `task*` | no |
| Item tries: `maxAttempts` — claims whose outcome never came back | `pile(...)` | `item*` (N3) | no |
| Missed beats: `skip`, `once`, `replay` | `schedulers(...)` | `schedulerMissed` | no |

Deliberate differences, not asymmetries: schedulers have no retries, timeouts or staleness rule,
because emitting is a database write assumed not to fail; tasks have no retries, because a task
either works or fails; items have tries because the task holding one can die before releasing it;
only tasks record `executionTime`.

### Documents — Keep, with one Open question

| | Task | Scheduler | Item |
|---|---|---|---|
| When | `deadline` | `deadline` | `createdAt` |
| Moments | `createdAt`, `claimedAt`, `finishedAt` | — | `createdAt`, `claimedAt`, `finishedAt` |
| Outcome | `result`, `error` | — | `result`, `error`, `attempts` |
| Duration | `executionTime` | — | on hold — a modular hook, not forced |

## 4. Code layout — Proposed

`core.py` is about 2,700 lines. The rebuild splits it by responsibility, so each invariant has one
home:

```
pymonque/
  app.py            BaseApp: construction, registries, reserved names, run/stop, signals
  declarations.py   collection / tasks / schedulers / pile / @task, and their checks
  settings.py       the shared constraints, TaskLimits, ItemLimits, AppDefaults, the "left out" marker
  documents.py      Document, CollectionEngine, bound documents, validated update
  claims.py         leases, claim ids, renewal, the worker loop
  tasks.py          Task, TaskEngine: execute, retries, timeouts, skipAfter, cancel, wait
  schedulers.py     Scheduler, SchedulerEngine: beats, missed, ensure
  piles.py          Item, PileEngine: claim, work(), done/fail/release
  calls.py          CallSpec, signature validators, call-time validation
  distributions.py  BaseDistributions, DistributionEngine
  versions.py       fingerprint, worker registry, heartbeat, backlog check
  exceptions.py
```

## 5. Decide before building — Open

Ordered by how much of the shape depends on them. Each carries the recommended answer; none is
agreed yet.

1. **Claim identity (B6).** One `claimId` on every claimed document — task, item, and a scheduler
   while it is held — replacing `attempts` matching for tasks and `deadline` matching for
   schedulers. *Recommended:* yes; it makes §2's "only the current claim writes" one mechanism, and
   tasks no longer keep `attempts` to match on.
2. **How a scheduler engine names its task engine.** An attribute name (`emitsInto="heavy"`) is
   checkable when the app is built; a reference to the declaration (`emitsInto=heavy`) is checkable
   where it is written but only works for engines declared above it in the class. *Recommended:*
   the attribute name, checked in `__init_subclass__`.
3. **Where lease length lives (P1).** It decides when another process may take work over, so it is
   shared behaviour: declared and fingerprinted, or a per-process constructor argument only.
   *Recommended:* declared per engine and fingerprinted; the constructor keeps only pacing (poll
   intervals, worker counts).
4. **Distributions registry placement (P3).** A class attribute, `distributions = MyDistributions`,
   rather than a constructor argument. *Recommended:* yes.
5. **What the fingerprint covers (P4).** Add each declared model's schema, collection name and key,
   so a changed payload or scheduler field refuses a mismatched worker like a changed signature does.
   *Recommended:* yes.
6. **Names (N2, N3, N7).** One noun per kind across descriptor, defaults, registry and default
   collection. *Recommended:* singular descriptors `task` / `scheduler` / `pile` / `collection`;
   defaults `taskTimeout`, `pileMaxAttempts`, `schedulerMissed`; collections
   `pymonque_<kind>_<name>`.
7. **Status vocabulary (N4).** *Recommended:* `pending` / `running` / `done` / `failed` /
   `canceled` shared by tasks and items; task-only `timeout`, `outdated`, `incompatible`.
8. **Verbs (N5, N6, B4).** *Recommended:* tasks `schedule`, items `add`, schedulers `add` / `ensure`;
   `cancel` / `cancelMany` on both (decided); waiting on items and finishing tasks by hand stay held;
   no new verbs until something needs one.
9. **Custom tasks (§3).** *Recommended:* context is stamped only on the task, with schedulers passing
   it through `taskFields()`; extra fields are data only, and a priority order can come later.

## 6. How to build it — Proposed

1. **Settle §5,** and fold the answers into this document.
2. **Turn the current tests into a behaviour checklist,** grouped by §2's headings, so nothing the
   old code guarantees is lost silently.
3. **Build bottom-up beside the old package** — settings and declarations, documents, calls, claims,
   then tasks, schedulers and piles, then the app, versions and workers — each layer with its tests
   before the next.
4. **Port the tests by behaviour, not by file,** writing them against the new declarations rather
   than patching the old ones.
5. **Write the reference and README from the new shape,** and an upgrade section covering renamed
   declarations, statuses and document fields.
6. **Replace the old package** once the checklist is covered.
