# Handoff

Start here. Read this, then `docs/rebuild.md` (the design) and `docs/rebuild-checklist.md` (what
must keep working). `docs/design-notes.md` holds the reasoning and history behind decisions.

## The project

`pymonque` — a typed MongoDB layer for an application, with work on top. An app declares its
documents and its work in one `BaseApp` subclass; any number of processes construct the same app
against one database and share it safely. No broker, no coordinator: the documents are the queue,
claimed atomically through one `leaseUntil` field.

Kinds: **collections** (typed documents), **task engines** (calls due at a time, run by worker
threads), **scheduler engines** (recurring schedules that emit tasks), **piles** (items claimed by
tasks), **distributions** (interval functions for schedulers).

- Python ≥ 3.13, pydantic 2, pymongo 4. Tests use mongomock.
- Package version 2.0.0; all code today lives in `src/pymonque/core.py` (~2,750 lines).
- Run tests with `uv run pytest` — there is no `python` on PATH; use `uv run python`.

## Where things stand

- **The library is being rebuilt, not refactored.** Too many fixes were fitted onto a shape that grew
  one kind at a time. The rebuild is designed in full; no rebuild code exists yet.
- **Branches:** `main` holds the current library and all docs. `rebuild` was branched from it and
  holds the checklist and this file. Neither is pushed — `main` is ahead of `github/main` by the doc
  commits since `fe85e6b`.
- The old package passes all 482 tests and stays untouched until the new one replaces it.

## Working rules

- **Do not edit `/home/janpa/projects/MakerNetV2` or `/home/janpa/projects/Gauth`.** They use
  pymonque for demonstration only; read them, never change them.
- **Commit locally; push only when asked.**
- **Decided or held design work goes into `docs/design-notes.md`** (and `docs/rebuild.md` when it
  changes the rebuild), not only into conversation.
- Don't start building a layer that depends on an undecided point — ask first.
- Code style of the existing library: camelCase for methods, fields and settings (`leaseUntil`,
  `maxAttempts`, `startWorkers`); comments explain *why*; test names are sentences
  (`test_a_stale_holder_cannot_finish_the_item`), one behaviour each, with a module docstring saying
  what the file covers.

## Decisions made most recently

All ten open decisions in `rebuild.md` §5 are settled. The ones that change the most:

**Failure and retries**
- **Tasks have no retries.** A task either works or fails; one that must succeed retries inside its
  own code. `maxAttempts` / `retryDelay` go from `@task` and the app. Task limits are `timeout` and
  `skipAfter` only.
- A task whose worker died ends `failed`, never rerun, with an error saying so.
- **Pile items keep tries**, because the task holding one can die. A claim uses a try; a lapsed lease
  leaves it spent; out of tries, the item is given up. Max tries is set on the app and per pile.
- `item.fail()` is final. `release()` hands an item back unfinished and **gives the try back**.
  Items have no retry delay.
- Items can be cancelled like tasks: `cancel(uid)`, `cancelMany(where)`, only when not being worked.

**The `work()` block** — the main pile example in the final docs
```python
with app.outbox.work() as w:
    if w is None:
        return
    if rateLimited():
        w.release()        # records, then ends only this block
    send(w.data)
# end of block → done; an exception → failed and re-raised
```
- `w.done(result)`, `w.fail(error)`, `w.release()` **write the outcome first**, then raise a private
  `BaseException` that `work()` catches and suppresses, so only the block ends.
- Nested blocks pass on another claim's early end.
- If user code swallows the early end (bare `except:`, `except BaseException:`, `return`/`break` in a
  `finally`), the outcome stands, nothing more is written, and a warning is logged. The rest of the
  block still runs — accepted: not bullet-proof, and not worth giving up the `with` block. Document
  the caveat.

**Timeouts**
- A timeout frees the worker and writes the task off, as today, plus: an exception is injected into
  the thread (stops pure-Python loops, not blocking C/I/O calls); a pile item it holds stops being
  renewed; and an opt-in makes a process with N abandoned threads stop claiming, drain and exit for
  its supervisor to restart.
- Accepted **only because it logs clearly**: every step names the task and prints the thread's stack
  (`sys._current_frames()`); the abandoned-thread count goes on the worker heartbeat.

**Shape**
- **Several task engines** per app. Tasks belong to no engine; any engine runs any task.
- A scheduler engine picks its task engine **by reference**: `nightly = schedulers(emitsInto=heavy)`,
  declared above it; left out means the default engine.
- Worker counts: an int means that many threads on every engine of the kind (default task engine
  included); a dict sets engines by name.
- **One claim check:** a `claimId` on every claimed document — tasks, items, held schedulers (B6).
- **Lease length** is declared per engine and fingerprinted (P1), with one app default per kind:
  `taskLeaseSeconds`, `schedulerLeaseSeconds`, `pileLeaseSeconds` (300 each), overridden by
  `leaseSeconds=`. **Distributions** are a class attribute (P3). The **fingerprint** also covers
  each model's schema, collection name and key (P4).
- One declaration shape, `kind(Model, collection=, extraIndexes=, …)`.
- **Names:** engine declarations are plural, `tasks(...)` / `schedulers(...)`, because `task` is the
  `@task` decorator; `pile(...)` and `collection(...)` stay. Everything else is singular: defaults
  (`taskTimeout`, `pileMaxAttempts`, `schedulerMissed`), default engines `app.task` / `app.scheduler`.
  `task = tasks(...)` replaces the default and hides `@task` below it in the class body; using a
  declaration as a decorator raises a clear error.
- **Collections:** `pymonque_task_<name>`, `pymonque_scheduler_<name>`, `pymonque_pile_<name>`; the
  default engines use `pymonque_task` and `pymonque_scheduler` (a declaration of that name takes
  over). Upgrading from 2.0 renames `pymonque_tasks` / `pymonque_schedulers`.
- Statuses: `pending` / `running` / `done` / `failed` / `canceled`, plus task-only `timeout`,
  `outdated`, `incompatible`.
- Verbs: tasks `schedule`, items `add`, schedulers `add` / `ensure`; `cancel` on tasks and items.
- **Custom tasks:** a task engine declaration takes a `Task` subclass with extra fields, given to
  `schedule(…)`. **Context is stamped only on the task**, by `Task.runWork()`;
  `Scheduler.emitWork()` is removed, and `Scheduler.taskFields()` passes a scheduler's fields to the
  tasks it emits. A scheduler with context must emit into an engine whose `Task` has those fields.
  Fields are data only.

**Still on hold** (not part of the first build): waiting on items, finishing a task by hand,
recording how long an item was held, a child process per task.

## How to build

Follow `rebuild.md` §6:

1. Build bottom-up in `src/pymonque_next/`, beside the old package, one layer per session:
   1. settings, declarations, documents, calls — plus the `Task`, `Scheduler` and `Item` models
      (fields only; declarations check against them) and distributions; checks that need
      `BaseApp` (reserved names, app defaults at class definition) are wired in layer 4;
   2. claims and task engines;
   3. scheduler engines and piles (including the `work()` block);
   4. app, versions, workers, shutdown, timeouts;
   5. README and reference from the new shape, with an upgrade section; then replace the old package.
2. Write tests against the new declarations, by behaviour, in a new test directory; tick items in
   `rebuild-checklist.md` as they get a test. Port nothing marked **Dropped**.
3. Commit each layer when its tests pass. Record anything decided along the way in
   `design-notes.md` and `rebuild.md`.

## Doc map

| File | What it is |
|---|---|
| `docs/handoff.md` | this: context, rules, latest decisions |
| `docs/rebuild.md` | the rebuild design: invariants, shape, decisions, layout, build plan |
| `docs/rebuild-checklist.md` | every behaviour to keep, change, drop or add, with its old test file |
| `docs/design-notes.md` | decisions with their reasoning, held work, deliberate asymmetries |
| `docs/reference.md`, `README.md` | docs for the current (old) library |
