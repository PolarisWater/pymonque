# Design notes

Decisions that are made but not built, work that is on hold, and asymmetries that are deliberate.
Finding ids (B1, P3, N2…) refer to the engine anatomy review of `c3e75e2`.

The library is to be rebuilt rather than refactored further; `docs/rebuild.md` gathers everything
here, plus the invariants the rebuild must keep and the decisions to make before building.

## Decided, not built yet

### No task retries; piles hold work that has to happen

- The model was clean from the start: schedulers "never" fail, and tasks either work or fail. A task
  that must succeed retries inside its own code; one that may fail needs no retry.
- Task `maxAttempts` and `retryDelay`, and the app's `taskMaxAttempts` / `taskRetryDelay`, go.
  A task whose worker dies is written off as failed, never rerun.
- Pile items keep tries, because the task holding one can die before freeing it. An item's lease
  lapsing (no response) or its holder releasing it (free) counts as a try. `fail()` is final: the
  item is marked failed and never claimed again. No retry delay.
- Max tries is set at two levels: an app default and each pile's declaration.

### Several task engines (B5, which also settles B1)

- An app may declare more than one task engine, each with its own collection.
- Tasks belong to no engine: every task engine can run any of the app's tasks. A scheduler engine
  emits into the task engine selected for it; everywhere else, users schedule on whichever task
  engine they want.
- Worker counts: `taskWorkers` and `schedulerWorkers` take an int, meaning that many threads on
  **every** engine of that kind, or a dict of engine name to count. Engines a dict leaves out get
  none.
- Still open when this is picked up:
  - how a scheduler engine names its task engine (attribute name, or a reference);
  - the collection name of a declared task engine (`pymonque_tasks_<name>`, matching schedulers);
  - how `init()`, the backlog warning and the fingerprint span several task engines;
  - whether the declaration can shape the engine's model, collection and indexes (the rest of B5).

## Proposed, not decided

### Custom tasks, shaped like custom schedulers

Builds on several task engines, and answers its open question about whether a declaration can
shape the engine's model, collection and indexes: yes, the way `schedulers()` does.

- **A task engine declaration takes a `Task` subclass,** as `schedulers()` takes a `Scheduler`
  subclass: `accountTasks = tasks(AccountTask, "account_tasks", extraIndexes=[...])`. The subclass
  adds fields stored on every task in that engine — context to query and index by, such as an
  account, a tenant or a priority.
- **Fields are given where a task is created,** and validated by pydantic:
  `app.accountTasks.schedule(App.sync(), accountId=42)`, like
  `app.accountOps.add(App.sync(), daily, accountId=42)`.
- **`Task.runWork() -> CallSpec` mirrors `Scheduler.emitWork()`:** the call that actually runs, by
  default `self.work`. A subclass can stamp its fields onto the call —
  `return self.work.bind(accountId=self.accountId)`. `schedule()` validates `runWork()` against the
  signature, as `validateScheduler()` validates `emitWork()`.
- **A scheduler builds the model of the task engine it emits into.** `Scheduler.taskFields() -> dict`
  (default `{}`) supplies the extra fields, so an `AccountScheduler` hands its `accountId` to an
  `AccountTask`. A required field it doesn't supply fails when the scheduler is added, not when it
  emits.
- **Tasks still belong to no engine,** so a subclass's fields and `runWork()` apply to every task
  stored in that engine, whichever function it names.
- Open:
  - whether context should be stamped in one place only — on the task, with schedulers just passing
    fields through `taskFields()` — rather than by both `emitWork()` and `runWork()`;
  - whether extra fields may steer claiming, such as a priority sort, or stay data only.

## On hold

- **Verbs that differ between tasks and items (B4, Matrix 3):** cancelling or waiting on an item,
  finishing a task by hand. Revisit after several task engines.
- **A hold duration for pile items,** like a task's `executionTime`. To be designed as something
  modular after several task engines, rather than forced onto items now.
- **Not yet discussed:** B6 (three claim-identity mechanisms), P1–P4 (setting placement and
  fingerprint coverage), N1–N7 (naming).

## Deliberate, not asymmetries

- **Schedulers have no retries, timeouts or staleness rule beyond `missed`.** Emitting is a database
  read and write, assumed not to fail; tasks can fail all day.
- **Pile items have leases and tries, tasks have neither retries nor tries** because the task
  holding an item can die before it releases it, while a task either works or fails.
- **Tasks record `executionTime`** because the library was built in part to fit a system that
  needed it.
