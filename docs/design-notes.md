# Design notes

Decisions that are made but not built, work that is on hold, and asymmetries that are deliberate.
Finding ids (B1, P3, N2…) refer to the engine anatomy review of `c3e75e2`.

## Decided, not built yet

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
- **Pile items have leases and retries** because the task holding an item can die before it
  releases it.
- **Tasks record `executionTime`** because the library was built in part to fit a system that
  needed it.
