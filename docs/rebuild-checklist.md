# Rebuild checklist

Every behaviour the old test suite guarantees, grouped by `docs/rebuild.md` §2, plus what the
decisions add. Tick an item when the new package has a test for it. Written from the tests at
`68c168d` (482 tests, all passing); the old test file each came from is in brackets.

Marks: **Changed** — kept, but reshaped by a decision; **Dropped** — removed by a decision, port
nothing; **New** — no old test, decided since.

## Claiming and leases

- [x] A due task is claimed and run; a future one is left alone; working nothing is a no-op [tasks]
- [x] Oldest deadline first; a task is never claimed twice, also across worker threads [tasks, app]
- [x] A fresh task carries a lease equal to its deadline; claiming pushes it out [leases]
- [x] A long task is not taken over while its lease is renewed; renewal stops when the work does [leases, tasks]
- [x] A lapsed lease is claimable by the next worker; a live one is left alone — tasks, schedulers, items [tasks, schedulers, piles, leases]
- [x] A new app instance does not disturb work in flight or live leases [leases, piles]
- [x] Leases keep renewing while a shutdown drains [leases]
- [x] Indexes back the claim query — tasks, schedulers, piles [tasks, schedulers, piles]
- [x] **Changed:** every claim gets its own `claimId` — tasks, items and held schedulers, one mechanism (B6) [claims]
- [x] A stale holder cannot finish, fail, release or renew; the current holder can [claims]
- [x] Renewal skips work another worker took; a renewal never overwrites an outcome [claims, tasks]
- [x] A lost task claim does not overwrite a cancel; a taken-over task keeps the new claim's outcome [claims]
- [x] `work()` logs when its outcome was not recorded [claims]
- [x] Moving a deadline by hand (save or update) moves a waiting task's or scheduler's lease [tasks, schedulers]
- [ ] **Dropped:** a waiting retry keeps its retry time on save; renewal skips a waiting retry [tasks]

## Failure

- [x] A raising task is marked failed with the traceback [tasks]
- [x] `sys.exit()` in a task or a `work()` block is a failure, not a dead worker thread [tasks, piles]
- [x] A result the driver cannot encode is a failure and does not leave the task claimed [tasks]
- [x] A task whose function vanished is flagged incompatible at `init()`; a finished one is not [tasks]
- [x] **Changed:** a task whose worker died is `failed`, never rerun, with an error saying so [retries]
- [ ] **Dropped:** task retries — retried until the last attempt, retry delay, success clears the old error, app default for retries, emitted tasks retry, crash below the limit recovered, outdated retry not run, `wait` treats a retry as unfinished [retries, wait, timestamps]
- [x] **New:** no `maxAttempts` / `retryDelay` on `@task` or the app; declaring them is refused

### Timeouts

- [x] No time limit by default; `@task(timeout=…)` declares one, looser or tighter than `taskTimeout` [timeouts]
- [x] An emitted task times out by its task's limit [timeouts]
- [x] A timed-out task records why and warns; a raise inside a timed task is still a failure [timeouts]
- [x] A timeout frees the worker for the next task, and is never rerun [timeouts, retries]
- [x] **New:** a pile item held by a timed-out call stops being renewed, so its lease lapses
- [x] **New (§5.10):** a timed-out thread gets an exception injected; the log says whether it stopped, with its stack
- [x] **New (§5.10):** opt-in retiring: past N abandoned threads a worker stops claiming, drains, and exits, logging which tasks and where they were stuck

### skipAfter

- [x] Without a limit a task runs however late; `None` makes a task unskippable [stale]
- [x] The app default applies to a bare task; a task declares its own in either direction [stale]
- [x] An outdated task is not run, says how late, and was claimed and finished [stale, timestamps]
- [x] An emitted task goes stale by its task's limit [stale]

## Piles

- [x] Each pile gets its own collection; piles are listed on the app; class access gives the declaration [piles]
- [x] The collection name can be overridden, or given as a collection object; a subclass can override a pile [piles, engines]
- [x] Two piles do not share items [piles]
- [x] `add` takes a model, a dict or kwargs; the payload is stored as plain data, defaults filled in, invalid refused [piles]
- [x] An untyped pile takes any dict; a payload is a plain model, not a document [piles, guards]
- [x] `addMany` inserts in one go, validates everything first, and nothing is a no-op [piles]
- [x] `claim` takes the oldest item, marks it, counts the try; empty or unmatched filter gives `None` [piles]
- [x] No two claims return the same item; a claimed item is not offered again [piles]
- [x] `done` records a result; `fail` records an error; both report whether they changed an item [piles]
- [x] An item can be finished by uid, as an operator's verdict, whatever the claim [piles, claims]
- [x] An item never claimed cannot be finished as a claim [claims]
- [x] `count`, `counts`, typed `find`, `purge` of one status only [piles]
- [x] Finished items survive a restart [piles]
- [x] A held item's lease can be renewed and reports whether there was one [piles]
- [x] A live holder is not taken over [piles]
- [x] An abandoned item with tries left is claimed again; out of tries it is given up at the next claim, without blocking the next item [piles]
- [x] A given-up item cannot be finished by its old holder [claims]
- [x] A pile can override the app's max tries; a bad value fails where it is written [piles, declarations]
- [x] A pile can declare extra indexes [piles]
- [x] `release` puts the item back in its own place, is claimed afresh, gives the try back, and needs a claim [piles, claims]
- [x] **Changed:** `fail()` is final, always — not only by uid or by default [piles]
- [ ] **Dropped:** a failure with tries left goes back on the pile; item retry delay; done after a retry clears the error [piles]
- [x] **New:** `cancel(uid)` / `cancelMany(where)` cancel an item not being worked, set `finishedAt`; a running one cannot be

### The `work()` block

- [x] Reaching the end marks the item done; an exception marks it failed and re-raises [piles]
- [x] Yields `None` on an empty pile; takes a filter [piles]
- [x] A task can drain the pile, and one on an empty pile still succeeds [piles]
- [x] The whole loop: a scheduler fires a task that claims a pile item [app]
- [x] **New:** `w.done(result)`, `w.fail(error)`, `w.release()` write first, then end only the block; the code after the `with` runs
- [x] **New:** a nested block passes on an outer block's early end
- [x] **New:** a swallowed early end keeps its outcome, writes nothing more, and logs that the block kept running

## Schedulers

- [x] `add` stores an enabled scheduler, first deadline one interval out; refuses unknown task or distribution [schedulers]
- [x] A due scheduler emits a task; a future one stays put; the earliest deadline fires first [schedulers]
- [x] Firing advances the deadline one interval and returns it to enabled; never claimed twice [schedulers]
- [x] A disabled scheduler walks its deadline without emitting or warning [schedulers]
- [x] An emitted task is runnable and points back at its scheduler [schedulers, models]
- [x] Missed beats: `replay` beat by beat, `once` one run then from now, `skip` nothing then from now [schedulers]
- [x] A missed beat warns; merely due does not, and emits whatever `missed` says [schedulers]
- [x] A scheduler faster than it can be served stops accumulating; `missed` applies after startup too [schedulers]
- [x] A scheduler whose task vanished skips its beats and stays enabled [schedulers]
- [x] A beat reclaimed after a crash is not emitted twice [schedulers]
- [x] A rhythm restarted mid-claim is not overwritten [schedulers]
- [x] A scheduler field can be cleared [schedulers]
- [x] `ensure`: creates, idempotent across restarts, uid from the name, names don't collide, keeps the deadline, updates changed work [schedulers]
- [x] Changing work keeps the rhythm; a new distribution restarts it [schedulers, engines]
- [x] `ensure` leaves disabled disabled, can force state, can create disabled, validates first [schedulers]
- [x] Declared and added schedulers coexist; unnamed ones don't collide; get / remove / declare again [schedulers]
- [x] Concurrent declarations create one scheduler [schedulers]
- [x] Is disabled, not cancelled [cancel]

### Scheduler engines and subclasses

- [x] Engines are collected, each with its own collection; class access gives the declaration [engines]
- [x] A declaration named `scheduler` replaces the default; a subclass can override an engine [engines, app]
- [x] **New:** a declaration named `task` replaces the default task engine; a declaration used as a decorator raises a clear error
- [x] Per-engine `missed` and poll interval; extra indexes created; `init()` and workers cover every engine [engines]
- [x] A scheduler emits its stored work unchanged [engines]
- [x] **Changed:** a scheduler's context reaches the emitted task as task fields via `taskFields()`, and the task's `runWork()` stamps it; the task runs; the stored work stays uncontextualised [engines]
- [ ] **Dropped:** a scheduler subclass stamping context via `emitWork()` [engines]
- [x] **Changed:** a scheduler is validated as it will run — its `taskFields()` against the target engine's `Task` model, and that task's `runWork()` against the signature; an engine that cannot supply the context refuses it [engines]
- [x] Bad kwarg, unknown task, unknown distribution, missing context field are refused [engines]
- [x] Each engine reads its own model; context survives a fire [engines]
- [x] `upsert` creates then replaces and validates; `find` and `count` filter; `get` misses cleanly [engines]
- [x] `update` changes work or context, keeps or restarts the rhythm, toggles enabled, validates the merge, writes nothing if invalid, `None` if missing [engines]
- [x] **Changed:** `ensure` takes context fields, stays idempotent, can change context, validates through `taskFields()` and the target task's `runWork()` [engines]
- [x] `delete` and `deleteMany` [engines]
- [x] **Changed:** schedulers share the default task engine unless `emitsInto=<declaration>` names another [engines]
- [x] **New:** `emitsInto` must reference a declaration above it on the same app class

## Task engines

- [x] `schedule` stores a pending task, validates first, defaults to now, stamps the default or a named factory [tasks]
- [x] `scheduleFromDistribution` pushes the deadline out and validates the distribution [tasks]
- [x] `work` returns the task it ran; a queue drains by looping on it [tasks]
- [x] An instance task runs with its app; a classmethod can be a task; a subclass's plain method unregisters it [tasks]
- [x] A task gets models and coerced values, not stored data [tasks]
- [x] `update` validates before writing [tasks]
- [x] A task can be given a model [collections]
- [x] Cancel: a waiting or abandoned task can be; running or finished cannot; unknown uid reports nothing; `cancelMany` filters and leaves running work alone; a cancelled task never runs [cancel]
- [x] `wait` returns the finished task, takes a uid, works from another process, counts a failure as finished, times out, errors on an unknown task [wait]
- [x] **New:** several task engines, each with its own collection; any engine runs any task
- [x] **New:** a `Task` subclass on a declaration adds fields given to `schedule(…)`; `Scheduler.taskFields()` supplies them
- [x] **New:** `Task.runWork()` is the only place context is stamped; `schedule()` validates it; a plain `Task` runs `work` unchanged

## Processes and versions

- [x] Engines are built, share the app's database and collections; default collection names [app]
- [x] Two apps on one database share state; separate databases stay separate [app]
- [x] An app with no tasks or piles starts; `startWorkers` defaults to none [app]
- [x] Task workers drain tasks and never run one twice; a scheduler worker keeps emitting [app]
- [x] A worker survives and logs a failing iteration [app]
- [x] A backlog drains without waiting per task; an idle worker waits; work reports whether it did anything [version]
- [x] Poll intervals and defaults reach the engines [app]
- [x] **Changed:** worker counts — an int is that many threads on every engine of the kind, a dict sets engines by name [app]
- [x] **Changed:** the distribution registry is a class attribute, and reaches the engines [app]

### Fingerprint and registry

- [x] Stable; changes with a signature, an added task, a distribution registry, a declared limit, an app default, an item max tries, `missed` [version, limits]
- [x] A body change alone does not change it; a default a task overrides does not matter; an address in a repr does not split it [version, limits]
- [x] Two apps of one class agree; a default is not a constructor argument [version]
- [x] **New:** changes with lease length, each declared model's schema, collection name and key (P1, P4)
- [x] A matching worker may join; a mismatched one is refused, naming both versions [version]
- [x] Constructing an app is never refused; a stale worker does not block a deploy; enforcement can be turned off [version]
- [x] A registered worker reports itself; the heartbeat keeps it live [version]
- [x] `init()` is refused beside a different live version, cannot flag its tasks, allowed alone or beside the same version, skipped when enforcement is off [version]

### Backlog

- [x] Empty queue no backlog; backlog is due and unclaimed, not held, not future [backlog]
- [x] No warning while young; warns when no one runs task workers or all are busy; silent once drained [backlog]
- [x] The count spans processes; warnings can be turned off [backlog]
- [x] **New:** backlog spans every task engine

### Shutdown and lifecycle

- [x] `stopWorkers` stops the loops, not delayed by the poll interval [shutdown]
- [x] Work in flight is finished; nothing new is claimed once stopping; a timeout reports what it could not wait for [shutdown]
- [x] `requestStop` does not block; workers can start again after a stop [shutdown, lifecycle]
- [x] A clean stop frees the version slot; a stopped process does not block the next version [shutdown, lifecycle]
- [x] Starting twice leaves one heartbeat; stopping takes the monitor threads with it [lifecycle]
- [x] SIGTERM starts a graceful shutdown; a second signal exits; opt-in; previous handlers restored [shutdown]
- [x] `run` blocks until a signal, then drains; an engine reports its own state [shutdown]

## Declarations

- [x] A bad setting fails where it is written and names itself; a bad app default fails at class definition [declarations, limits]
- [x] Settings coerce like pydantic fields [declarations]
- [x] Left-out settings take the app's defaults; given ones win; `None` only means no limit [declarations, limits]
- [x] An existing collection object is accepted [declarations]
- [x] A bare task takes the defaults; a declared limit wins, the rest default; both decorator forms work [limits]
- [x] A task runs under its function's limits; declared once; calls, stored tasks and schedulers carry no limits [limits]
- [x] A declaration or task cannot shadow the app; the reserved names cover every attribute `__init__` sets [app]
- [x] **Changed:** one declaration shape, `kind(Model, collection=, extraIndexes=, …)` (N1)
- [x] **Changed:** declarations `tasks` / `schedulers` / `pile` / `collection` / `@task`; defaults `task*`, `pile*`, `scheduler*` (N3)
- [x] **Changed:** collections `pymonque_task_<name>`, `pymonque_scheduler_<name>`, `pymonque_pile_<name>`; defaults `pymonque_task` and `pymonque_scheduler`, taken over by a declaration of that name (N2)
- [x] **New:** lease defaults `taskLeaseSeconds`, `schedulerLeaseSeconds`, `pileLeaseSeconds` (300), overridden by `leaseSeconds=` on a declaration
- [x] **Changed:** status vocabulary `pending` / `running` / `done` / `failed` / `canceled` (+ task-only `timeout`, `outdated`, `incompatible`) (N4)

## Calls

- [x] The string form and the class form build the same call spec [tasks]
- [x] Unknown task, missing, unknown or wrongly typed argument refused; an optional one may be omitted; an instance task does not expect `self` [tasks, specs]
- [x] `*args` and positional-only refused where written; `**kwargs` accepts extras but checks the named ones [guards]
- [x] Unannotated accepts anything; an `Annotated` constraint is enforced [guards, specs]
- [x] CallSpec: packs kwargs, dispatches against a mapping or an object, `KeyError` on unknown, roundtrips through Mongo [specs]
- [x] `bind` merges kwargs, overrides without touching the original [engines]
- [x] FuncSpec: class access gives one, calling it builds a CallSpec; instance access runs a staticmethod or binds an instance task [specs]
- [x] Tasks are discovered across the MRO; a subclass overrides; a plain method is not a task [specs]
- [x] Staticmethods collected through the MRO, the child's wins; `uuid4str` unique [specs]

## Distributions

- [x] `constant` is exact; every distribution gives a positive interval; mean matches the daily frequency; `normal` never negative [distributions, guards]
- [x] The registry walks the MRO; a custom registry extends the built-ins; the default does not see custom ones [distributions]
- [x] Calling builds and validates a CallSpec; unknown, missing, unknown or wrongly typed argument refused [distributions]
- [x] `gen` produces an interval, coerces what validation accepted, refuses a wrong return type [distributions]
- [x] A frequency that cannot give a positive interval is refused; a custom dead interval is refused; a scheduler cannot be built on one [guards]

## Documents

- [x] Collections are collected, default to their attribute name, can name an existing collection; class access gives the declaration; a subclass overrides [collections]
- [x] The key is indexed uniquely; a custom key is used throughout; a duplicate is refused [collections]
- [x] A task can reach a collection [collections]
- [x] `create`, `insert`, `insertMany` (nothing is a no-op), `save` replaces or creates, `update` merges without reading [collections]
- [x] `delete`, `deleteMany`, `get` misses cleanly, `findOne`, `find` filters/sorts/limits, `count` and `exists` [collections]
- [x] Documents come back typed; a field can be set back to `None`; a saved document stores its `None`s [collections]
- [x] `update` accepts nested models, rejects unknown or invalid fields, `None` for a missing document [collections]
- [x] Created, fetched and inserted documents are bound; a handmade one is not until stored; binding is not stored [collections, lifecycle]
- [x] A bound document saves, deletes and reloads itself, using the key it was stored under [collections, lifecycle]
- [x] Changing the key renames rather than copies; saving again after a rename keeps one row [lifecycle]
- [x] The task, scheduler and pile engines are collection engines [collections]
- [x] `utc_now` is naive UTC; `model_dump` keeps `None` and falsy values and is still pydantic's own [models]
- [x] Task defaults, unique uids, unknown status refused; `executionTime` accepts, rejects nonsense, serialises to seconds [models]
- [x] A task roundtrips through Mongo; emit stamps the factory; scheduler defaults [models]

### Timestamps

- [x] Tasks and items record `createdAt`, `claimedAt`, `finishedAt` [timestamps]
- [x] A new task has only been created; a run one records its claim and finish [timestamps]
- [x] Cancelled finished without a claim; incompatible and given-up tasks are finished [timestamps]
- [x] **New:** a cancelled item finished without a claim

## Reprs

- [x] CallSpec, FuncSpec, task, factory, scheduler, item, pile engine and pile declaration stay readable [repr]
- [x] **Changed:** one repr pattern on every collection engine; the default task engine named after its attribute (N7)
