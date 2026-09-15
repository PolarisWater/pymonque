# Rebuild checklist

Every behaviour the old test suite guarantees, grouped by `docs/rebuild.md` §2, plus what the
decisions add. Tick an item when the new package has a test for it. Written from the tests at
`68c168d` (482 tests, all passing); the old test file each came from is in brackets.

Marks: **Changed** — kept, but reshaped by a decision; **Dropped** — removed by a decision, port
nothing; **New** — no old test, decided since.

## Claiming and leases

- [ ] A due task is claimed and run; a future one is left alone; working nothing is a no-op [tasks]
- [ ] Oldest deadline first; a task is never claimed twice, also across worker threads [tasks, app]
- [ ] A fresh task carries a lease equal to its deadline; claiming pushes it out [leases]
- [ ] A long task is not taken over while its lease is renewed; renewal stops when the work does [leases, tasks]
- [ ] A lapsed lease is claimable by the next worker; a live one is left alone — tasks, schedulers, items [tasks, schedulers, piles, leases]
- [ ] A new app instance does not disturb work in flight or live leases [leases, piles]
- [ ] Leases keep renewing while a shutdown drains [leases]
- [ ] Indexes back the claim query — tasks, schedulers, piles [tasks, schedulers, piles]
- [ ] **Changed:** every claim gets its own `claimId` — tasks, items and held schedulers, one mechanism (B6) [claims]
- [ ] A stale holder cannot finish, fail, release or renew; the current holder can [claims]
- [ ] Renewal skips work another worker took; a renewal never overwrites an outcome [claims, tasks]
- [ ] A lost task claim does not overwrite a cancel; a taken-over task keeps the new claim's outcome [claims]
- [ ] `work()` logs when its outcome was not recorded [claims]
- [ ] Moving a deadline by hand (save or update) moves a waiting task's or scheduler's lease [tasks, schedulers]
- [ ] **Dropped:** a waiting retry keeps its retry time on save; renewal skips a waiting retry [tasks]

## Failure

- [ ] A raising task is marked failed with the traceback [tasks]
- [ ] `sys.exit()` in a task or a `work()` block is a failure, not a dead worker thread [tasks, piles]
- [ ] A result the driver cannot encode is a failure and does not leave the task claimed [tasks]
- [ ] A task whose function vanished is flagged incompatible at `init()`; a finished one is not [tasks]
- [ ] **Changed:** a task whose worker died is `failed`, never rerun, with an error saying so [retries]
- [ ] **Dropped:** task retries — retried until the last attempt, retry delay, success clears the old error, app default for retries, emitted tasks retry, crash below the limit recovered, outdated retry not run, `wait` treats a retry as unfinished [retries, wait, timestamps]
- [ ] **New:** no `maxAttempts` / `retryDelay` on `@task` or the app; declaring them is refused

### Timeouts

- [ ] No time limit by default; `@task(timeout=…)` declares one, looser or tighter than `taskTimeout` [timeouts]
- [ ] An emitted task times out by its task's limit [timeouts]
- [ ] A timed-out task records why and warns; a raise inside a timed task is still a failure [timeouts]
- [ ] A timeout frees the worker for the next task, and is never rerun [timeouts, retries]
- [ ] **New:** a pile item held by a timed-out call stops being renewed, so its lease lapses
- [ ] **New (§5.10):** a timed-out thread gets an exception injected; the log says whether it stopped, with its stack
- [ ] **New (§5.10):** opt-in retiring: past N abandoned threads a worker stops claiming, drains, and exits, logging which tasks and where they were stuck

### skipAfter

- [ ] Without a limit a task runs however late; `None` makes a task unskippable [stale]
- [ ] The app default applies to a bare task; a task declares its own in either direction [stale]
- [ ] An outdated task is not run, says how late, and was claimed and finished [stale, timestamps]
- [ ] An emitted task goes stale by its task's limit [stale]

## Piles

- [ ] Each pile gets its own collection; piles are listed on the app; class access gives the declaration [piles]
- [ ] The collection name can be overridden, or given as a collection object; a subclass can override a pile [piles, engines]
- [ ] Two piles do not share items [piles]
- [ ] `add` takes a model, a dict or kwargs; the payload is stored as plain data, defaults filled in, invalid refused [piles]
- [ ] An untyped pile takes any dict; a payload is a plain model, not a document [piles, guards]
- [ ] `addMany` inserts in one go, validates everything first, and nothing is a no-op [piles]
- [ ] `claim` takes the oldest item, marks it, counts the try; empty or unmatched filter gives `None` [piles]
- [ ] No two claims return the same item; a claimed item is not offered again [piles]
- [ ] `done` records a result; `fail` records an error; both report whether they changed an item [piles]
- [ ] An item can be finished by uid, as an operator's verdict, whatever the claim [piles, claims]
- [ ] An item never claimed cannot be finished as a claim [claims]
- [ ] `count`, `counts`, typed `find`, `purge` of one status only [piles]
- [ ] Finished items survive a restart [piles]
- [ ] A held item's lease can be renewed and reports whether there was one [piles]
- [ ] A live holder is not taken over [piles]
- [ ] An abandoned item with tries left is claimed again; out of tries it is given up at the next claim, without blocking the next item [piles]
- [ ] A given-up item cannot be finished by its old holder [claims]
- [ ] A pile can override the app's max tries; a bad value fails where it is written [piles, declarations]
- [ ] A pile can declare extra indexes [piles]
- [ ] `release` puts the item back in its own place, is claimed afresh, gives the try back, and needs a claim [piles, claims]
- [ ] **Changed:** `fail()` is final, always — not only by uid or by default [piles]
- [ ] **Dropped:** a failure with tries left goes back on the pile; item retry delay; done after a retry clears the error [piles]
- [ ] **New:** `cancel(uid)` / `cancelMany(where)` cancel an item not being worked, set `finishedAt`; a running one cannot be

### The `work()` block

- [ ] Reaching the end marks the item done; an exception marks it failed and re-raises [piles]
- [ ] Yields `None` on an empty pile; takes a filter [piles]
- [ ] A task can drain the pile, and one on an empty pile still succeeds [piles]
- [ ] The whole loop: a scheduler fires a task that claims a pile item [app]
- [ ] **New:** `w.done(result)`, `w.fail(error)`, `w.release()` write first, then end only the block; the code after the `with` runs
- [ ] **New:** a nested block passes on an outer block's early end
- [ ] **New:** a swallowed early end keeps its outcome, writes nothing more, and logs that the block kept running

## Schedulers

- [ ] `add` stores an enabled scheduler, first deadline one interval out; refuses unknown task or distribution [schedulers]
- [ ] A due scheduler emits a task; a future one stays put; the earliest deadline fires first [schedulers]
- [ ] Firing advances the deadline one interval and returns it to enabled; never claimed twice [schedulers]
- [ ] A disabled scheduler walks its deadline without emitting or warning [schedulers]
- [ ] An emitted task is runnable and points back at its scheduler [schedulers, models]
- [ ] Missed beats: `replay` beat by beat, `once` one run then from now, `skip` nothing then from now [schedulers]
- [ ] A missed beat warns; merely due does not, and emits whatever `missed` says [schedulers]
- [ ] A scheduler faster than it can be served stops accumulating; `missed` applies after startup too [schedulers]
- [ ] A scheduler whose task vanished skips its beats and stays enabled [schedulers]
- [ ] A beat reclaimed after a crash is not emitted twice [schedulers]
- [ ] A rhythm restarted mid-claim is not overwritten [schedulers]
- [ ] A scheduler field can be cleared [schedulers]
- [ ] `ensure`: creates, idempotent across restarts, uid from the name, names don't collide, keeps the deadline, updates changed work [schedulers]
- [ ] Changing work keeps the rhythm; a new distribution restarts it [schedulers, engines]
- [ ] `ensure` leaves disabled disabled, can force state, can create disabled, validates first [schedulers]
- [ ] Declared and added schedulers coexist; unnamed ones don't collide; get / remove / declare again [schedulers]
- [ ] Concurrent declarations create one scheduler [schedulers]
- [ ] Is disabled, not cancelled [cancel]

### Scheduler engines and subclasses

- [ ] Engines are collected, each with its own collection; class access gives the declaration [engines]
- [ ] A declaration named `scheduler` replaces the default; a subclass can override an engine [engines, app]
- [ ] Per-engine `missed` and poll interval; extra indexes created; `init()` and workers cover every engine [engines]
- [ ] The default scheduler emits its work unchanged; a subclass stamps context via `emitWork()` [engines]
- [ ] The context reaches the emitted task, which runs; the stored work stays uncontextualised [engines]
- [ ] Work is validated as it will be emitted; an engine that cannot supply the context refuses it [engines]
- [ ] Bad kwarg, unknown task, unknown distribution, missing context field are refused [engines]
- [ ] Each engine reads its own model; context survives a fire [engines]
- [ ] `upsert` creates then replaces and validates; `find` and `count` filter; `get` misses cleanly [engines]
- [ ] `update` changes work or context, keeps or restarts the rhythm, toggles enabled, validates the merge, writes nothing if invalid, `None` if missing [engines]
- [ ] `ensure` takes context fields, stays idempotent, can change context, validates through `emitWork()` [engines]
- [ ] `delete` and `deleteMany` [engines]
- [ ] **Changed:** schedulers share the default task engine unless `emitsInto=<declaration>` names another [engines]
- [ ] **New:** `emitsInto` must reference a declaration above it on the same app class

## Task engines

- [ ] `schedule` stores a pending task, validates first, defaults to now, stamps the default or a named factory [tasks]
- [ ] `scheduleFromDistribution` pushes the deadline out and validates the distribution [tasks]
- [ ] `work` returns the task it ran; a queue drains by looping on it [tasks]
- [ ] An instance task runs with its app; a classmethod can be a task; a subclass's plain method unregisters it [tasks]
- [ ] A task gets models and coerced values, not stored data [tasks]
- [ ] `update` validates before writing [tasks]
- [ ] A task can be given a model [collections]
- [ ] Cancel: a waiting or abandoned task can be; running or finished cannot; unknown uid reports nothing; `cancelMany` filters and leaves running work alone; a cancelled task never runs [cancel]
- [ ] `wait` returns the finished task, takes a uid, works from another process, counts a failure as finished, times out, errors on an unknown task [wait]
- [ ] **New:** several task engines, each with its own collection; any engine runs any task
- [ ] **New:** a `Task` subclass on a declaration adds fields given to `schedule(…)`; `Scheduler.taskFields()` supplies them

## Processes and versions

- [ ] Engines are built, share the app's database and collections; default collection names [app]
- [ ] Two apps on one database share state; separate databases stay separate [app]
- [ ] An app with no tasks or piles starts; `startWorkers` defaults to none [app]
- [ ] Task workers drain tasks and never run one twice; a scheduler worker keeps emitting [app]
- [ ] A worker survives and logs a failing iteration [app]
- [ ] A backlog drains without waiting per task; an idle worker waits; work reports whether it did anything [version]
- [ ] Poll intervals and defaults reach the engines [app]
- [ ] **Changed:** worker counts — an int is that many threads on every engine of the kind, a dict sets engines by name [app]
- [ ] **Changed:** the distribution registry is a class attribute, and reaches the engines [app]

### Fingerprint and registry

- [ ] Stable; changes with a signature, an added task, a distribution registry, a declared limit, an app default, an item max tries, `missed` [version, limits]
- [ ] A body change alone does not change it; a default a task overrides does not matter; an address in a repr does not split it [version, limits]
- [ ] Two apps of one class agree; a default is not a constructor argument [version]
- [ ] **New:** changes with lease length, each declared model's schema, collection name and key (P1, P4)
- [ ] A matching worker may join; a mismatched one is refused, naming both versions [version]
- [ ] Constructing an app is never refused; a stale worker does not block a deploy; enforcement can be turned off [version]
- [ ] A registered worker reports itself; the heartbeat keeps it live [version]
- [ ] `init()` is refused beside a different live version, cannot flag its tasks, allowed alone or beside the same version, skipped when enforcement is off [version]

### Backlog

- [ ] Empty queue no backlog; backlog is due and unclaimed, not held, not future [backlog]
- [ ] No warning while young; warns when no one runs task workers or all are busy; silent once drained [backlog]
- [ ] The count spans processes; warnings can be turned off [backlog]
- [ ] **New:** backlog spans every task engine

### Shutdown and lifecycle

- [ ] `stopWorkers` stops the loops, not delayed by the poll interval [shutdown]
- [ ] Work in flight is finished; nothing new is claimed once stopping; a timeout reports what it could not wait for [shutdown]
- [ ] `requestStop` does not block; workers can start again after a stop [shutdown, lifecycle]
- [ ] A clean stop frees the version slot; a stopped process does not block the next version [shutdown, lifecycle]
- [ ] Starting twice leaves one heartbeat; stopping takes the monitor threads with it [lifecycle]
- [ ] SIGTERM starts a graceful shutdown; a second signal exits; opt-in; previous handlers restored [shutdown]
- [ ] `run` blocks until a signal, then drains; an engine reports its own state [shutdown]

## Declarations

- [ ] A bad setting fails where it is written and names itself; a bad app default fails at class definition [declarations, limits]
- [ ] Settings coerce like pydantic fields [declarations]
- [ ] Left-out settings take the app's defaults; given ones win; `None` only means no limit [declarations, limits]
- [ ] An existing collection object is accepted [declarations]
- [ ] A bare task takes the defaults; a declared limit wins, the rest default; both decorator forms work [limits]
- [ ] A task runs under its function's limits; declared once; calls, stored tasks and schedulers carry no limits [limits]
- [ ] A declaration or task cannot shadow the app; the reserved names cover every attribute `__init__` sets [app]
- [ ] **Changed:** one declaration shape, `kind(Model, collection=, extraIndexes=, …)`, and one noun per kind (N1–N3)
- [ ] **Changed:** status vocabulary `pending` / `running` / `done` / `failed` / `canceled` (+ task-only `timeout`, `outdated`, `incompatible`) (N4)

## Calls

- [ ] The string form and the class form build the same call spec [tasks]
- [ ] Unknown task, missing, unknown or wrongly typed argument refused; an optional one may be omitted; an instance task does not expect `self` [tasks, specs]
- [ ] `*args` and positional-only refused where written; `**kwargs` accepts extras but checks the named ones [guards]
- [ ] Unannotated accepts anything; an `Annotated` constraint is enforced [guards, specs]
- [ ] CallSpec: packs kwargs, dispatches against a mapping or an object, `KeyError` on unknown, roundtrips through Mongo [specs]
- [ ] `bind` merges kwargs, overrides without touching the original [engines]
- [ ] FuncSpec: class access gives one, calling it builds a CallSpec; instance access runs a staticmethod or binds an instance task [specs]
- [ ] Tasks are discovered across the MRO; a subclass overrides; a plain method is not a task [specs]
- [ ] Staticmethods collected through the MRO, the child's wins; `uuid4str` unique [specs]

## Distributions

- [ ] `constant` is exact; every distribution gives a positive interval; mean matches the daily frequency; `normal` never negative [distributions, guards]
- [ ] The registry walks the MRO; a custom registry extends the built-ins; the default does not see custom ones [distributions]
- [ ] Calling builds and validates a CallSpec; unknown, missing, unknown or wrongly typed argument refused [distributions]
- [ ] `gen` produces an interval, coerces what validation accepted, refuses a wrong return type [distributions]
- [ ] A frequency that cannot give a positive interval is refused; a custom dead interval is refused; a scheduler cannot be built on one [guards]

## Documents

- [ ] Collections are collected, default to their attribute name, can name an existing collection; class access gives the declaration; a subclass overrides [collections]
- [ ] The key is indexed uniquely; a custom key is used throughout; a duplicate is refused [collections]
- [ ] A task can reach a collection [collections]
- [ ] `create`, `insert`, `insertMany` (nothing is a no-op), `save` replaces or creates, `update` merges without reading [collections]
- [ ] `delete`, `deleteMany`, `get` misses cleanly, `findOne`, `find` filters/sorts/limits, `count` and `exists` [collections]
- [ ] Documents come back typed; a field can be set back to `None`; a saved document stores its `None`s [collections]
- [ ] `update` accepts nested models, rejects unknown or invalid fields, `None` for a missing document [collections]
- [ ] Created, fetched and inserted documents are bound; a handmade one is not until stored; binding is not stored [collections, lifecycle]
- [ ] A bound document saves, deletes and reloads itself, using the key it was stored under [collections, lifecycle]
- [ ] Changing the key renames rather than copies; saving again after a rename keeps one row [lifecycle]
- [ ] The task, scheduler and pile engines are collection engines [collections]
- [ ] `utc_now` is naive UTC; `model_dump` keeps `None` and falsy values and is still pydantic's own [models]
- [ ] Task defaults, unique uids, unknown status refused; `executionTime` accepts, rejects nonsense, serialises to seconds [models]
- [ ] A task roundtrips through Mongo; emit stamps the factory; scheduler defaults [models]

### Timestamps

- [ ] Tasks and items record `createdAt`, `claimedAt`, `finishedAt` [timestamps]
- [ ] A new task has only been created; a run one records its claim and finish [timestamps]
- [ ] Cancelled finished without a claim; incompatible and given-up tasks are finished [timestamps]
- [ ] **New:** a cancelled item finished without a claim

## Reprs

- [ ] CallSpec, FuncSpec, task, factory, scheduler, item, pile engine and pile declaration stay readable [repr]
- [ ] **Changed:** one repr pattern on every collection engine; the default task engine named after its attribute (N7)
