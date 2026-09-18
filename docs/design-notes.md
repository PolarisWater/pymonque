# Design notes

Decisions that are made but not built, work that is on hold, and asymmetries that are deliberate.
Finding ids (B1, P3, N2…) refer to the engine anatomy review of `c3e75e2`.

3.0.0 is the rebuild these notes led to. Its design document (`docs/rebuild.md`, cited below as
"rebuild §n"), the behaviour checklist and the handoff were removed once it shipped; they are in
the history at `2797a57`.

## Decided while building

### Layer 1: settings, declarations, documents, calls

Built in `src/pymonque_next/`, tested in `tests_next/`. Small decisions made along the way:

- **Two kinds of declaration error.** A bad value raises pydantic's `ValidationError` naming the
  setting; a setting that does not exist raises `TypeError` naming it. Settings removed since 2.0
  (`maxAttempts` / `retryDelay` on `@task`; `retryDelay`, `itemsCollection`, `payload` on `pile`;
  `schedulersCollection`, `schedulerModel`, `taskEngine`, `policy` on `schedulers`;
  `tasksCollection`, `taskModel` on `tasks`)
  say what became of them. Removed app defaults (`taskMaxAttempts`, `taskRetryDelay`,
  `itemMaxAttempts`, `itemRetryDelay`) are refused the same way by `AppDefaults.of()`, which layer 4
  calls when an app class is defined.
- **The model and the collection by position, the rest by keyword:** `pile(Email, "outbox_items",
  maxAttempts=3)`. The model argument is called `model` on every kind.
- **`collection(key=)` must name a field of the model,** checked where written. An aliased key is
  stored, indexed and queried under its alias.
- **A declaration resolves itself; an engine takes what was resolved.** `collectionIn(db)` gives the
  collection — given, named, or the default name. `settingsWith(defaults)` gives a frozen
  `TaskEngineSettings`, `SchedulerEngineSettings` or `PileSettings`, and `@task`'s
  `limitsWith(defaults)` a `TaskLimits`: the shared settings the fingerprint will hash.
  `CollectionEngine(collection, model, name=, key=, extraIndexes=)` takes no app. `pollInterval`
  stays on `tasks()` / `schedulers()` as pacing, outside those settings.
- **Declarations have no `__get__`.** Class access gives the declaration; the intent for layer 4 is
  that the app sets each part it builds on the instance, under the same name.
- **Call arguments are checked with a TypedDict built from the signature,** not a pydantic model, so
  a parameter may have any name (`_private`, `model_config`), and arguments left out stay left out:
  the function applies its own defaults. The app's tasks and a distribution registry are both a
  `Functions` — `build`, `validate`, `call`.
- **`@task` names its call after the attribute it is declared as;** `self` may be positional-only,
  and an instance task without `self` is refused where written.
- **`emitsInto` accepts an engine the class inherits** (`schedulers(emitsInto=Parent.heavy)`) and
  refuses one the same class body replaces. **Decided for layer 4: follow the name.** When a
  subclass replaces the engine an inherited scheduler engine emits into, the scheduler engine emits
  into the subclass's engine, as a subclass's redefinition replaces the parent's everywhere else.
  The reference works as a name editors can check.
- **For layer 4, following from arguments left out:** a queued call does not store the defaults, so
  the function's defaults apply when it runs. The fingerprint must therefore hash each parameter's
  default with the signature, so a changed default refuses a mismatched worker like a changed type.
- **Documents.** `build()` gives a document bound to its engine but not stored: `storedKey` is set
  only once it is written, and cleared by `delete()`. An `update()` that changes the key returns the
  renamed document. MongoDB's `_id` is dropped on load, so a model forbidding extras still loads. The
  time fields of `Task`, `Scheduler` and `Item` turn an aware datetime into naive UTC.
- **Distributions:** `stdFraction` and `sigma` must be ≥ 0; a registry must subclass
  `BaseDistributions`.
- **Left for later layers,** because they need an engine or `BaseApp`: a declaration named `task` or
  `scheduler` replacing the default; reserved names; app defaults checked at class definition;
  `Scheduler.taskFields()` and emitting (`Task.runWork()` and scheduling came with layer 2).
  Checklist items that straddle layers stay unticked until their last part has a test.

### Layer 2: claims and task engines

`claims.py` and the task engine in `tasks.py`. Small decisions made along the way:

- **Renewal follows the claim, not the worker threads.** Each claiming engine has a `Leases`: a claim
  is held while its work runs and let go when it ends, and one thread per collection renews what is
  held, starting with the first claim and ending with the last. A lease is therefore renewed through
  a shutdown that waits for the work, with no tie to the worker loop. It renews every
  `leaseSeconds / 3` with no one-second floor, so a short lease is still kept. For layer 4: a timeout
  must stop renewing the pile items its thread holds (§5.10), so a hold will need to know its thread.
- **An outcome clears the claimId.** `writeClaimed` writes only while the claimId matches, and clears
  it in the same write, so a claim gets one outcome and a renewal racing it matches nothing. A
  cancel clears it too, so a worker whose lease lapsed cannot write over the cancel.
- **A claim returns the document as it found it.** A task still `running` was held by a worker that
  stopped renewing: the claim that finds it writes it off as `failed`, keeps the time it was
  started, logs an error, and does not run it.
- **A task's outcome writes only the outcome** — `status`, `claimedAt`, `finishedAt`,
  `executionTime`, `result`, `error` — so a field changed while the task ran survives.
- **The task engine takes what the app resolved:** the collection, model and name; the app's tasks as
  `taskFunctions(...)`, one `Functions` every task engine shares; `TaskEngineSettings`; each task's
  resolved `TaskLimits` — one entry for every task, built by the app from `@task(...)` and the app's
  defaults, so a bare `@task` has an entry too (usually "no timeout, no skipAfter"); users never
  declare limits on every task, and a missing entry is a wiring bug the engine refuses; the
  `DistributionEngine`; and a default
  factory, `TaskFactory(name="default")` unless given. `pollInterval` belongs to the worker loop.
- **`work()` claims and runs one task,** returning it as it ended, or None. The worker threads that
  loop on it come with layer 4, and so do timeouts: layer 2 resolves `timeout` but does not enforce it.
- **`schedule(work, deadline=, factory=, **fields)` takes only the fields the model adds to `Task`.**
  A limit is refused as belonging to `@task`, a field of `Task` itself as the engine's to keep, and
  anything else by naming the fields the model does add. The call is checked as `runWork()` returns
  it. `_newTask()` builds that checked task unstored, for a scheduler to emit.
- **`flagIncompatible()` marks waiting tasks with no function as incompatible, per engine;** when the
  app calls it, and across which engines, is §5.11.
- **In tests,** mongomock's `find_one_and_update` finds, then updates by `_id`, so two threads can
  take one document. `tests_next/conftest.py` locks it, standing in for MongoDB's atomic claim.
- **Decided after review, and built:**
  - **Calling a task engine checks only the arguments given** (`app.accountTasks("sync")`): unknown
    names and the types of what is given. `runWork()` may supply any argument, so completeness is
    left to `schedule()`, which checks the call as `runWork()` returns it. Calling a distribution
    engine keeps the full check, since nothing adds arguments to a distribution; the docs say why
    the two differ, in one sentence. Built as `Functions.validate(call, complete=False)`.
  - **A task left `running` by a dead worker whose function has since gone is written off by
    housekeeping,** alongside `flagIncompatible()`: marked `failed` with the worker-died error, not
    `incompatible`, since its worker died before its function went. Same version check; part of
    §5.11. Built as `writeOffStuck()`, for the app to call with `flagIncompatible()`.
- **Fixed after the layer 2 review:**
  - **An engine refuses a function with no `limits` entry,** as it refuses an entry with no function.
    `limitsFor` no longer falls back to no limits, so a task left out of the map by the app cannot
    silently lose `taskTimeout` / `taskSkipAfter`. The app (layer 4) passes every task's resolved
    limits, defaults filled in.
  - **`work()` writes the outcome inside `leases.holding(...)`,** so a claim is renewed until its
    outcome is written, not only until its call returns.
- **Fixed before layer 3** (found checking the review's build, `5a4732e`):
  - **A task written off by a claim loses when its worker died.** The worker-died error no longer
    carries the time, on the grounds that the stored `leaseUntil` keeps it; but the claim that finds
    the task sets a fresh lease first, and the write-off writes only the outcome fields, so the stored
    `leaseUntil` is the write-off claim's own lease (reproduced: lapsed 20:52:55, stored 21:07:55).
    `writeOffStuck()` is unaffected, since it does not touch `leaseUntil`. Fix: `_writeOff` also
    writes back the `leaseUntil` it found (`before["leaseUntil"]`), beside the `claimedAt` it already
    restores; a test checks the stored `leaseUntil` equals the lapsed one.

### Layer 3: scheduler engines and piles

The scheduler engine in `schedulers.py`, the pile engine and the `work()` block in `piles.py`. Small
decisions made along the way:

- **A scheduler engine takes the task engine it emits into** (`tasks=`), and draws intervals from that
  engine's `DistributionEngine`, so an app has one registry. Which engine that is — `emitsInto`, or
  the default — is layer 4's wiring.
- **A held scheduler has a `claimId` too** (B6). The claim sets only the claimId and the lease —
  `status` still means enabled or disabled — and the deadline write lets the claim go, landing only
  while the claim and the deadline are both unchanged, so `ensure()` or `update()` restarting the
  rhythm mid-claim wins. A held scheduler is still not renewed.
- **A scheduler's claim index is `leaseUntil` alone,** because its claim does not look at `status`;
  tasks and piles keep `(status, leaseUntil)`.
- **A beat's task is built by the target engine's `_newTask()`:** the scheduler's work unchanged, its
  `taskFields()`, the scheduler as factory, and a uid fixed to the beat. `add`, `upsert`, `update` and
  `ensure` build it the same way to check a scheduler as it will run: one whose fields the target
  `Task` does not have is refused with the `TypeError` `schedule()` gives, and one that leaves out a
  field the target needs with pydantic's `ValidationError`.
- **A beat the app cannot run is skipped with a warning** — no function (`TaskNotFound`), or a stored
  call that no longer fits (`TaskValidationError`) — and the scheduler stays enabled and walks on.
- **`build()`, `add()` and `ensure()` pass their fields to the model as given,** `uid`, `name` and
  `status` included, since `ensure()` sets those itself — unlike `schedule()`, which refuses `Task`'s
  own fields. **Superseded** — see "To fix before layer 4" below.
- **`schedulerUid()` and `beatUid()` keep 2.0's namespace,** so a scheduler declared by name keeps its
  uid across the upgrade.
- **Items are `running` while held.** A claim uses a try; an item found out of tries is given up at
  that claim, which moves on to the next item. `done()` no longer clears an earlier error, since
  nothing retries. `renewLease()` and `release()` need the item `running`. Passing an `Item` acts
  under its claim; passing a uid is an operator's verdict, whatever holds the item.
- **Cancelling is one mechanism:** `claims.notStarted()` and `claims.cancelled()` serve tasks and
  items — waiting, or held by a worker whose lease lapsed — and a cancel clears the claimId.
- **`work()` yields a `Work`, not the `Item`:** `w.data`, `w.uid`, `w.attempts`, `w.item`, and
  `w.done()`, `w.fail()`, `w.release()`, which call the pile's own and then raise `_Ended`.
  `pile.done/fail/release(item)` called outside a block write and return whether they did.
- **Superseded — see "To fix before layer 4" below.** Built as: an outer block's early end
  releases the inner block's item. When `outer.release()` (or `done` / `fail`) unwinds through a nested `work()` block, the
  inner item is handed back with its try, since its work did not happen, and the early end is passed
  on. The alternative, writing nothing, would spend a try and hold the item until its lease lapsed.
- **Engine reprs are `CollectionEngine`'s one pattern** (N7): `PileEngine outbox
  (pymonque_pile_outbox)`, not 2.0's `Pile outbox (…)`.
- **`CollectionEngine._assign()`** sets fields validated against the model; `update()` and the
  scheduler engine's merges share it.
- **Left for layer 4:** worker threads on scheduler engines and poll intervals; `init()`; and a pile
  item held by a timed-out call no longer being renewed (§5.10 — a hold will need its thread).
- **Fixed before layer 4** (from the layer 3 review of `2eab7ff`; these replace the two "for review"
  points above):
  - **Bug — a scheduler whose beat cannot be built sticks.** `_emit` catches only `TaskNotFound` and
    `TaskValidationError`. Building a beat also raises pydantic's `ValidationError` (the target `Task`
    now needs a field the scheduler does not supply) or `TypeError` (`taskFields()` gives a field the
    target does not have). `add`/`ensure` check that, but schedulers stored before a deploy that
    changed the task model are not rechecked. `work()` then raises before the deadline write, so the
    scheduler stays claimed, its deadline never moves, and it fails again after every lease.
    Reproduced: stored against `Task`, redeployed with `AccountTask(accountId: int)` → `ValidationError`,
    deadline not moved, claimId held. **Fix:** any error building the beat is handled like a missing
    task — the beat is skipped with a warning naming the scheduler and the error, the deadline moves on,
    and the scheduler stays enabled. An error drawing the interval (a removed distribution) cannot move
    the deadline; leaving it to retry after the lease is acceptable, logged by layer 4's worker loop. A
    test covers both model-change cases.
  - **Nested blocks: an outer early end requeues the inner item with its try spent.** Changes the
    "releases the inner block's item" choice above. The inner holder never said its work did not
    happen — the block may have done part of it — so returning the try contradicts what a try means,
    "a run whose outcome nobody knows". The inner item goes back to be claimed immediately (as a
    lapsed lease would, without waiting for the lapse) and keeps the try it used; out of tries, the
    next claim gives it up as usual.
  - **Scheduler fields: refuse the engine's own, as `schedule()` does.** Changes the "passed as given"
    choice above. `add()`, `upsert()`'s builders, `update()` and `ensure()` refuse `uid`, `claimId`,
    `leaseUntil` and `status` by name — enabling goes through `enabled=` — and keep `name`, and
    `deadline` on `update()` (moving a deadline by hand is a feature). `ensure()` sets `uid` and
    `status` itself, internally, not through the public fields.
  - **A deadline moved mid-claim releases the claim.** `update()` / `ensure()` moving a deadline also
    clear `claimId`, so the stored document matches "moving the deadline releases that claim" (today
    the stale claimId stays; harmless, but untrue).
  - **By-hand verdicts only touch unfinished work.** `done(uid)`, `fail(uid)` and `release(uid)` on a
    pile act only on an item that is `pending` or `running`; a finished one (`done`, `failed`,
    `canceled`) is left alone and they return False. 2.0 let an operator overwrite any status; this
    API does not. The same rule for any by-hand verdict tasks get later.
  - **Built as:** `_emit` skips the beat on any error building it; `PileEngine._handBack(item,
    returnTry=)` serves both `release()` and the nested case, and a given-up item's error now says
    its tries' outcomes never came back, which covers both; `SchedulerEngine._refuseFields()` guards
    `build`, `add`, `ensure` and `update` (whose `uid` is positional-only, so a `uid=` field is
    refused rather than clashing), and `ensure()` sets `uid` and `status` through the internal
    `_build()`; `_changes()` clears `claimId` whenever it writes a deadline; a uid given to `done`,
    `fail` or `release` matches only `pending` or `running` items.

- **Decided after the fixes** (two gaps found checking them):
  - **Saving a whole scheduler: the claim is the engine's — built.** `SchedulerEngine.save()` (so
    `upsert()` and a bound scheduler's `save()`) ignores whatever `claimId` the document carries. A
    stored scheduler whose deadline is unchanged keeps the claim and lease holding it, so the holder's
    deadline write still lands; a moved deadline releases the claim and puts the lease at the new
    deadline, as `update()` / `ensure()` do. Deadlines compare to the millisecond MongoDB keeps. The
    other fields are the caller's to shape. Three tests in `test_schedulers.py`.
  - **A stored scheduler that no longer fits its own model — accepted.** If a `Scheduler` subclass
    gains a required field, loading the stored document in `work()` raises before the beat, and it
    fails again after every lease, as a removed distribution does. Layer 4's worker loop logs it;
    documents in any collection break the same way after a schema change, and a migration is the fix.

### Layer 4: app, versions, workers, shutdown, timeouts

`app.py` (`BaseApp`), `versions.py` (fingerprint, worker registry), the worker loop in `claims.py`,
and timeouts in `tasks.py`. Small decisions made along the way:

- **`BaseApp` declares the default engines itself:** `task = tasks()` and `scheduler = schedulers()`
  are class attributes of `BaseApp`, so replacing one is replacing a declaration by name like any
  other. The reserved-name check lets `task` and `scheduler` be replaced only by a declaration of
  the same kind, and refuses every other declaration or `@task` under a name `BaseApp` has or
  `__init__` sets. The defaults (`taskTimeout`, …) are class attributes of `BaseApp` with their
  values, and are checked by `AppDefaults.of()` at class definition.
- **`emitsInto` follows the name** (as decided in layer 1): the app builds each scheduler engine
  against whatever task engine the class resolves that name to. A subclass that replaces it with
  something that is not a task engine is refused at class definition.
- **Each engine that runs workers has a `workers: WorkerLoop`** (`app.task.workers.running`,
  `.count`, `.busy`, `.stop()`), so an engine reports its own state. Its poll interval is the
  declaration's `pollInterval`, or the constructor's `taskPollInterval` / `schedulerPollInterval`.
  A failing iteration is logged naming the engine (`scheduler-nightly worker iteration failed`) and
  the worker carries on — which is how a stored scheduler that no longer fits its model shows up.
- **Worker counts** are checked: a dict naming an engine the app lacks raises `ValueError` listing
  the engines it has; a count that is not a non-negative int is refused.
- **The fingerprint is computed once, at construction,** and stored as `app.fingerprint`. It hashes
  each task's signature (defaults included, addresses stripped) with its resolved limits; each
  distribution's signature; and for each task engine, scheduler engine, pile and collection its
  name, collection name, resolved settings (`emitsInto` for scheduler engines, the key for
  collections) and model schema. A model's schema is its JSON schema; a model with a field that has
  none falls back to its fields' annotations and defaults. Only the collection's name is hashed,
  not its database.
- **The heartbeat reports counts by engine name:** `taskWorkers` and `schedulerWorkers` are dicts,
  and `abandonedThreads` is a number. `app.taskWorkers()` sums the first across live processes.
- **`init()` does its work on task engines only:** indexes are made at construction, so scheduler
  engines, piles and collections have no housekeeping. It logs what it flagged or wrote off.
- **Backlog:** `TaskEngine.backlog()` gives `(due, oldest wait)`; `app.backlog()` gives it for each
  engine, and `_checkBacklog()` warns once per engine behind.
- **Timeouts.** A task with a timeout runs its call in a thread of its own; one without runs in the
  worker. At the timeout:
  1. the call's stack is taken;
  2. every claim its thread holds, in any collection, stops being renewed (`abandonHolds()`, over
     every `Leases` in the process, since each hold now records its thread);
  3. `TaskStopped` is injected into the thread;
  4. a warning is logged, naming the task, its uid, the limit and the stack;
  5. the task is recorded as `timeout`, with the stack in its `error`.

  After `TaskEngine.stopGrace` seconds (3 by default; a per-process attribute), a watcher logs that
  the call stopped, or logs an error with the stack again and counts the thread abandoned.
  `engine.abandoned()` and `app.abandoned()` list abandoned calls whose threads are still alive.
- **`TaskStopped` derives from `BaseException`,** so `except Exception:` in a task does not swallow it.
  In a pile `work()` block it passes through without writing anything: the held item's lease lapses,
  which spends a try, as §2 says. If an abandoned call later finishes, its outcome still lands if
  nobody has claimed the item since, because the claim check still holds. Accepted.
- **Retiring:** `retireAfter=N` on the constructor (per process; None, the default, never retires).
  When the Nth thread is abandoned, the app logs every abandoned call with how long it has run and
  where it is stuck, then "no longer claiming, draining N", and calls `requestStop()`. It also sets
  `app.retired`. `run()` returns once the workers have drained, logs that it is exiting for the
  supervisor to restart it, and still returns whether it drained. The exit code is the caller's:
  `sys.exit(3 if app.retired else 0 if drained else 1)`, shown in `run()`'s docstring.
- **Left for layer 5:** the README and reference from the new shape, with an upgrade section; the
  migration that renames `pymonque_tasks` / `pymonque_schedulers` and the old statuses and fields;
  then replacing the old package.

### Layer 5: docs, the 2.0 upgrade, and replacing the old package

- **Decided with the user:** the upgrade is an explicit, idempotent `upgradeFrom2(app)`, refused
  while any worker process is live; replacing means moving the rebuild into `src/pymonque` and
  `tests`, deleting 2.0's code and tests, and version 3.0.0; everything is committed on `rebuild`,
  not merged or pushed.
- **`upgradeFrom2()` also renames each declared scheduler engine's 2.0 default,**
  `pymonque_schedulers_<name>`, to `pymonque_scheduler_<name>`; an engine declared with a collection
  of its own keeps it. Found writing the upgrade section: the decided rename covered only the
  defaults, which would have left declared engines empty after an upgrade.
- **Tasks 2.0 held back for a retry run once more** after the upgrade — they are `pending` — rather
  than being failed: 2.0 had promised them another run. A `processing` task becomes `running`, and
  the next claim writes it off.
- **`app.taskWorkers()` ignores a 2.0 heartbeat,** which reports one number rather than a count per
  engine.
- **The README and reference are written from the new shape;** their examples were run against the
  code before the old package was replaced. `reference.md` keeps a pointer to 2.0's "Upgrading from
  0.x" in the repository history.

### After the full review of 3.0.0

- **`run()` refuses to start no workers.** With both counts 0 — the defaults, or a dict of zeros — it
  used to start nothing, find nothing to wait for, and return `True` at once: a worker process that
  exits without saying why. It now raises `ValueError` before installing signal handlers or
  registering. `startWorkers()` still accepts no workers.
- **A timeout's stop cannot reach another thread.** The stop is raised by thread id, and an ended
  thread's id can be reused at once. The call's thread now marks itself finished under a lock, and
  the stop — dropping the thread's holds, then raising `TaskStopped` — is sent under that lock only
  while it is not (`tasks._StoppableCall`). A stop that lands as the call ends is swallowed in the
  thread rather than escaping as an unhandled exception.

### Lease edge cases (after 3.0.0)

- **One clock: the database server's.** `utc_now()` is this host's clock plus an offset measured from
  the server's `hello` `localTime` (half the round trip corrected), so every lease, deadline and
  heartbeat is compared on the server's clock, whichever host wrote it. Measured when an app is built
  and on every heartbeat; a host more than 1 s off is logged. Chosen over `$$NOW` in pipeline updates
  because mongomock ignores `$$NOW` silently, which would have left every lease untested; the offset
  also covers what `$$NOW` would not — skipAfter, the backlog, heartbeats, documents' own times. A
  server that will not say leaves the host's clock. One offset per process.
- **Short leases are logged** where the app is built: under 10 s, a stall of 2L/3 hands live work over.
- **`release(delay=…)`:** an item released with a delay is due once it has passed, and queues by that
  time rather than in its own place, so an item not ready yet no longer sits at the front of the pile
  taken and handed back by every claim. The try is still returned. No delay keeps the old behaviour.
  No pile-wide default delay: that would need a setting, an app default and a fingerprint entry, for
  what one call-site argument already covers.
- **Documented, not changed:** a frozen holder is indistinguishable from a dead one, so a pile item
  can be worked twice (use its `uid` as an idempotency key); a task whose worker died shows `running`
  until a worker of its engine claims next.

## Decided, not built yet

### No task retries; piles hold work that has to happen

- The model was clean from the start: schedulers "never" fail, and tasks either work or fail. A task
  that must succeed retries inside its own code; one that may fail needs no retry.
- Task `maxAttempts` and `retryDelay`, and the app's `taskMaxAttempts` / `taskRetryDelay`, go.
  A task whose worker dies is written off as failed, never rerun.
- Pile items keep tries, because the task holding one can die before freeing it. A claim uses a
  try, so a lapsed lease (no response) leaves it used. `fail()` is final: the item is marked failed
  and never claimed again. No retry delay.
- `release()` hands an item back unfinished and gives its try back: the holder says the work did
  not happen (shutting down, a rate limit, not ready yet), so a try means only "a run whose outcome
  nobody knows". An endless release loop is visible and the caller's to stop.
- Max tries is set at two levels: an app default and each pile's declaration.
- **Items can be cancelled, like tasks:** `cancel(uid)` and `cancelMany(where)` mark an item that is
  not running `canceled` and set `finishedAt`; a running item cannot be cancelled, only waited out.
  `canceled` becomes a status tasks and items share.

### Several task engines (B5, which also settles B1)

- An app may declare more than one task engine, each with its own collection.
- Tasks belong to no engine: every task engine can run any of the app's tasks. A scheduler engine
  emits into the task engine selected for it; everywhere else, users schedule on whichever task
  engine they want.
- Worker counts: `taskWorkers` and `schedulerWorkers` take an int, meaning that many threads on
  **every** engine of that kind, or a dict of engine name to count. Engines a dict leaves out get
  none.
- Still open when this is picked up:
  - ~~how a scheduler engine names its task engine~~ — decided: a reference,
    `schedulers(emitsInto=heavy)`, declared above; left out means the default engine;
  - ~~whether an int worker count covers the default task engine~~ — decided: yes;
  - ~~the collection name of a declared task engine~~ — decided: `pymonque_task_<name>` (rebuild §5.6);
  - ~~how `init()`, the backlog warning and the fingerprint span several task engines~~ — decided
    (rebuild §5.11): one fingerprint per app; `init()` cleans every task engine; the backlog warning
    is per engine and names it;
  - ~~whether the declaration can shape the engine's model, collection and indexes~~ — decided: yes,
    see custom tasks (rebuild §5.9).

### `with pile.work() as w:` — done, fail or release end the block early

```python
with app.outbox.work() as w:
    if w is None:
        return                  # pile empty
    if rateLimited():
        w.release()             # ends the block here: back on the pile, try returned
    if not w.data.to:
        w.fail("no recipient")  # ends the block here: failed, final
    if alreadySent(w.data):
        w.done("duplicate")     # ends the block here: done
    send(w.data)
# reaching the end of the block: done; an exception: failed and re-raised
```

- **The block is the unit of work.** `work()` claims one item and, for as long as the block runs,
  holds it: the lease is renewed in the background, and every outcome write checks the claim is
  still this one. It ends in exactly one outcome:
  - the block reaches its end → `done()`;
  - the block raises → `fail()` with the traceback, and the exception is re-raised;
  - `w.done(result)`, `w.fail(error)` or `w.release()` → that outcome, and the block ends there;
  - the claim was lost meanwhile (lease lapsed, taken over, cancelled) → nothing is written, and it is
    logged.
- **Write first, then end the block.** `w.done()`, `w.fail()` and `w.release()` record the outcome
  immediately — which clears the claim and stops lease renewal — and only then raise a private
  `_Ended(claimId)` to leave the block. Python can only leave a block by raising; `_Ended` derives
  from `BaseException` so `except Exception:` in the block does not catch it. `work()` catches it
  and suppresses it: the code after the `with` carries on, and nothing outside sees an exception.
  Helpers and loops inside the block are unwound on the way; `finally` and inner `with` cleanups run.
- `work()` handles only its own claim's `_Ended` and re-raises anyone else's, so nested blocks end
  the right item.
- `pile.done/fail/release(item)` called outside a block write directly and do not raise.
- **If something swallows the early end** — a bare `except:`, `except BaseException:`,
  `contextlib.suppress(BaseException)`, or `return`/`break`/`continue` in a `finally` — the outcome
  is already stored and stays: when the block later reaches its end or raises, the claim check finds
  the claim gone and writes nothing, and `work()` logs a warning that the block kept running after
  `w.release()` (or done/fail). What it cannot undo is the code that ran after the call: after a
  release or fail, another worker may already hold the item while this block carries on. Caveat to
  document.
- **In the final docs:** the main pile example is the `with` block. The docs cover that `done`,
  `fail` and `release` all end it, and everything the block does for you — lease renewal, the claim
  check on every outcome, done at the end, fail on an exception, the lost-claim case — with the
  caveat beside it.

### Custom tasks, shaped like custom schedulers (rebuild §5.9)

Builds on several task engines, and answers its open question about whether a declaration can
shape the engine's model, collection and indexes: yes, the way `schedulers()` does.

- **A task engine declaration takes a `Task` subclass,** as `schedulers()` takes a `Scheduler`
  subclass: `accountTasks = tasks(AccountTask, "account_tasks", extraIndexes=[...])`. The subclass
  adds fields stored on every task in that engine — context to query and index by, such as an
  account, a tenant or a priority.
- **Fields are given where a task is created,** and validated by pydantic:
  `app.accountTasks.schedule(App.sync(), accountId=42)`, like
  `app.accountOps.add(App.sync(), daily, accountId=42)`.
- **`Task.runWork() -> CallSpec` is the one place context is stamped:** the call that actually
  runs, by default `self.work`. A subclass stamps its fields onto the call —
  `return self.work.bind(accountId=self.accountId)`. `schedule()` validates `runWork()` against the
  signature.
- **Decided: `Scheduler.emitWork()` is removed.** A scheduler never stamps a call; it builds the
  model of the task engine it emits into. `Scheduler.taskFields() -> dict` (default `{}`) supplies
  the extra fields, so an `AccountScheduler` hands its `accountId` to an `AccountTask`, whose
  `runWork()` stamps it. A required field it doesn't supply fails when the scheduler is added, not
  when it emits. The scheduler's stored work is emitted unchanged.
- **Cost, accepted:** a scheduler with context cannot emit into a plain task engine; it needs one
  whose `Task` subclass has the fields — a declared engine, or `task = tasks(AccountTask)`. In
  return, context has one home, and tasks scheduled by hand get it the same way.
- **Tasks still belong to no engine,** so a subclass's fields and `runWork()` apply to every task
  stored in that engine, whichever function it names.
- **Decided:** extra fields are data only and do not steer claiming; a priority order can come later.

## On hold

- **Verbs that differ between tasks and items (B4, Matrix 3):** waiting on an item, finishing a
  task by hand. Revisit after several task engines. (Cancelling an item is decided, above.)
- **A hold duration for pile items,** like a task's `executionTime`. To be designed as something
  modular after several task engines, rather than forced onto items now.
- ~~Not yet discussed: B6, P1–P4, N1–N7~~ — all decided, in rebuild §5.

## Deliberate, not asymmetries

- **Schedulers have no retries, timeouts or staleness rule beyond `missed`.** Emitting is a database
  read and write, assumed not to fail; tasks can fail all day.
- **Pile items have leases and tries, tasks have neither retries nor tries** because the task
  holding an item can die before it releases it, while a task either works or fails.
- **Tasks record `executionTime`** because the library was built in part to fit a system that
  needed it.
