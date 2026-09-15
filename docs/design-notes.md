# Design notes

Decisions that are made but not built, work that is on hold, and asymmetries that are deliberate.
Finding ids (B1, P3, N2…) refer to the engine anatomy review of `c3e75e2`.

The library is to be rebuilt rather than refactored further; `docs/rebuild.md` gathers everything
here, plus the invariants the rebuild must keep and the decisions to make before building.

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
  `Task.runWork()` and `Scheduler.taskFields()`; emitting and scheduling. Checklist items that
  straddle layers stay unticked until their last part has a test.

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
  - **still open, before layer 4:** how `init()`, the backlog warning and the fingerprint span
    several task engines (rebuild §5.11);
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
