# OOP and OOD: Exercises

> Module-level problem set for `04-oop-and-ood`. This is the **design** module: cohesion, coupling,
> dependency injection, domain modeling, aggregates, the three architectures (layered, hexagonal, clean),
> SOLID, DRY/KISS/YAGNI, and the Law of Demeter — applied to the
> **Distributed Task Queue and Event Processing Platform** we build across the whole roadmap.
>
> **Solutions live in [`./solutions.md`](./solutions.md), keyed by the same IDs** (`E1`…`H10`). Do not
> peek until you have a compiling attempt or a finished diagram. Every exercise is numbered so the
> solutions file references it directly.

This set is deliberately **design-heavy and refactoring-first**. You will:

- split god classes into cohesive collaborators and name each one's *single reason to change*,
- apply all five **SOLID** principles to the worker subsystem,
- draw **aggregate boundaries** and encode invariants inside the root,
- convert a **layered** stack to **hexagonal** (ports and adapters) with inward-only dependencies,
- introduce **dependency injection** by hand and then with Spring.

Most of the unit of work here is *reshaping a design*, not writing one method. Where a problem asks for
code, write **compiling Java 21** — records, sealed interfaces, switch pattern matching, `var` where it
reads well. Where it asks for a design, deliver a Mermaid `classDiagram`/`flowchart` plus crisp prose.

---

## How the exercises are numbered

The IDs encode **difficulty**, and a `[tag]` encodes **kind**:

| Band | IDs | Difficulty | What it drills |
| --- | --- | --- | --- |
| Knowledge-Check | `E1`–`E8` | Easy | definitions you must be able to state in one breath |
| Coding | `E9`, `E10`, `M1`–`M4` | Easy→Medium | small, compiling, dependency-inverted units |
| Refactoring | `M5`–`M9` | Medium | god-class split, polymorphism, composition, ports, good-DRY |
| Design | `M10`–`H3` | Medium→Hard | aggregates, architecture choice, SOLID, package structure |
| Interview | `H4`–`H7` | Hard | design conversations under pushback |
| Stretch | `H8`–`H10` | Hard | repo-shaped: ArchUnit gate, zero-infra tests, redesign a "clever" mess |

Tags used below: `[coding]`, `[refactor]`, `[design]`, `[interview]`, `[stretch]`. Knowledge-Check
items are untagged short-answer warm-ups.

```mermaid
flowchart LR
    KC["E1-E8<br/>knowledge"] --> CODE["E9-M4<br/>coding"]
    CODE --> REF["M5-M9<br/>refactor"]
    REF --> DES["M10-H3<br/>design"]
    DES --> INT["H4-H7<br/>interview"]
    INT --> STR["H8-H10<br/>stretch"]
    style KC fill:#1f2937,stroke:#60a5fa,color:#fff
    style STR fill:#7f1d1d,stroke:#fca5a5,color:#fff
```

> **Budget.** ~1 hour for the Knowledge band, ~4–6 hours for Coding+Refactoring, a full day for the
> Design and Stretch problems. The Refactoring and Design bands are where the real learning is.

---

## The canonical model you will work with

Every exercise references the roadmap's shared domain model. The minimum slice you need for most
problems (the full model is defined in [domain-modeling.md](./domain-modeling.md)):

```java
public enum TaskStatus { PENDING, SCHEDULED, RUNNING, SUCCEEDED, FAILED, RETRYING, DEAD }

public record Task(
        String id,                       // a UUID
        String type,
        String payload,                  // JSON string
        TaskStatus status,
        int attempts,
        int maxAttempts,
        java.time.Instant createdAt,
        java.time.Instant scheduledAt,
        int priority) {

    // You will add behavior to this record in several exercises (M1, M9, M10).
    public Task withStatus(TaskStatus s) {
        return new Task(id, type, payload, s, attempts, maxAttempts, createdAt, scheduledAt, priority);
    }
    public Task withAttempt(int n) {
        return new Task(id, type, payload, status, n, maxAttempts, createdAt, scheduledAt, priority);
    }
}

@FunctionalInterface
public interface TaskHandler { TaskResult handle(Task task) throws Exception; }

public record TaskResult(boolean success, String message, boolean retryable) {
    public static TaskResult ok(String m)   { return new TaskResult(true,  m, false); }
    public static TaskResult fail(String m) { return new TaskResult(false, m, true);  }
}

public interface TaskQueue {
    void enqueue(Task t);
    Task dequeue() throws InterruptedException;
    int size();
}
```

> **Two notes on equality.** `Task` is an **entity** — two snapshots of the same task (before and after a
> retry) are the *same* task. The record's auto-generated `equals` compares *all* fields, which is
> **value-object** equality and wrong for an entity; several exercises ask you to override `equals`/
> `hashCode` to compare on `id` only. `TaskResult` and `Duration` *are* value objects, so field equality
> is correct there.

---

## Part A — Knowledge-Check (E1–E8) · Easy

Short-answer. Aim for two to five precise sentences each. No code unless stated. These map one-to-one to
the same IDs in [`solutions.md`](./solutions.md).

### E1 — Cohesion and coupling, one sentence each
Define **cohesion** and **coupling** in one sentence each *without using the word "related."* Then say
which you want **high** and which **low**, and explain why they are *correlated* (not opposites). Give
one concrete example from our project where splitting a class to raise cohesion *also* lowered coupling.
See [cohesion.md](./cohesion.md) and [coupling.md](./coupling.md).

### E2 — The seven cohesion types, worst to best
Name the seven cohesion types from **worst to best**. Then state which one `Worker` aims for and justify
it in one line. (Hint: `Worker` pulls a task, finds its handler, runs it, records the outcome — that is
*one* job.)

### E3 — Dependency Inversion vs Dependency Injection
State the **Dependency Inversion Principle (DIP)** and explain how it differs from **Dependency
Injection (DI)**. Which is the *design rule* and which is the *technique*? Give the canonical example:
`Worker` depends on the `TaskQueue` interface, not on `InMemoryTaskQueue`. Name one way you can satisfy
DIP *without* constructor injection. See [dependency-injection.md](./dependency-injection.md).

### E4 — Entity vs Value Object
In Domain-Driven Design, what is the difference between an **Entity** and a **Value Object**? Classify
each of these from our model and justify: `Task`, `TaskResult`, `Duration`, `Job`. What goes wrong if you
give `Task` the record's default all-fields `equals`? See [domain-modeling.md](./domain-modeling.md).

### E5 — Aggregate and Aggregate Root
Define **aggregate** and **aggregate root**. Give one aggregate from *our* domain and name the invariant
its root protects. Why must external code hold a reference only to the *root*, never to an internal
member? Why is one giant "everything" aggregate a bad idea? See [aggregates.md](./aggregates.md).

### E6 — Layered vs hexagonal: the one structural difference
In one or two sentences, what is the **single structural difference that matters** between layered and
hexagonal architecture? (Hint: it is about the *direction* of the persistence dependency, not about
whether layers exist.) See [layered-architecture.md](./layered-architecture.md) and
[hexagonal-architecture.md](./hexagonal-architecture.md).

### E7 — Law of Demeter, spotted
State the **Law of Demeter** ("don't talk to strangers"): a method may call methods only on which four
categories of object? Then identify the violation in:

```java
worker.getQueue().getMetrics().increment();
```

Explain *why* it is brittle (how many classes' internal structure does the caller now depend on?) and
give the smallest fix. Does the Law of Demeter ban *all* method chaining? See
[law-of-demeter.md](./law-of-demeter.md).

### E8 — DRY vs KISS vs YAGNI
Define **DRY**, **KISS**, and **YAGNI** in one line each. When do **DRY** and **YAGNI** conflict, and who
wins? Distinguish "duplicate **knowledge**" from "duplicate **text**," and state a rule of thumb for when
to extract a shared abstraction. See [dry-kiss-yagni.md](./dry-kiss-yagni.md).

---

## Part B — Coding (E9, E10, M1–M4)

Write compiling Java 21. Inject dependencies through constructors; never `new` infrastructure inside
business logic.

### E9 — `[coding]` · Easy · Make `Worker` depend on abstractions (constructor injection)
You are handed a `Worker` that constructs its own collaborators:

```java
class Worker implements Runnable {
    private final TaskQueue queue = new InMemoryTaskQueue();            // hard-wired concrete type
    private final Map<String, TaskHandler> handlers = new HashMap<>();  // built internally, unconfigurable

    @Override public void run() { /* dequeue, look up handler, run */ }
}
```

Rewrite it so both the queue and the handler map are **injected via the constructor**, satisfying DIP.
Requirements:

1. The constructor parameter for the queue must be the **interface** `TaskQueue`, not a concrete class.
2. Defensively copy the handler map (`Map.copyOf`) so a caller cannot mutate the registry after
   construction.
3. The `run()` loop must exit cleanly on interruption (restore the interrupt flag; do not swallow
   `InterruptedException`).

Explain in one sentence why constructor injection makes "there is no valid `Worker` without a queue" a
*compile-time* fact, and why setter injection does not.

### E10 — `[coding]` · Easy · Replace the silent `null` handler with a Null Object
In E9, when no handler matches a task type, the loop silently `continue`s. Make "no handler" an explicit,
cohesive collaborator using the **Null Object** pattern:

1. Write `UnknownTypeHandler implements TaskHandler` that records a metric (`task.unknown_type`) and
   returns a **non-retryable** failure (retrying an unroutable task forever is a poison-pill loop).
2. Write a `HandlerRegistry` whose `resolve(String type)` is a **total function** — it returns the Null
   Object fallback instead of `null`.
3. Refactor `Worker` so it calls `registry.resolve(type).handle(task)` with **no `null` branch**.

Use this minimal collaborator so your snippet compiles:

```java
interface MetricsCollector { void increment(String name, String tag); }
```

Explain why throwing an unchecked exception from `resolve` would be the *wrong* choice (a missing handler
is *data*, e.g. deploy skew, not exceptional control flow).

### M1 — `[coding]` · Medium · Model `RetryDelay` as a Value Object with validation
Replace a raw `long delayMillis` scattered through the retry code with a self-validating immutable value
object. Implement `RetryDelay`:

1. A `record RetryDelay(java.time.Duration value)` with a **compact constructor** that rejects `null`,
   rejects negative durations, and caps the value at 30 minutes (a real production rule: never schedule a
   retry past the visibility window).
2. Static factories `ofSeconds(long)` and `none()`, plus a `plus(RetryDelay)` combinator and `isZero()`.
3. Three JUnit 5 + AssertJ tests proving: a negative delay is rejected, an over-cap delay is rejected,
   and two equal-valued `RetryDelay`s are `.equals()` (value equality is *correct* here).

State the principle this demonstrates: **make illegal states unrepresentable**, so downstream code never
has to defend against a -3-second delay. See [domain-modeling.md](./domain-modeling.md).

### M2 — `[coding]` · Medium · `RetryPolicy` as a Strategy with two implementations
Build the `RetryPolicy` abstraction and two interchangeable strategies so the retry behavior can be
swapped with **no caller code change** (open/closed in action):

```java
public interface RetryPolicy {
    /** Empty => give up (send to DLQ). attempt is 1-based: the attempt we are about to schedule. */
    java.util.Optional<java.time.Duration> nextDelay(int attempt);
}
```

- `FixedDelayRetryPolicy(Duration delay, int maxAttempts)` — same delay until `attempt > maxAttempts`,
  then `Optional.empty()`.
- `ExponentialBackoffRetryPolicy(Duration base, int maxAttempts, Duration cap)` — `base * 2^(attempt-1)`,
  capped at `cap`, with **jitter** applied. Guard the shift so a runaway `attempt` cannot overflow into a
  negative delay.

Then show a caller using the policy with exactly **one** branch:
`policy.nextDelay(n).map(this::schedule).orElseGet(this::sendToDlq)`. Explain why **jitter is mandatory**
in production (the thundering-herd problem) and why returning a sentinel `-1` `Duration` for "give up" is
a bug magnet compared to `Optional.empty()`.

### M3 — `[coding]` · Medium · Apply the Law of Demeter: a tell-don't-ask `Worker` metric
Kill the `worker.getQueue().getMetrics().increment()` train wreck from E7 by giving `Worker` a
**tell-style** API. Requirements:

1. `Worker` holds its **own** `MetricsCollector` field (injected), `TaskQueue`, and `HandlerRegistry`.
2. The outside world *tells* the `Worker` to process; the `Worker` records its own outcome internally —
   no caller chains through `getQueue()`/`getMetrics()`.
3. Extract a private `process(Task)` that increments `task.succeeded`, `task.failed`, or `task.errored`
   depending on the result, so a handler exception cannot crash the loop.

State which classes' public APIs *no longer* need a getter once you are done, and explain why adding a
`Worker.getMetrics()` so callers can chain "one hop fewer" is *not* a fix. See
[law-of-demeter.md](./law-of-demeter.md).

### M4 — `[coding]` · Medium · A cohesive `TaskSubmissionService` (one reason to change)
Build the application-service entry point the REST `TaskController` will call (Phase 2). It must have
**exactly one responsibility**: turn a validated command into a persisted, enqueued task.

1. Define `record SubmitTaskCommand(String type, String payload, int maxAttempts, int priority)`.
2. Define the `TaskRepository` port (`save`, `findById` returning `Optional<Task>`, `pollDue(int)`).
3. Implement `TaskSubmissionService` with constructor-injected `TaskRepository`, `TaskQueue`, and a
   `java.time.Clock` (so `createdAt` is deterministic in tests).
4. `submit(cmd)` must build the `Task` (`PENDING`, `attempts = 0`, UUID id), **persist first, then
   enqueue**, and return the created `Task`.

Explain the deliberate **save-then-enqueue ordering** (durable before visible: if the process dies
between the two, a recovery sweep can re-enqueue persisted tasks; the reverse order could hand a worker a
task that was never saved). State the SRP argument for why the controller must *not* do all three steps
itself. See [domain-modeling.md](./domain-modeling.md).

---

## Part C — Refactoring (M5–M9) · the heart of the module

Each problem gives you **bad code**; produce the **improved** version (and, where noted, the
**production-quality** one). The solutions file shows all stages with reasoning and a "common wrong
approach."

### M5 — `[refactor]` · Medium · Break a god `TaskManager` into cohesive collaborators
Here is a god class. It changes for **five** unrelated reasons; every PR collides in it.

```java
class TaskManager {
    void submit(String type, String json) { /* validate + insert SQL + enqueue */ }
    void runNext()                        { /* dequeue + reflection lookup + execute */ }
    void retry(Task t)                    { /* compute backoff + re-enqueue */ }
    void recordMetric(String n)           { /* http POST to statsd */ }
    void emailOnFailure(Task t)           { /* SMTP */ }
    // 600 lines, 9 fields, touched by every feature
}
```

**Tasks.**
1. List every responsibility this class owns and name the *single reason to change* for each (aim for at
   least five).
2. **Improved version:** extract one collaborator per responsibility — `TaskSubmissionService` (M4),
   `Worker` (M3), a `RetryHandler` (decides retry-vs-DLQ), and the ports `TaskRepository`, `TaskQueue`,
   `DeadLetterQueue`, `MetricsCollector`, `TaskScheduler`, `RetryPolicy`. Wire everything by **constructor
   injection**; no `new` of infrastructure inside any service.
3. Implement `RetryHandler.onFailure(Task task, TaskResult result)` whose *single* decision is "schedule
   another attempt or dead-letter." Use the `RetryPolicy` from M2: non-retryable → DLQ; delay present →
   schedule a `RETRYING` re-enqueue with incremented attempts; delay empty → DLQ ("max attempts
   exhausted").
4. Draw a Mermaid `classDiagram` of the *after* state. Every dependency must point at an **interface**
   (dashed `..>`), not an implementation.

Acceptance: each extracted class fits on one screen; `RetryHandler` changes *only* when the retry
decision changes. Warn against the wrong split: by *layer of noun* (`TaskData`/`TaskLogic`/`TaskUtil`)
instead of by responsibility — that just renames the god class. See [solid.md](./solid.md) and
[cohesion.md](./cohesion.md).

### M6 — `[refactor]` · Medium · Replace an `instanceof`/switch-on-type chain with polymorphism
A notifier branches on task type. Adding a type means editing this method — an **open/closed** violation.

```java
String describe(Task t) {
    switch (t.type()) {
        case "email":   return "Email to " + t.payload();
        case "report":  return "Report job " + t.payload();
        case "webhook": return "Webhook " + t.payload();
        default:        return "Unknown";
    }
}
```

**Tasks.**
1. **Option A (open set):** put the behavior on the thing that has it — a `TaskKind` interface with
   `EmailKind`, `ReportKind`, … each owning its own `describe`. Adding a type is a **new class, zero
   edits** to existing code.
2. **Option B (closed set):** model the cases as a `sealed interface Notification permits ...` of records
   and dispatch with a **switch pattern match** that needs **no `default`** — the compiler forces you to
   handle every case.
3. State the decision rule: *when* do you choose A vs B? (Open, growing set ⇒ polymorphism; closed, known
   set with cross-cutting operations ⇒ sealed + switch.) Explain why "leave the switch but add
   `default: throw`" fixes nothing structurally. See [solid.md](./solid.md).

### M7 — `[refactor]` · Medium · From inheritance to composition for `Worker` variants
A multi-level inheritance chain is rigid — you cannot get "metered but not logging," and order is
hard-coded:

```java
class Worker { void process(Task t) { /* run */ } }
class LoggingWorker extends Worker  { void process(Task t){ log(t);   super.process(t);} }
class MeteredWorker extends LoggingWorker { void process(Task t){ meter(t); super.process(t);} }
```

**Tasks.**
1. Introduce a `TaskProcessor` interface (`TaskResult process(Task) throws Exception`) and a
   `CoreProcessor` that delegates to the `HandlerRegistry`.
2. Write **decorators** `MeteringProcessor` and `LoggingProcessor`, each wrapping a delegate
   `TaskProcessor`.
3. Show that you can compose **any** of the 2³ combinations/orders at the wiring site with **zero** extra
   classes (e.g. `new LoggingProcessor(new MeteringProcessor(new CoreProcessor(registry)))`).

State the rule this demonstrates ("favor composition over inheritance"; inheritance is for *is-a* with a
stable hierarchy, cross-cutting concerns are *has-a* capabilities you bolt on) and why "add boolean flags
to the constructor" is just a `switch` hiding in a constructor. See
[../02-core-oop/chapter-17-composition-vs-inheritance.md](../02-core-oop/chapter-17-composition-vs-inheritance.md).

### M8 — `[refactor]` · Medium · Move persistence out of the domain (hexagonal port)
A domain `Task` has JDBC inside it — the dependency points *outward*:

```java
class Task {
    void save(java.sql.Connection c) throws Exception {
        var ps = c.prepareStatement("INSERT INTO tasks(...) VALUES (...)");
        // domain object now drags JDBC, SQL dialect, and connection lifecycle around
    }
}
```

**Tasks.**
1. Define the `TaskRepository` **port** — it lives in and is *owned by* the domain, and mentions **no
   JDBC** (`save`, `findById` → `Optional<Task>`, `pollDue(int)`).
2. Implement a `JdbcTaskRepository` **adapter** in infrastructure that depends *inward* on the port and
   the `Task` type, takes a `javax.sql.DataSource`, and makes `save` **idempotent** with
   `INSERT ... ON CONFLICT (id) DO UPDATE` (a retry-heavy system needs this).
3. Draw a Mermaid `flowchart` showing the dependency arrow pointing **inward** (adapter → port).

Acceptance: the domain `Task` has **zero** compile dependency on `java.sql`. State the common mistake:
putting the port in the *infrastructure* package — then the domain imports infrastructure and you have a
layered design wearing a hexagonal costume. See [hexagonal-architecture.md](./hexagonal-architecture.md).

### M9 — `[refactor]` · Medium · Apply DRY correctly — extract the *real* duplicated knowledge
Three services each compute "is this task due?" slightly differently — and have already drifted into a
bug:

```java
// in scheduler:  boolean due = t.scheduledAt().isBefore(Instant.now());
// in worker:     boolean due = t.scheduledAt().compareTo(Instant.now()) <= 0;   // off-by-one vs above!
// in api:        boolean due = !t.scheduledAt().isAfter(Instant.now());
```

**Tasks.**
1. Identify *why* this is the dangerous kind of duplication (duplicated **knowledge** — the rule for
   "due" — that has silently diverged into conflicting definitions: strictly-before vs at-or-before).
2. Add **one** authoritative `Task.isDue(java.time.Clock clock)` method on the type that owns
   `scheduledAt`, defining "due" once (at-or-before now). Pass `Clock` so it stays testable.
3. Update all three call sites to `task.isDue(clock)`.

Contrast with the **wrong** "de-duplication": extracting a `DateUtils.compare(a, b)` helper removes
*textual* similarity while leaving the *semantic* decision duplicated at each call site — the bug
survives. Extract the **decision**, not the comparison primitive. State why this also satisfies
"tell, don't ask." See [dry-kiss-yagni.md](./dry-kiss-yagni.md).

---

## Part D — Design (M10–H3)

Deliver UML (Mermaid `classDiagram`/`flowchart`/`stateDiagram-v2`) and crisp prose. Implement code only
where a problem says so.

### M10 — `[design]` · Medium · The `Job → Task` aggregate with enforced invariants
Design a `Job` aggregate **root** that fans out into child `Task`s and guarantees: *"a `Job` is
`COMPLETED` only when every child task is terminal (`SUCCEEDED`/`DEAD`), and `FAILED` if any child is
`DEAD`."*

**Tasks.**
1. Implement `Job` with `JobStatus { RUNNING, COMPLETED, FAILED }`. Children live in a **private** map
   keyed by task id; the map is **never leaked** (`tasksView()` returns a copy).
2. The **only** mutator is `recordOutcome(String taskId, TaskStatus outcome)`, which updates the child
   and atomically recomputes the job status. No public setter exposes `status` or the children.
3. Draw a Mermaid `classDiagram` showing **composition** (`*--`, filled diamond — children have no
   meaning outside their job) between `Job` and `Task`, and a `note` marking `Job` as the aggregate root.

State the core property: the invariant ("Job status is a pure function of its children's states") can
*never* be violated because the only mutator recomputes it. Warn against exposing `getTasks()` returning
the live list — then the invariant lives in *every caller's discipline*. See
[aggregates.md](./aggregates.md) and [domain-modeling.md](./domain-modeling.md).

### M11 — `[design]` · Medium · Choose layered vs hexagonal for our platform, and justify it
Pick an architecture for the Task Queue platform and **defend the tradeoff** with a comparison table.

**Tasks.**
1. Build a table scoring layered vs hexagonal on at least five forces: swappable queue
   (in-mem → Postgres → Kafka), testability without DB/broker, onboarding/familiarity, risk of an anemic
   domain, and boilerplate. Mark a winner per row.
2. Argue your pick in two or three sentences grounded in *our specific change profile* — we deliberately
   swap the broker across Phase 1 → 2 → 4.
3. State the **counter-case**: when would you *not* choose your pick? (Hint: a CRUD app with one database
   that will never change — there the extra ports are pure ceremony and YAGNI says use plain layered.)

See [layered-architecture.md](./layered-architecture.md) and
[hexagonal-architecture.md](./hexagonal-architecture.md).

### M12 — `[design]` · Medium · Apply all five SOLID principles to the worker subsystem
Give **one concrete application of each SOLID letter** in our domain (not generic definitions):

- **S** — which class has exactly one reason to change, and what is it?
- **O** — how does the system stay closed to modification when a new task type ships?
- **L** — why does `Optional<Duration>` (not a `-1` sentinel) make every `RetryPolicy`
  Liskov-substitutable? Write a short `assertPolicyContract(RetryPolicy)` helper that checks the contract
  (non-negative present delay; eventually gives up).
- **I** — why does `TaskQueue` expose only `enqueue`/`dequeue`/`size` and *not* `getMetrics()`?
- **D** — which interfaces does `Worker` depend on, injected how?

Then state the **anti-pattern**: treating SOLID as five boxes to tick on *every* class. The `Task`
record has no interface and that is fine — applying ISP to a 3-method value object is cargo-culting.
See [solid.md](./solid.md).

### H1 — `[design]` · Hard · A pluggable `RateLimiter` boundary that is easy to test and swap
Design the rate-limiting seam (Phase 3) so the algorithm — token bucket now, sliding window later,
distributed Redis later still — is swappable without touching callers.

**Tasks.**
1. Define `RateLimiter` with a single non-blocking method `boolean tryAcquire()` (ISP: the API layer
   codes against exactly this).
2. Implement `TokenBucketRateLimiter` with lazy time-based refill that never exceeds `capacity` and is
   thread-safe. **Inject the clock** as a `java.util.function.LongSupplier` (nanos) so tests advance a
   fake clock and assert exact refill behavior with **no `Thread.sleep`**.
3. Write a test that advances the fake clock and asserts that the bucket grants exactly the expected
   number of permits over a window.

State why reaching for `System.nanoTime()` *inside* the limiter (instead of injecting it) makes every
rate-limit test slow and flaky, and note where a Phase-4 distributed bucket would move state (Redis + a
Lua script for atomicity). See [coupling.md](./coupling.md).

### H2 — `[design]` · Hard · Event publishing (Phase 4) without coupling producers to subscribers
Design the `EventBus` so the `Worker` that *produces* `TaskSucceeded`/`TaskFailed`/`TaskDeadLettered`
events knows nothing about the metrics, audit, and webhook subscribers that *consume* them.

**Tasks.**
1. Define a `sealed interface TaskEvent permits ...` with one record per event (carry `taskId()` and
   `at()`).
2. Define `@FunctionalInterface TaskEventListener` and the `EventBus` port (`publish`, `subscribe`).
3. Implement an `InMemoryEventBus` doing synchronous fan-out. **Isolate listener failures**: wrap each
   `onEvent` in try/catch so one bad subscriber cannot poison the publish path. Use a thread-safe
   subscriber list (`CopyOnWriteArrayList`).

Explain how this is the **observer pattern** as a coupling-breaker: a new consumer `subscribe`s without
the producer recompiling. Contrast with the wrong design (the `Worker` holding a `MetricsCollector`, an
`AuditLog`, and a `WebhookClient` and calling all three inline — a fourth consumer edits `Worker`). See
[observer.md](../05-design-patterns/observer.md).

### H3 — `[design]` · Hard · Sketch the clean/hexagonal package structure for the whole platform
Lay out the package structure so dependencies point **inward only**:
`infrastructure → application → domain`, with the domain depending on nothing.

**Tasks.**
1. Produce the package tree: `domain` (entities + the **ports it owns**), `application` (use-cases /
   orchestration: `TaskSubmissionService`, `Worker`, `RetryHandler`, `HandlerRegistry`), `infrastructure`
   (the *only* place Spring/JDBC/Kafka live — web, persistence, queue, metrics adapters).
2. Place each canonical type in the right package and justify the placements.
3. Draw a Mermaid `flowchart` showing all arrows pointing inward (adapters `-. implements .->` ports).

State the rule and the common mistake: the **ports live with the domain** (they express the domain's
*needs* in the domain's own language); putting them in `infrastructure` reverses the dependency. See
[clean-architecture.md](./clean-architecture.md) and [hexagonal-architecture.md](./hexagonal-architecture.md).

---

## Part E — Interview (H4–H7) · Hard

Treat each as a 10–20 minute design conversation. Write your answer the way you would *speak* it: state
assumptions, propose, critique, refine. Expect the pushback noted in parentheses.

### H4 — `[interview]` — "How do you decide what becomes a class vs. a method vs. a field?"
Give your decision framework grounded in **responsibilities and change**, not nouns. When does something
earn its own class (a seam/interface)? When is it just a method on an existing type? When is it a field?
Use real examples: `RetryPolicy` (class — independent reason to change), `Task.isDue(clock)` (method —
small decision over owned data), `Worker.metrics` (field — remembered state). (Pushback: *"Isn't more
classes always cleaner?"* — answer the over-classing trap.)

### H5 — `[interview]` — "Composition over inheritance — when would you still use inheritance?"
Explain why you default to composition (runtime flexibility; avoids the fragile base class problem; see
M7), and name the *three* conditions under which you would still reach for inheritance (a genuine **is-a**,
a **stable** hierarchy, and Liskov-substitutable reuse). Use `sealed interface TaskEvent permits ...` as
an example of inheritance used *well*, and explain why logging/metrics/retry are *not* inheritance cases.

### H6 — `[interview]` — "Walk me through breaking a circular dependency between two services."
`TaskSubmissionService` calls `NotificationService`, which calls back into `TaskSubmissionService` — a
cycle. Narrate how you break it: first choice, **invert one direction with an event** (publish to the
`EventBus`; the other service subscribes — H2); fallback, **extract the shared abstraction** that the
callee owns and have the caller depend on it (DIP), wired at the composition root. (Pushback: *"Why not
just mark both beans `@Lazy`?"* — explain why that hides the cycle instead of fixing the design.)

### H7 — `[interview]` — "What is an anemic domain model and why is it a smell?"
Define the **anemic domain model** (data bags + all behavior in services). Explain why it is a smell
(rules scatter across services and drift into bugs — the "is this task due?" rule of M9 reimplemented
three times; violates encapsulation and tell-don't-ask). Give the fix (move behavior next to its data:
`Task.isDue`, `Job.recordOutcome`). Add the **nuance**: anemic models are fine for genuinely CRUD-shaped,
logic-free data — the smell is real only when domain logic exists and has been exiled to services.

---

## Part F — Stretch (H8–H10) · Hard · repo-shaped

Multi-hour problems that build a real slice of the platform and *prove* the design paid off.

### H8 — `[stretch]` — Enforce the dependency rule with an automated test
Turn "domain must not depend on infrastructure" (H3) into a **failing test**, not a code-review hope.

**Tasks.**
1. Add **ArchUnit** (`com.tngtech.archunit:archunit-junit5`) and write a rule: no class in `..domain..`
   may depend on `..infrastructure..`, `org.springframework..`, or `java.sql..`.
2. Write a second `layeredArchitecture()` rule encoding inward-only access: `Domain` may access nothing;
   `Application` may access only `Domain`; `Infrastructure` may access `Application` and `Domain`.
3. Prove it fails: temporarily import `java.sql.Connection` into a `domain` class and watch the test go
   red, then revert.

Explain why this is the difference between an architecture *diagram* and an architecture you actually
*have* — and why package-private visibility plus code-review discipline is not enough across a team and a
year.

### H9 — `[stretch]` — Design for testability: the full submission flow with zero infrastructure
Prove the hexagonal seams pay off by testing `TaskSubmissionService` (M4) with **only in-memory fakes**
implementing the ports — no database, no Spring, no broker, no `Thread.sleep`.

**Tasks.**
1. Write `FakeRepo implements TaskRepository` (a `HashMap`) and `FakeQueue implements TaskQueue` (a
   `BlockingQueue`).
2. Wire `TaskSubmissionService` with the fakes and a **fixed `Clock`**
   (`Clock.fixed(Instant.parse(...), ZoneOffset.UTC)`).
3. In one JUnit 5 + AssertJ test, submit a command and assert: status is `PENDING`, `createdAt` equals
   the fixed instant (deterministic), the task is in the repo, and the queue size is 1.

State the return on investment: every DI/port decision in this module exists to make a microsecond,
deterministic test like this possible — *if you can't write it, your seams are in the wrong place.*
Contrast with the wrong instinct (`@SpringBootTest` + Testcontainers Postgres to test *submission*
logic); reserve heavyweight integration tests for the **adapters**.

### H10 — `[stretch]` — Critique and redesign a "clever" design
Here is a design that *looks* sophisticated. Find everything wrong and redesign it.

```java
// "Flexible" generic mega-bus. One method to rule them all.
class UniversalProcessor {
    Object process(String op, Map<String, Object> args) {     // stringly-typed, untyped bag
        switch (op) {
            case "submit":  return submit(args);
            case "retry":   return retry(args);
            case "metric":  return metric(args);
            default: throw new IllegalArgumentException(op);
        }
    }
    // each branch casts args.get("...") and prays
}
```

**Tasks.**
1. Write a critique naming at least five concrete problems (stringly-typed; zero cohesion / three reasons
   to change; not open/closed; ISP violation; type safety erased by `Map<String,Object>` + casts).
2. Redesign with **explicit typed commands**: a `sealed interface Command permits Submit, Retry` of
   records, and a `CommandRouter` that `switch`es over the sealed set (exhaustive, no stringly-typed
   `default`). Route `Submit` to `TaskSubmissionService`, `Retry` to `RetryHandler`.
3. Argue that *metrics is not a command* — it is a cross-cutting concern handled by the event bus (H2),
   so it leaves this class entirely, restoring cohesion.

State the meta-lesson: the fix for an untyped mega-method is **more types, not fancier ones** (a generic
`<T> T process(String, Object...)` just moves the casts). "Generic and configurable" had become "untyped
and unsafe."

---

## Self-assessment rubric

Score yourself per band. You are ready to move on when you hit **Proficient** across all bands.

| Band | Emerging | Proficient | Mastery |
| --- | --- | --- | --- |
| Knowledge (E1–E8) | Recalls definitions | Maps each principle to a project class | Argues the tradeoff both directions |
| Coding (E9–M4) | Compiles, tests pass | Dependencies injected, no `new` infra in logic | Value objects + `Optional` give-up signal, zero LoD violations |
| Refactoring (M5–M9) | God class split | Every collaborator is an interface | Polymorphic/sealed dispatch, "good DRY" decision extracted |
| Design (M10–H3) | Draws a class diagram | Correct aggregate boundary + enforced invariant | Hexagon with inward-only arrows, ports owned by domain |
| Interview (H4–H7) | States the principle | Narrates a live refactor | Defends a tradeoff under pushback |
| Stretch (H8–H10) | Runs / compiles | Core tests with zero infrastructure | ArchUnit gate green; "clever" design redesigned to typed commands |

---

## Common mistakes while doing these exercises

- **"DI" without inversion.** Passing a `new InMemoryTaskQueue()` into a constructor is progress, but if
  the *parameter type* is the concrete class you have inverted nothing. Depend on the **interface**
  `TaskQueue`.
- **Anemic refactors.** Splitting a god class into ten classes that reach into each other's getters just
  relocates the mess. Watch fan-out and the Law of Demeter as you split.
- **Aggregate too big.** Putting attempt history, schedules, and metrics all inside one `Task`/`Job`
  aggregate creates contention and giant transactions. Reference by id across aggregates.
- **Exhaustiveness theatre.** Using a `sealed` interface but writing a `default` branch defeats the
  compiler's exhaustiveness check. Drop the `default`.
- **Ports in the wrong package.** A `TaskRepository` interface living in `infrastructure` reverses the
  dependency — it is a layered design in a hexagonal costume. Ports belong with the domain.
- **Sentinels over `Optional`.** Returning `-1` for "give up retrying" leaks: one forgotten check
  schedules a `-1ms` retry and you get a hot loop. Make the give-up case a different type shape.
- **Reaching for the global clock.** `System.nanoTime()`/`Instant.now()` inside business logic makes it
  untestable without sleeping. Inject `Clock`/`LongSupplier`.
- **Hidden statics.** A `static MetricsCollector.INSTANCE` reintroduces global coupling and breaks test
  isolation. Inject it.

---

## What We Can Improve In Our Project Using This Concept

These exercises *are* the improvement backlog for the module. Worked end to end, Parts C–F convert the
early Phase-1 code from a single procedural `TaskManager` into a properly **dependency-inverted core**:
every infrastructure concern (queue, repository, metrics, DLQ, scheduler, rate limiter, event bus) sits
behind a port the domain owns, and the only place that names a concrete implementation is the composition
root. The `Job` aggregate (M10) moves consistency from caller discipline into code, and the ArchUnit gate
(H8) keeps the whole thing honest over time. That is the precondition for Phase 2 (swap in PostgreSQL via
`JdbcTaskRepository`) and Phase 4 (swap in Kafka/Redis) **without touching domain logic**.

## Project Refactoring Task

Ship these in order, each as its own mergeable commit that leaves the build green:

1. **M5** — split the god `TaskManager` into `TaskSubmissionService` + `Worker` + `RetryHandler`.
2. **M8** — move JDBC behind the `TaskRepository` port; add `JdbcTaskRepository`.
3. **H8** — add the two ArchUnit tests so `domain` can never import infrastructure again.
4. **H9** — prove the seams with the infrastructure-free `TaskSubmissionServiceTest`.

After step 4 the codebase satisfies: no class except the composition root constructs infrastructure;
`TaskSubmissionService`/`Worker` are unit-tested with fakes in microseconds; and the dependency graph is
acyclic and points inward.

## Git Commit For This Chapter

```text
refactor(oop-ood): decompose god TaskManager into cohesive services behind ports

- split TaskManager into TaskSubmissionService, Worker, RetryHandler (SRP, M5)
- introduce TaskRepository/TaskQueue/DeadLetterQueue/MetricsCollector ports owned by the domain
- move JDBC behind JdbcTaskRepository adapter; dependency points inward (M8)
- constructor-inject all collaborators; depend on abstractions only (DIP, E9)
- model Job aggregate enforcing the child-task terminal-state invariant (M10)
- add ArchUnit tests enforcing the inward-only dependency rule (H8)
- add infrastructure-free TaskSubmissionServiceTest proving the seams (H9)

Files touched:
  src/main/java/.../domain/Task.java
  src/main/java/.../domain/Job.java
  src/main/java/.../domain/port/{TaskRepository,TaskQueue,DeadLetterQueue,MetricsCollector,RetryPolicy}.java
  src/main/java/.../application/{TaskSubmissionService,Worker,RetryHandler,HandlerRegistry}.java
  src/main/java/.../infrastructure/persistence/JdbcTaskRepository.java
  src/test/java/.../ArchitectureTest.java
  src/test/java/.../TaskSubmissionServiceTest.java
  04-oop-and-ood/exercises.md
```

## Architecture Impact

Doing this set flips the dependency direction of the entire Phase-1 module: domain and application code
stop importing infrastructure, and infrastructure starts depending on domain-owned ports. This is the
single most important structural change before Phase 2 — it is what makes "add PostgreSQL" a new
**adapter** rather than a rewrite, and it is the foundation the [hexagonal](./hexagonal-architecture.md)
and [clean-architecture](./clean-architecture.md) chapters build on. Fan-out on the core classes drops,
cohesion rises, the object graph becomes acyclic, and the ArchUnit gate freezes the gain in place.

## Interview Takeaways

- "High cohesion, low coupling" is a *direction*, not an absolute — be ready to name a case where they
  trade off.
- The fastest way to spot an SRP violation is to ask each method "for *whose* reason would this change?"
- DI is about **inversion** (depend on interfaces), not merely about passing arguments.
- An aggregate boundary is a **transaction + invariant** boundary; cross-aggregate links are by id.
- Hard-to-test code is a *design smell*, not a testing problem — usually a missing seam or a hidden `new`.
- Prefer **more types over fancier ones**: model each operation/state as its own type and let `sealed` +
  exhaustive `switch` make the compiler walk you through every site that must change.
- Hexagonal/Clean architecture earns its keep through **swap-ability and testability**, but YAGNI applies:
  introduce the port when the second implementation is on the horizon, and design it so it fits cleanly.
