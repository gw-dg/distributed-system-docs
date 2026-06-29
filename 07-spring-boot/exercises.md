# Spring Boot: Exercises

> Where this fits: this is the module-wide problem set for `07-spring-boot/`. Every exercise uses the Distributed Task Queue project. The point is to prove you can introduce Spring Boot as an evolution of the existing Java design, not as a separate annotation tutorial. Solutions live in [./solutions.md](./solutions.md).

These exercises assume you have read:

- [chapter-01-why-spring-and-ioc.md](./chapter-01-why-spring-and-ioc.md)
- [chapter-02-rest-api-and-request-lifecycle.md](./chapter-02-rest-api-and-request-lifecycle.md)
- [chapter-03-configuration-and-dependency-injection.md](./chapter-03-configuration-and-dependency-injection.md)
- [chapter-04-persistence-jdbc-jpa-and-transactions.md](./chapter-04-persistence-jdbc-jpa-and-transactions.md)
- [chapter-05-validation-exception-handling-and-testing.md](./chapter-05-validation-exception-handling-and-testing.md)
- [chapter-06-async-scheduling-events-and-caching.md](./chapter-06-async-scheduling-events-and-caching.md)
- [chapter-07-observability-security-and-production.md](./chapter-07-observability-security-and-production.md)

They also rely on the earlier modules: [dependency injection](../04-oop-and-ood/dependency-injection.md), [layered architecture](../04-oop-and-ood/layered-architecture.md), [strategy](../05-design-patterns/strategy.md), [executor service](../06-concurrency/executor-service.md), [task queues](../07-queues-and-messaging/task-queues.md), and [idempotency](../08-distributed-systems/idempotency.md).

---

## How To Use This File

- Exercise IDs are stable. Solutions reference the same IDs.
- `K` = knowledge check, `C` = coding, `R` = refactoring, `D` = design, `I` = interview, `S` = stretch.
- Do not start with Spring annotations. For every coding exercise, write down the manual Java version or pain point first.
- Keep the canonical model names: `Task`, `TaskStatus`, `TaskQueue`, `TaskRepository`, `TaskHandler`, `RetryPolicy`, `DeadLetterQueue`, `Worker`, `WorkerPool`.

### Shared Model Slice

```java
enum TaskStatus { PENDING, SCHEDULED, RUNNING, SUCCEEDED, FAILED, RETRYING, DEAD }

record Task(String id, String type, String payload, TaskStatus status,
            int attempts, int maxAttempts,
            java.time.Instant createdAt,
            java.time.Instant scheduledAt,
            int priority) {}

record TaskResult(boolean success, String message, boolean retryable) {}

interface TaskQueue {
    void enqueue(Task task);
    Task dequeue() throws InterruptedException;
    int size();
}

interface TaskRepository {
    void save(Task task);
    java.util.Optional<Task> findById(String id);
    java.util.List<Task> pollDue(int limit);
}

interface RetryPolicy {
    java.util.Optional<java.time.Duration> nextDelay(int attempt);
}

interface DeadLetterQueue {
    void send(Task task, String reason);
}
```

---

## Part 1 - Knowledge Checks

### K1 - IoC vs DI (Easy)

Explain the difference between Inversion of Control and Dependency Injection using `Worker` and `TaskQueue`.

### K2 - Constructor Injection (Easy)

Why is constructor injection better than field injection for `TaskSubmissionService`?

### K3 - Request Lifecycle (Easy)

List the major steps a `POST /tasks` request takes from `DispatcherServlet` to `TaskRepository`.

### K4 - HTTP Semantics (Easy)

Should task submission return `200`, `201`, or `202`? Defend your answer for an asynchronous queue.

### K5 - Configuration (Medium)

Name six Task Queue settings that should be externalized. Which ones are safe in `application.yml`, and which should come from environment variables or secrets?

### K6 - Transactions (Medium)

Why should the worker not execute `TaskHandler.handle(task)` inside the same transaction that leases the task row?

### K7 - JPA Tradeoff (Medium)

For `pollDue(limit)`, would you prefer JPA or SQL through `JdbcTemplate`? Explain with locking and query shape.

### K8 - Validation Boundary (Medium)

What belongs in Bean Validation annotations, and what belongs in service/domain validation?

### K9 - `@Async` Misuse (Hard)

Why is replacing `TaskQueue` with `@Async process(Task)` incorrect for this project?

### K10 - Observability (Hard)

Why is "oldest pending task age" often a better alert than queue depth? What labels would you allow on task execution metrics?

---

## Part 2 - Coding Exercises

### C1 - Spring Bean Graph (Easy)

Create a minimal Spring Boot application that wires:

- `TaskQueue`
- `RetryPolicy`
- `DeadLetterQueue`
- two `TaskHandler` beans
- `HandlerRegistry`
- `Worker`

Requirements:

- Use constructor injection.
- Keep `Task` and `TaskResult` annotation-free.
- Build the handler map from `List<TaskHandler>`.

### C2 - REST API (Medium)

Implement:

```text
POST /tasks
GET /tasks/{id}
```

Requirements:

- Use DTOs, not domain objects, as request and response contracts.
- Return `202 Accepted` and `Location: /tasks/{id}` on submission.
- Return `404` for missing tasks.
- Keep controller logic thin.

### C3 - Typed Configuration (Medium)

Create `TaskQueueProperties` bound from:

```yaml
taskqueue:
  queue:
    type: postgres
    capacity: 50000
  workers:
    count: 8
  retry:
    max-attempts: 5
    base-delay: 1s
    max-delay: 5m
```

Use those properties to configure `RetryPolicy` and `WorkerPool`.

### C4 - Profile or Property-Based Queue Selection (Medium)

Wire `InMemoryTaskQueue` for local and `PostgresTaskQueue` for production.

Requirements:

- Exactly one `TaskQueue` bean should exist at runtime.
- The rest of the application must not know which implementation is active.

### C5 - JDBC Repository (Hard)

Implement `JdbcTaskRepository` with:

- `save(Task)`
- `findById(String)`
- `pollDue(int)`

Requirements:

- Use `JdbcTemplate`.
- Use `FOR UPDATE SKIP LOCKED` in `pollDue`.
- Keep `pollDue` transactional.
- Include a row mapper.

### C6 - Validation and Error Responses (Medium)

Add validation to `SubmitTaskRequest` and a global exception handler.

Requirements:

- Invalid input returns `400`.
- Full queue returns `429`.
- Unknown task returns `404`.
- Error body has stable fields: `code`, `message`, `fields`, `timestamp`, `path`.

### C7 - Test Coverage (Hard)

Write:

- Unit test for `TaskSubmissionService`.
- `@WebMvcTest` for `TaskController`.
- Testcontainers integration test for `JdbcTaskRepository`.
- Concurrency test proving a task is not double-leased.

### C8 - Worker Executor and Scheduling (Medium)

Configure a named `ThreadPoolTaskExecutor`, use it in `WorkerPool`, and add a scheduled retry poller.

Requirements:

- Executor thread names start with `task-worker-`.
- Worker count comes from configuration.
- Scheduled job uses `fixedDelay`.

### C9 - Events and Metrics (Medium)

Publish `TaskSubmittedEvent`, `TaskSucceededEvent`, and `TaskFailedEvent`. Add listeners that update Micrometer metrics.

Requirements:

- Keep metric labels bounded.
- Do not put `taskId` in metric labels.
- Include `taskId` in logs instead.

### C10 - Production Shell (Hard)

Add:

- Actuator health and Prometheus endpoints.
- Graceful shutdown.
- Dockerfile.
- Docker Compose with API and Postgres.
- JWT resource server security for `/tasks`.

---

## Part 3 - Refactoring Tasks

### R1 - Manual DI To Spring Beans

Take the Phase 1 manual `AppBootstrap` and replace it with Spring-managed beans. Preserve constructor injection and interfaces.

### R2 - Controller Thinness

Move task creation, defaulting, and persistence out of `TaskController` into `TaskSubmissionService`.

### R3 - Repository Port

If any service depends on `JdbcTemplate` directly, refactor it to depend on `TaskRepository`.

### R4 - Worker Lifecycle

If workers start in constructors or `main`, refactor them into a lifecycle-managed component.

### R5 - Configuration Cleanup

Find hard-coded worker count, queue capacity, retry delays, and DB URLs. Move them into typed configuration.

---

## Part 4 - Design Exercises

### D1 - Profiles vs Conditional Properties

Decide whether queue selection should use profiles or `@ConditionalOnProperty`. Write the reasoning for local, test, staging, and production.

### D2 - JPA Boundary

Design which Task Queue persistence operations use JDBC and which can use JPA. Explain based on query shape, locking, and aggregate complexity.

### D3 - Event Boundary

Decide which lifecycle events can be Spring in-process events and which must become brokered events in Phase 4.

### D4 - Metrics Cardinality Budget

Define all metric names, labels, and allowed label values for the Task Queue.

### D5 - Production Failure Runbook

Write a runbook for: "DLQ rate spiked after deploy."

---

## Part 5 - Interview Drills

### I1

Explain Spring Boot auto-configuration without saying "magic."

### I2

Walk through `POST /tasks` from socket to database insert.

### I3

Explain why `@Transactional` is proxy-based and what self-invocation breaks.

### I4

Defend `JdbcTemplate` over JPA for the leasing query.

### I5

Explain how you would test `FOR UPDATE SKIP LOCKED`.

### I6

Explain why `@Async` does not give durable async processing.

### I7

Explain liveness vs readiness for this service.

### I8

Explain how you would secure `POST /tasks` with JWT scopes.

---

## Part 6 - Stretch

### S1 - Idempotency Key

Add `Idempotency-Key` support for `POST /tasks` using a unique database constraint.

### S2 - Redis Cache

Cache `GET /tasks/{id}` with Redis for five seconds and evict on status updates.

### S3 - Multi-Node Scheduler Safety

Make the scheduled retry poller safe when two application instances are running.

### S4 - OpenAPI Contract

Generate an OpenAPI document for the Task API and verify request/response examples.

### S5 - Production Dashboard

Design a Grafana dashboard for queue health: submit rate, execution latency, retry rate, DLQ rate, queue depth, oldest pending age, worker utilization, DB pool usage.
