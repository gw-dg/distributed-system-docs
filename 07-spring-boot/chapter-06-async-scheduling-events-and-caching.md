# Async, Scheduling, Events, and Caching

> Where this fits: the Task Queue is asynchronous by nature. Earlier modules built a `WorkerPool` manually with `ExecutorService`, a delayed retry loop, and an eventual `EventBus`. This chapter shows where Spring's async execution, scheduling, events, and cache abstraction help, and where they should not replace the core queue semantics.

The warning up front: `@Async` is not a task queue. It is a method-execution convenience. A durable task queue still needs persisted task state, leasing, retry, and idempotency.

---

## Why This Exists

The project already has asynchronous work:

```text
POST /tasks -> save task -> return 202
WorkerPool -> dequeue -> handler.handle(task)
```

Spring adds useful infrastructure around that:

- `TaskExecutor` for managed thread pools.
- `@Async` for simple fire-and-forget methods.
- `@Scheduled` for recurring jobs such as lease reaping and retry polling.
- Spring events for in-process lifecycle notifications.
- Cache abstraction for repeated reads such as `GET /tasks/{id}` or handler metadata.
- Redis cache for distributed cache state.

The key is choosing the right tool for each part.

---

## Real-World Problem

The Task Queue needs these background activities:

| Activity | Tool |
| --- | --- |
| Execute tasks from the queue | Dedicated `WorkerPool` with managed executor |
| Poll due retries or scheduled tasks | `@Scheduled` job or queue polling loop |
| Publish `TaskSubmitted`, `TaskSucceeded`, `TaskFailed` | Spring events for local listeners; broker later for distributed events |
| Cache status reads | Spring Cache with bounded TTL |
| Cache handler metadata | Local cache or precomputed registry |
| Retry failed downstream calls | Retry policy plus scheduler, not raw `@Async` |

Spring helps with infrastructure, but the queue's correctness still comes from the task lifecycle model.

---

## Manual Implementation

Manual async execution:

```java
public final class WorkerPool {
    private final ExecutorService executor;
    private final int workerCount;
    private final Supplier<Worker> workerFactory;

    public WorkerPool(int workerCount, Supplier<Worker> workerFactory) {
        this.workerCount = workerCount;
        this.workerFactory = workerFactory;
        this.executor = Executors.newFixedThreadPool(workerCount);
    }

    public void start() {
        for (int i = 0; i < workerCount; i++) {
            executor.submit(workerFactory.get());
        }
    }

    public void shutdown() {
        executor.shutdownNow();
    }
}
```

Manual scheduling:

```java
ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
scheduler.scheduleAtFixedRate(
        retryPoller::moveDueRetriesBackToQueue,
        0,
        1,
        TimeUnit.SECONDS);
```

Manual events:

```java
public final class InMemoryEventBus implements EventBus {
    private final List<TaskEventListener> listeners = new CopyOnWriteArrayList<>();

    public void publish(TaskEvent event) {
        for (TaskEventListener listener : listeners) {
            listener.onEvent(event);
        }
    }
}
```

This works, but you own thread naming, lifecycle, error handling, shutdown, and instrumentation.

---

## Pain Points

- Thread pools are created in multiple places.
- Background tasks may continue after Spring starts shutting down.
- Scheduled jobs can overlap if they run longer than their interval.
- Event listeners can block the publisher.
- Caches can return stale data if invalidation is forgotten.
- `@Async` failures can disappear if return types are ignored.

Spring gives consistent infrastructure, but it does not remove these tradeoffs.

---

## Spring TaskExecutor

Configure a named executor:

```java
@Configuration
@EnableAsync
@EnableScheduling
public class AsyncConfig {

    @Bean(name = "workerTaskExecutor")
    ThreadPoolTaskExecutor workerTaskExecutor(TaskQueueProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.workers().count());
        executor.setMaxPoolSize(properties.workers().count());
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("task-worker-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }
}
```

Use it inside the worker pool:

```java
public final class WorkerPool {
    private final TaskExecutor executor;
    private final int workerCount;
    private final Supplier<Worker> workerFactory;

    public void start() {
        for (int i = 0; i < workerCount; i++) {
            executor.execute(workerFactory.get());
        }
    }
}
```

This keeps thread pool configuration in Spring while preserving the project's queue semantics.

---

## `@Async`

`@Async` runs a Spring bean method on an executor:

```java
@Service
public class NotificationService {
    @Async("workerTaskExecutor")
    public CompletableFuture<Void> sendAcceptedNotification(String taskId) {
        // non-critical side effect
        return CompletableFuture.completedFuture(null);
    }
}
```

Use `@Async` for small independent side effects. Do not use it as the main task queue implementation:

```java
// Do not replace durable task submission with this.
@Async
public void process(Task task) {
    handler.handle(task);
}
```

That loses durability, leasing, retry state, and idempotency.

Internally, `@Async` is also proxy-based. The call must go through the Spring proxy; self-invocation will not be asynchronous.

---

## Scheduling

Use scheduling for recurring infrastructure jobs:

```java
@Component
public final class RetryScheduler {
    private final RetryService retryService;

    public RetryScheduler(RetryService retryService) {
        this.retryService = retryService;
    }

    @Scheduled(fixedDelayString = "${taskqueue.retry.poll-interval:1s}")
    public void moveDueRetries() {
        retryService.moveDueRetriesBackToPending();
    }
}
```

Use `fixedDelay` when a new run should wait until the previous run finishes. Use `fixedRate` only when overlap is harmless or prevented. In a multi-node deployment, a scheduled job may run on every node, so singleton jobs need a distributed lock or leader election.

---

## Spring Events

Manual event publishing becomes:

```java
public record TaskSubmittedEvent(String taskId, String type, Instant occurredAt) {}
public record TaskSucceededEvent(String taskId, String type, Instant occurredAt) {}
public record TaskFailedEvent(String taskId, String type, String reason, Instant occurredAt) {}
```

Publish events:

```java
@Service
public final class TaskSubmissionService {
    private final ApplicationEventPublisher events;

    public Task submit(SubmitTaskRequest request) {
        Task task = createAndSave(request);
        events.publishEvent(new TaskSubmittedEvent(task.id(), task.type(), Instant.now()));
        return task;
    }
}
```

Listen to events:

```java
@Component
public final class TaskMetricsListener {
    private final TaskMetrics metrics;

    @EventListener
    public void on(TaskSubmittedEvent event) {
        metrics.recordSubmitted(event.type());
    }
}
```

By default, Spring events are synchronous. If a listener is slow, the publisher is slow. Add async listeners only when you accept eventual execution and handle failures.

```java
@Async("eventTaskExecutor")
@EventListener
public void on(TaskSucceededEvent event) {
    auditLog.write(event);
}
```

Spring events are in-process. Phase 4's distributed `EventBus` still needs a broker.

---

## Cache Abstraction

Cache repeated reads:

```java
@Service
public final class TaskQueryService {
    private final TaskRepository repository;

    @Cacheable(cacheNames = "tasks", key = "#id")
    public Optional<Task> findById(String id) {
        return repository.findById(id);
    }

    @CacheEvict(cacheNames = "tasks", key = "#task.id()")
    public void evict(Task task) {
    }
}
```

This can reduce database reads for hot status endpoints, but task status changes frequently. Cache only with a small TTL, and evict on status transitions when practical.

Configure Redis cache for shared production cache:

```yaml
spring:
  cache:
    type: redis
  data:
    redis:
      host: redis
      port: 6379
```

Local cache is per JVM. Redis cache is shared across nodes. Both can be stale.

---

## How Spring Works Internally

| Feature | Internal mechanism |
| --- | --- |
| `@Async` | Proxy intercepts method call and submits to an executor |
| `@Scheduled` | Scheduled annotation processor registers tasks with a scheduler |
| `@EventListener` | Listener methods are registered with the application event multicaster |
| `@Cacheable` | Proxy checks cache before invoking method |
| `@CacheEvict` | Proxy removes cache entries around method execution |

Because many features are proxy-based, calls inside the same class can bypass them. Keep async, transactional, and cached methods on Spring-managed collaborators called from other beans.

---

## Project Integration

Recommended integration:

- Keep `WorkerPool` as the core queue executor.
- Back it with a Spring-managed `TaskExecutor`.
- Use `@Scheduled` for retry scanning, lease reaping, and metrics refresh.
- Use Spring events for in-process metrics and audit listeners.
- Use broker-backed events in Phase 4 for cross-process subscribers.
- Use caching only for read paths and metadata, not as the source of truth.

---

## Tradeoffs

| Feature | Good for | Not good for |
| --- | --- | --- |
| `@Async` | Small independent side effects | Durable task execution |
| `TaskExecutor` | Managed thread pools | Replacing queue state |
| `@Scheduled` | Periodic jobs | Singleton distributed scheduling without locks |
| Spring events | In-process decoupling | Reliable distributed events |
| Cache abstraction | Hot reads, metadata | Strongly consistent status |
| Redis cache | Shared cache across nodes | Primary task storage |

---

## Production Considerations

- Name every executor and monitor queue depth, active threads, and rejected tasks.
- Use bounded executors. Unbounded queues hide overload until memory fails.
- Make scheduled jobs idempotent.
- Use distributed locks for singleton jobs in multi-node deployments.
- Decide whether event listeners are synchronous or asynchronous deliberately.
- Set cache TTLs; never cache task status forever.
- Treat Redis cache outages as degraded reads, not data loss.

---

## Exercises

### Easy

1. Explain why `@Async` is not a durable task queue.
2. Add a named `ThreadPoolTaskExecutor`.
3. Write a `TaskSubmittedEvent` record.

### Medium

4. Refactor `WorkerPool` to use a Spring-managed `TaskExecutor`.
5. Add a scheduled retry poller using `@Scheduled`.
6. Add an event listener that records metrics on `TaskSucceededEvent`.

### Hard

7. Add Redis-backed caching for `GET /tasks/{id}` with a short TTL and eviction on status update.
8. Make the scheduled retry job safe in a two-node deployment by adding a lock boundary.

---

## Solutions

### S1-S3

`@Async` only moves a method call to another thread. If the process crashes, the method call is gone. A durable queue writes task state before returning to the client.

```java
public record TaskSubmittedEvent(String taskId, String type, Instant occurredAt) {}
```

### S4-S6

Inject `TaskExecutor` into `WorkerPool` and call `execute`. Use `@Scheduled(fixedDelayString = "...")` for retry polling. Publish events from services and handle metrics in listeners.

```java
@EventListener
void on(TaskSucceededEvent event) {
    metrics.recordTerminal(event.type(), "SUCCEEDED");
}
```

### S7-S8

Use `@Cacheable` for read-through caching and `@CacheEvict` after status changes. For two-node scheduling, protect the job with a database advisory lock, Redis lock with fencing, or a leader election mechanism. The job itself must remain idempotent.

---

## Project Refactoring Task

1. Configure worker and event executors.
2. Refactor `WorkerPool` to use `TaskExecutor`.
3. Add scheduled retry or lease reaper jobs.
4. Publish task lifecycle events.
5. Add a cache only to the query side.

---

## Git Commit For This Chapter

```bash
git add src/main/java src/main/resources src/test/java
git commit -m "feat: manage worker async scheduling events and cache"
```

---

## Architecture Impact

Spring now manages execution infrastructure:

```text
WorkerPool -> TaskExecutor
Scheduler -> retry and lease maintenance
Application services -> Spring events -> metrics/audit listeners
Query service -> cache -> repository
```

This improves lifecycle and observability without replacing the queue as the source of truth.

---

## Interview Questions

1. Why is `@Async` not a replacement for a queue?
2. What is `TaskExecutor`?
3. What is the difference between `fixedRate` and `fixedDelay`?
4. Are Spring events synchronous?
5. How does `@Cacheable` work?
6. What breaks when a proxy-based annotation is invoked through `this`?
7. How do you make scheduled jobs safe across multiple nodes?
8. What metrics should you expose for executors?

## Interview Takeaways

Spring's async tools manage threads and lifecycle. The task queue's correctness still comes from durable state, leasing, idempotency, and retry semantics.
