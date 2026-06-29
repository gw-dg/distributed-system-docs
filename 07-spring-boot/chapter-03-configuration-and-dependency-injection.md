# Configuration and Dependency Injection

> Where this fits: the same Task Queue must run locally with an in-memory queue, in tests with controlled fakes or Testcontainers, and in production with PostgreSQL, tuned worker pools, retry limits, and queue capacity. This chapter moves those choices out of code and into externalized Spring Boot configuration.

Spring configuration is not "where annotations live." It is the boundary between code that should be stable and operational values that must vary by environment.

---

## Why This Exists

Hard-coded configuration is harmless in Phase 1:

```java
TaskQueue queue = new InMemoryTaskQueue(10_000);
WorkerPool pool = new WorkerPool(4, workerFactory);
RetryPolicy retry = new ExponentialBackoffRetryPolicy(
        Duration.ofSeconds(1), Duration.ofMinutes(1), 5, true);
```

It becomes a problem the moment the service runs in more than one environment:

- Local development wants small queues and fast retries.
- Tests want deterministic clocks and tiny delays.
- Production wants larger pools, real database credentials, and conservative retry caps.
- Emergency operations may need to lower worker count or disable a handler without a rebuild.

The code should say what configuration exists. The environment should provide the values.

---

## Real-World Problem

The Task Queue has operational knobs:

| Setting | Local | Production |
| --- | --- | --- |
| `taskqueue.queue.type` | `memory` | `postgres` |
| `taskqueue.queue.capacity` | `1000` | `50000` |
| `taskqueue.workers.count` | `2` | CPU and downstream dependent |
| `taskqueue.retry.max-attempts` | `2` | `5` |
| `taskqueue.retry.base-delay` | `100ms` | `1s` |
| `taskqueue.retry.max-delay` | `5s` | `5m` |
| `spring.datasource.url` | local container | managed database |

None of these should be compiled into `Worker`, `PostgresTaskQueue`, or `RetryPolicy`.

---

## Manual Implementation

A manual Java application usually starts by reading environment variables:

```java
public final class ManualConfig {
    public int workerCount() {
        return Integer.parseInt(envOrDefault("TASKQUEUE_WORKERS_COUNT", "4"));
    }

    public int queueCapacity() {
        return Integer.parseInt(envOrDefault("TASKQUEUE_QUEUE_CAPACITY", "10000"));
    }

    public Duration retryBaseDelay() {
        return Duration.parse(envOrDefault("TASKQUEUE_RETRY_BASE_DELAY", "PT1S"));
    }

    private String envOrDefault(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
```

Then the composition root consumes it:

```java
ManualConfig config = new ManualConfig();
TaskQueue queue = new InMemoryTaskQueue(config.queueCapacity());
WorkerPool pool = new WorkerPool(config.workerCount(), workerFactory);
RetryPolicy retry = new ExponentialBackoffRetryPolicy(
        config.retryBaseDelay(), Duration.ofMinutes(1), 5, true);
```

This is honest but limited.

---

## Pain Points

Manual configuration creates familiar bugs:

- String parsing is repeated and inconsistent.
- Missing or invalid values fail late.
- Naming conventions drift between code, docs, and deployment files.
- Environment-specific branching grows in Java.
- Tests must manipulate process environment or duplicate constructors.
- Secrets can accidentally be committed into config files.

Spring Boot's configuration model solves this with property sources, profiles, type binding, and validation.

---

## Spring Boot Implementation

Start with `application.yml`:

```yaml
taskqueue:
  queue:
    type: memory
    capacity: 10000
    poll-batch-size: 25
  workers:
    count: 4
    shutdown-timeout: 30s
  retry:
    max-attempts: 5
    base-delay: 1s
    max-delay: 5m
    jitter: true
```

Bind it to a typed properties class:

```java
@ConfigurationProperties(prefix = "taskqueue")
public record TaskQueueProperties(
        Queue queue,
        Workers workers,
        Retry retry) {

    public record Queue(String type, int capacity, int pollBatchSize) {}

    public record Workers(int count, Duration shutdownTimeout) {}

    public record Retry(int maxAttempts,
                        Duration baseDelay,
                        Duration maxDelay,
                        boolean jitter) {}
}
```

Enable the properties:

```java
@SpringBootApplication
@ConfigurationPropertiesScan
public class TaskQueueApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaskQueueApplication.class, args);
    }
}
```

Use them in bean configuration:

```java
@Configuration
public class QueueConfig {

    @Bean
    RetryPolicy retryPolicy(TaskQueueProperties properties) {
        TaskQueueProperties.Retry retry = properties.retry();
        return new ExponentialBackoffRetryPolicy(
                retry.baseDelay(),
                retry.maxDelay(),
                retry.maxAttempts(),
                retry.jitter());
    }

    @Bean
    WorkerPool workerPool(TaskQueueProperties properties, Supplier<Worker> workers) {
        return new WorkerPool(properties.workers().count(), workers);
    }
}
```

---

## Profiles

Profiles select environment-specific beans and property files.

`application-local.yml`:

```yaml
taskqueue:
  queue:
    type: memory
    capacity: 1000
  workers:
    count: 2
  retry:
    base-delay: 100ms
    max-delay: 2s
```

`application-prod.yml`:

```yaml
taskqueue:
  queue:
    type: postgres
    capacity: 50000
  workers:
    count: 16
  retry:
    base-delay: 1s
    max-delay: 5m
```

Beans can be profile-specific:

```java
@Bean
@Profile("local")
TaskQueue inMemoryTaskQueue(TaskQueueProperties properties) {
    return new InMemoryTaskQueue(properties.queue().capacity());
}

@Bean
@Profile("prod")
TaskQueue postgresTaskQueue(TaskRepository repository) {
    return new PostgresTaskQueue(repository);
}
```

Prefer property conditions when the choice is a product configuration, and profiles when the whole environment changes:

```java
@Bean
@ConditionalOnProperty(name = "taskqueue.queue.type", havingValue = "postgres")
TaskQueue postgresTaskQueue(TaskRepository repository) {
    return new PostgresTaskQueue(repository);
}
```

---

## How Spring Works Internally

Spring Boot loads configuration from ordered property sources:

1. Defaults in code.
2. `application.yml`.
3. Profile files such as `application-prod.yml`.
4. Environment variables.
5. JVM system properties.
6. Command-line arguments.

Later sources override earlier ones. That means production can override a safe default without rebuilding:

```bash
TASKQUEUE_WORKERS_COUNT=8 java -jar taskqueue.jar
```

Spring maps environment variable names to property names:

```text
TASKQUEUE_WORKERS_COUNT -> taskqueue.workers.count
SPRING_DATASOURCE_URL   -> spring.datasource.url
```

For `@ConfigurationProperties`, Boot uses type conversion to bind strings into `int`, `Duration`, `DataSize`, enums, and other structured values.

---

## `@Configuration` and `@Bean`

Use `@Component` for classes that are naturally part of the application, such as services and handlers. Use `@Bean` when construction needs decisions:

- Choosing an implementation behind an interface.
- Creating third-party classes.
- Supplying constructor arguments from properties.
- Centralizing infrastructure configuration.

```java
@Configuration
public class TimeConfig {
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
```

Inject `Clock` into services instead of calling `Instant.now()` directly. This makes tests deterministic.

---

## Project Integration

Configure the Task Queue by environment:

```text
local profile:
  TaskQueue -> InMemoryTaskQueue
  DataSource -> optional
  retry delays -> short

prod profile:
  TaskQueue -> PostgresTaskQueue
  DataSource -> HikariCP
  retry delays -> production values
```

Keep service code unchanged:

```java
public final class Worker {
    public Worker(TaskQueue queue, RetryPolicy retryPolicy, DeadLetterQueue dlq) {
        ...
    }
}
```

The object does not know which profile is active. That is the point.

---

## Tradeoffs

| Technique | Use when | Cost |
| --- | --- | --- |
| `application.yml` | Defaults and non-secret config | Can grow large |
| Profiles | Environment-wide differences | Profile explosion if overused |
| Environment variables | Deployment overrides and secrets references | Poor structure, strings only |
| `@ConfigurationProperties` | Many related settings | Requires a properties type |
| `@Value` | One-off simple values | Scatters config keys through code |
| `@Bean` | Explicit construction choices | Config classes can become a second application |

Prefer `@ConfigurationProperties` over many `@Value` fields for project settings.

---

## Production Considerations

- Do not commit real secrets. Use environment variables, secret managers, or platform-injected files.
- Validate configuration at startup so bad deploys fail before accepting traffic.
- Keep safe defaults for local development, not for production capacity.
- Document every property in the README or deployment docs.
- Make queue type a config choice only when both implementations satisfy the same port.
- Avoid mixing profile checks inside business services.

---

## Exercises

### Easy

1. List five Task Queue settings that should not be hard-coded.
2. Explain why `Clock` should be a bean.
3. Convert `TASKQUEUE_WORKERS_COUNT` into its Spring property name.

### Medium

4. Create `TaskQueueProperties` with nested `queue`, `workers`, and `retry` records.
5. Add `application-local.yml` and `application-prod.yml`.
6. Wire `RetryPolicy` from `TaskQueueProperties`.

### Hard

7. Use `@ConditionalOnProperty` to choose between `InMemoryTaskQueue` and `PostgresTaskQueue`.
8. Add validation so `workers.count > 0`, `retry.maxAttempts > 0`, and `retry.maxDelay >= retry.baseDelay`.

---

## Solutions

### S1-S3

Worker count, queue capacity, retry attempts, retry delay, database URL, profile, cache TTL, and shutdown timeout belong in configuration. `Clock` is a bean because time is an external dependency; tests can inject a fixed clock.

Environment mapping:

```text
TASKQUEUE_WORKERS_COUNT -> taskqueue.workers.count
```

### S4-S6

```java
@ConfigurationProperties(prefix = "taskqueue")
public record TaskQueueProperties(Queue queue, Workers workers, Retry retry) {
    public record Queue(String type, int capacity) {}
    public record Workers(int count) {}
    public record Retry(int maxAttempts, Duration baseDelay, Duration maxDelay) {}
}
```

```java
@Bean
RetryPolicy retryPolicy(TaskQueueProperties properties) {
    var retry = properties.retry();
    return new ExponentialBackoffRetryPolicy(
            retry.baseDelay(), retry.maxDelay(), retry.maxAttempts(), true);
}
```

### S7-S8

```java
@Bean
@ConditionalOnProperty(name = "taskqueue.queue.type", havingValue = "memory")
TaskQueue inMemoryTaskQueue(TaskQueueProperties properties) {
    return new InMemoryTaskQueue(properties.queue().capacity());
}

@Bean
@ConditionalOnProperty(name = "taskqueue.queue.type", havingValue = "postgres")
TaskQueue postgresTaskQueue(TaskRepository repository) {
    return new PostgresTaskQueue(repository);
}
```

For validation, annotate properties with `@Validated` and use Jakarta Validation constraints on fields. Records can use compact constructors for cross-field checks when needed.

---

## Project Refactoring Task

1. Add `application.yml`, `application-local.yml`, and `application-prod.yml`.
2. Add `TaskQueueProperties`.
3. Replace hard-coded worker count, queue capacity, retry values, and shutdown timeout.
4. Add conditional queue beans.
5. Add tests for property binding.

---

## Git Commit For This Chapter

```bash
git add src/main/resources src/main/java src/test/java
git commit -m "feat: externalize task queue configuration"
```

---

## Architecture Impact

Configuration becomes an adapter boundary:

```text
Environment -> Spring property binding -> beans -> stable services
```

The service code stops changing when deployment environments change. That is the practical payoff of externalized configuration.

---

## Interview Questions

1. What is externalized configuration?
2. How do Spring profiles work?
3. Why prefer `@ConfigurationProperties` over many `@Value` fields?
4. How are environment variables mapped to property names?
5. When would you use `@Bean` instead of `@Component`?
6. How do you choose between profiles and conditional properties?
7. How would you validate configuration at startup?
8. Why should secrets not live in `application.yml`?

## Interview Takeaways

Configuration is part of architecture. Good Spring code keeps operational variability out of business classes and makes environment differences visible, typed, and testable.
