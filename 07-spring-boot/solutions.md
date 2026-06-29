# Spring Boot: Solutions

> Where this fits: this is the answer key for [exercises.md](./exercises.md). The answers are intentionally project-specific. A correct Spring Boot answer for this curriculum always starts from the Task Queue problem, names the manual pain point, and then introduces Spring as the refactor.

---

## Knowledge Checks

### K1 - IoC vs DI

Inversion of Control means `Worker` no longer controls which `TaskQueue` implementation it uses. Dependency Injection is the technique: Spring passes a `TaskQueue` into the `Worker` constructor. IoC is the design principle; DI is one implementation of that principle.

### K2 - Constructor Injection

Constructor injection makes required dependencies explicit, allows `final` fields, supports direct unit tests without Spring, and prevents half-constructed services. Field injection hides required collaborators and makes tests rely on reflection or a Spring context.

### K3 - Request Lifecycle

`POST /tasks` enters `DispatcherServlet`, which asks `HandlerMapping` for the matching controller method. A `HandlerAdapter` invokes the method, `HttpMessageConverter` converts JSON into `SubmitTaskRequest`, validation runs, `TaskController` calls `TaskSubmissionService`, the service saves through `TaskRepository`, and the controller returns `ResponseEntity` with status, headers, and body.

### K4 - HTTP Semantics

Use `202 Accepted` for an asynchronous task queue because the server accepted work but has not completed it. Include `Location: /tasks/{id}` so the client can poll status. `201 Created` is defensible if the task resource is created immediately, but `202` better communicates that execution is pending.

### K5 - Configuration

Externalize queue type, queue capacity, poll batch size, worker count, retry attempts, retry delays, datasource URL, cache TTL, and shutdown timeout. Defaults and non-secret operational values can live in `application.yml`. Secrets, passwords, tokens, and environment-specific endpoints should come from environment variables, secret managers, or platform-provided config.

### K6 - Transactions

Leasing should be short: claim the row, mark it `RUNNING`, commit. Handler execution may call SMTP, S3, HTTP APIs, or slow code. Holding a database transaction and row lock during that work reduces throughput, increases lock waits, and makes crash behavior worse.

### K7 - JPA Tradeoff

Prefer SQL through `JdbcTemplate` for `pollDue(limit)` because the query shape and locking semantics are the core correctness mechanism: `FOR UPDATE SKIP LOCKED`, ordering, limits, and status updates. JPA is useful for simpler CRUD, but queue polling should be explicit SQL.

### K8 - Validation Boundary

Bean Validation should handle request shape: required fields, string format, numeric bounds, and simple local constraints. Service/domain validation should handle use-case rules: handler availability, idempotency conflicts, account permissions, queue capacity, and state transitions.

### K9 - `@Async` Misuse

`@Async` moves a method call to another thread. It does not persist work, provide leasing, recover after crash, track attempts, schedule retries, or send exhausted tasks to the DLQ. A durable task queue needs stored task state and worker coordination.

### K10 - Observability

Queue depth says how many tasks are waiting; oldest pending age says whether any task is violating latency expectations. A small queue with one task waiting for an hour can be worse than a large queue draining quickly. Safe metric labels include bounded values such as `type`, `status`, and `outcome`. Do not use `taskId`, `userId`, payload, or raw error message as labels.

---

## Coding Solutions

### C1 - Spring Bean Graph

```java
@SpringBootApplication
@ConfigurationPropertiesScan
public class TaskQueueApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaskQueueApplication.class, args);
    }
}
```

```java
@Configuration
public class QueueConfig {
    @Bean
    TaskQueue taskQueue(TaskQueueProperties properties) {
        return new InMemoryTaskQueue(properties.queue().capacity());
    }

    @Bean
    RetryPolicy retryPolicy(TaskQueueProperties properties) {
        var retry = properties.retry();
        return new ExponentialBackoffRetryPolicy(
                retry.baseDelay(), retry.maxDelay(), retry.maxAttempts(), true);
    }

    @Bean
    DeadLetterQueue deadLetterQueue() {
        return new LoggingDeadLetterQueue();
    }
}
```

```java
@Component
public final class HandlerRegistry {
    private final Map<String, TaskHandler> handlers;

    public HandlerRegistry(List<TaskHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toUnmodifiableMap(TaskHandler::supportedType, h -> h));
    }

    public Optional<TaskHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }
}
```

The important part is that `Worker` still uses constructor injection and domain types remain annotation-free.

### C2 - REST API

```java
public record SubmitTaskRequest(
        @NotBlank String type,
        @NotNull JsonNode payload,
        @Min(0) @Max(100) Integer priority,
        @Min(1) @Max(20) Integer maxAttempts,
        Instant scheduledAt) {}

public record TaskResponse(String id, String type, TaskStatus status) {
    static TaskResponse from(Task task) {
        return new TaskResponse(task.id(), task.type(), task.status());
    }
}
```

```java
@RestController
@RequestMapping("/tasks")
public final class TaskController {
    private final TaskSubmissionService submissionService;
    private final TaskQueryService queryService;

    @PostMapping
    ResponseEntity<TaskResponse> submit(@Valid @RequestBody SubmitTaskRequest request) {
        Task task = submissionService.submit(request);
        return ResponseEntity.accepted()
                .location(URI.create("/tasks/" + task.id()))
                .body(TaskResponse.from(task));
    }

    @GetMapping("/{id}")
    ResponseEntity<TaskResponse> get(@PathVariable String id) {
        return queryService.findById(id)
                .map(TaskResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
```

### C3 - Typed Configuration

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
WorkerPool workerPool(TaskQueueProperties properties, Supplier<Worker> workerFactory) {
    return new WorkerPool(properties.workers().count(), workerFactory);
}
```

### C4 - Queue Selection

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

Only one property value should be active. The rest of the application receives `TaskQueue`.

### C5 - JDBC Repository

```java
@Repository
public final class JdbcTaskRepository implements TaskRepository {
    private final JdbcTemplate jdbc;

    public JdbcTaskRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void save(Task task) {
        jdbc.update("""
                INSERT INTO tasks
                  (id, type, payload, status, attempts, max_attempts,
                   created_at, scheduled_at, priority, version)
                VALUES (?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, 0)
                """,
                UUID.fromString(task.id()), task.type(), task.payload(),
                task.status().name(), task.attempts(), task.maxAttempts(),
                task.createdAt(), task.scheduledAt(), task.priority());
    }

    public Optional<Task> findById(String id) {
        return jdbc.query("""
                SELECT id, type, payload, status, attempts, max_attempts,
                       created_at, scheduled_at, priority
                FROM tasks
                WHERE id = ?
                """, mapper(), UUID.fromString(id)).stream().findFirst();
    }

    @Transactional
    public List<Task> pollDue(int limit) {
        List<Task> tasks = jdbc.query("""
                SELECT id, type, payload, status, attempts, max_attempts,
                       created_at, scheduled_at, priority
                FROM tasks
                WHERE status IN ('PENDING', 'RETRYING', 'SCHEDULED')
                  AND scheduled_at <= now()
                ORDER BY priority DESC, created_at ASC
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """, mapper(), limit);

        for (Task task : tasks) {
            jdbc.update("""
                    UPDATE tasks
                    SET status = 'RUNNING', version = version + 1
                    WHERE id = ?
                    """, UUID.fromString(task.id()));
        }
        return tasks.stream().map(t -> withStatus(t, TaskStatus.RUNNING)).toList();
    }

    private RowMapper<Task> mapper() {
        return (rs, rowNum) -> new Task(
                rs.getObject("id", UUID.class).toString(),
                rs.getString("type"),
                rs.getString("payload"),
                TaskStatus.valueOf(rs.getString("status")),
                rs.getInt("attempts"),
                rs.getInt("max_attempts"),
                rs.getObject("created_at", Instant.class),
                rs.getObject("scheduled_at", Instant.class),
                rs.getInt("priority"));
    }
}
```

`withStatus` can be a helper on `Task` or a small mapper method.

### C6 - Validation and Error Responses

```java
public record ErrorResponse(
        String code,
        String message,
        List<FieldError> fields,
        Instant timestamp,
        String path) {
    public record FieldError(String field, String message) {}
}
```

```java
@RestControllerAdvice
public final class ApiExceptionHandler {
    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ErrorResponse> validation(MethodArgumentNotValidException ex,
                                             HttpServletRequest request) {
        List<ErrorResponse.FieldError> fields = ex.getBindingResult().getFieldErrors()
                .stream()
                .map(e -> new ErrorResponse.FieldError(e.getField(), e.getDefaultMessage()))
                .toList();
        return ResponseEntity.badRequest().body(new ErrorResponse(
                "validation_failed", "Request validation failed",
                fields, Instant.now(), request.getRequestURI()));
    }

    @ExceptionHandler(TaskQueueFullException.class)
    ResponseEntity<ErrorResponse> queueFull(TaskQueueFullException ex,
                                            HttpServletRequest request) {
        return ResponseEntity.status(429).body(new ErrorResponse(
                "queue_full", ex.getMessage(), List.of(),
                Instant.now(), request.getRequestURI()));
    }
}
```

### C7 - Test Coverage

Use direct unit tests for services:

```java
@Test
void submitPersistsPendingTask() {
    FakeTaskRepository repository = new FakeTaskRepository();
    Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    TaskSubmissionService service = new TaskSubmissionService(repository, clock);

    Task task = service.submit(new SubmitTaskRequest("email.send", json("{}"), 0, 3, null));

    assertThat(repository.findById(task.id())).contains(task);
}
```

Use `@WebMvcTest` for MVC behavior and Testcontainers for PostgreSQL behavior. The double-lease test should run two concurrent transactions against the same pending row and assert only one worker receives it.

### C8 - Worker Executor and Scheduling

```java
@Bean(name = "workerTaskExecutor")
ThreadPoolTaskExecutor workerTaskExecutor(TaskQueueProperties properties) {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(properties.workers().count());
    executor.setMaxPoolSize(properties.workers().count());
    executor.setQueueCapacity(0);
    executor.setThreadNamePrefix("task-worker-");
    executor.initialize();
    return executor;
}
```

```java
@Scheduled(fixedDelayString = "${taskqueue.retry.poll-interval:1s}")
void moveDueRetries() {
    retryService.moveDueRetriesBackToPending();
}
```

### C9 - Events and Metrics

```java
public record TaskSucceededEvent(String taskId, String type, Instant occurredAt) {}
```

```java
@EventListener
void on(TaskSucceededEvent event) {
    Counter.builder("taskqueue_tasks_terminal_total")
            .tag("type", event.type())
            .tag("status", "SUCCEEDED")
            .register(registry)
            .increment();
}
```

Use `taskId` in logs, not metric labels:

```java
log.info("task succeeded taskId={} type={}", event.taskId(), event.type());
```

### C10 - Production Shell

Minimal production config:

```yaml
server:
  shutdown: graceful
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      probes:
        enabled: true
```

Dockerfile:

```dockerfile
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/taskqueue.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/app.jar"]
```

Security:

```java
@Bean
SecurityFilterChain security(HttpSecurity http) throws Exception {
    return http
            .csrf(csrf -> csrf.disable())
            .authorizeHttpRequests(auth -> auth
                    .requestMatchers("/actuator/health/**").permitAll()
                    .requestMatchers(HttpMethod.POST, "/tasks").hasAuthority("SCOPE_tasks:write")
                    .requestMatchers(HttpMethod.GET, "/tasks/**").hasAuthority("SCOPE_tasks:read")
                    .anyRequest().authenticated())
            .oauth2ResourceServer(oauth -> oauth.jwt(Customizer.withDefaults()))
            .build();
}
```

---

## Refactoring Solutions

### R1 - Manual DI To Spring Beans

Move concrete construction out of `AppBootstrap` and into `@Configuration` classes. Convert handlers and services to components. Keep constructor injection. Delete manual maps only after `HandlerRegistry` builds from `List<TaskHandler>`.

### R2 - Controller Thinness

The controller should parse HTTP and return HTTP. Task creation belongs in `TaskSubmissionService` or `TaskFactory`. Persistence belongs behind `TaskRepository`.

### R3 - Repository Port

If a service imports `JdbcTemplate`, it knows too much about persistence. Introduce a method on `TaskRepository` and move SQL into `JdbcTaskRepository`.

### R4 - Worker Lifecycle

Use `SmartLifecycle` or an application runner to start workers after the context is ready. Stop workers during shutdown. Do not start threads in constructors.

### R5 - Configuration Cleanup

Move hard-coded values into `TaskQueueProperties`. Use environment-specific YAML and environment variables for overrides.

---

## Design Solutions

### D1 - Profiles vs Conditional Properties

Use profiles for broad environment differences such as local, test, staging, and production. Use conditional properties for product choices within an environment, such as `taskqueue.queue.type=postgres`. For this project, `local` profile can default to memory, while `prod` can require explicit `postgres` or broker configuration.

### D2 - JPA Boundary

Use JDBC for `pollDue`, lease updates, and hot status transitions because SQL and locking are central. Use JPA only for simpler admin views or related metadata where object mapping pays for itself and query count is controlled.

### D3 - Event Boundary

Spring events are fine for local metrics, local audit hooks, and decoupling inside one JVM. Lifecycle events that other services or worker nodes must observe belong on the Phase 4 broker-backed `EventBus`.

### D4 - Metrics Cardinality Budget

Recommended metrics:

| Metric | Type | Labels |
| --- | --- | --- |
| `taskqueue_tasks_submitted_total` | counter | `type` |
| `taskqueue_tasks_terminal_total` | counter | `type`, `status` |
| `taskqueue_task_retries_total` | counter | `type` |
| `taskqueue_dead_letter_total` | counter | `type`, `reason_code` |
| `taskqueue_task_execution_seconds` | timer | `type`, `outcome` |
| `taskqueue_queue_depth` | gauge | none or `queue` |
| `taskqueue_oldest_pending_age_seconds` | gauge | none |

Allowed label values must be bounded. Use `reason_code` from an enum, not exception messages.

### D5 - Production Failure Runbook

For "DLQ rate spiked after deploy":

1. Check deploy timestamp against DLQ spike.
2. Break down DLQ by `type` and `reason_code`.
3. Inspect recent logs for affected `taskId`s.
4. Compare handler exception messages before and after deploy.
5. Check downstream health and response codes.
6. Pause or rate-limit the affected task type if needed.
7. Roll back if the deploy introduced deterministic failures.
8. After fix, redrive only idempotent tasks or tasks with safe dedupe keys.

---

## Interview Drill Model Answers

### I1 - Auto-Configuration

Spring Boot auto-configuration is conditional bean registration. Boot looks at the classpath, existing beans, and properties. If it sees web dependencies, it configures MVC. If it sees a datasource and no custom datasource bean, it creates one. It is not magic; it is a large set of `@Conditional...` configuration classes.

### I2 - `POST /tasks`

The request reaches the servlet container, enters `DispatcherServlet`, maps to `TaskController.submit`, deserializes JSON into `SubmitTaskRequest`, validates it, calls `TaskSubmissionService`, creates a `Task`, saves through `TaskRepository`, and returns `202 Accepted` with a `Location` header.

### I3 - Transactions

`@Transactional` is usually applied by a Spring proxy. The proxy starts a transaction before the method and commits or rolls back after it. If a method calls another method on `this`, the call bypasses the proxy, so transactional behavior on the inner method does not apply.

### I4 - JDBC Over JPA

The leasing query needs exact SQL: status filters, scheduling condition, priority ordering, limit, and `FOR UPDATE SKIP LOCKED`. JDBC keeps that explicit. JPA can hide query count and locking behavior, which is risky in the queue hot path.

### I5 - Testing `SKIP LOCKED`

Use Testcontainers with PostgreSQL. Insert a pending task, start two concurrent transactions that call `pollDue(1)`, and assert only one gets the task. A mock cannot prove database lock behavior.

### I6 - `@Async`

`@Async` schedules a method call on an executor. If the process crashes, that work is lost. Durable async processing requires writing task state before returning, then workers lease, execute, retry, and persist outcome.

### I7 - Liveness vs Readiness

Liveness means the process is healthy enough to keep running; if it fails, the platform restarts it. Readiness means the instance should receive traffic. During startup, migration, DB outage, or graceful shutdown, readiness may be false while liveness remains true.

### I8 - JWT Scopes

Configure the app as an OAuth2 resource server. Validate token signature, issuer, audience, and expiration. Require `tasks:write` for `POST /tasks` and `tasks:read` for `GET /tasks/{id}`. Authentication identifies the caller; authorization decides allowed operations.

---

## Stretch Solutions

### S1 - Idempotency Key

Add `idempotency_key` to `tasks` with a unique index. On submit, insert with the key. On conflict, read the existing task and return it. Store a request hash if you need to reject the same key used with different payloads.

### S2 - Redis Cache

Configure Redis cache and add `@Cacheable` on `TaskQueryService.findById`. Use a short TTL such as five seconds. Evict on status changes. Treat the database as source of truth.

### S3 - Multi-Node Scheduler Safety

Use a database lock, Redis lock with fencing, or leader election. The scheduled job must also be idempotent so a brief double-run does not corrupt state.

### S4 - OpenAPI Contract

Use `springdoc-openapi` to generate the contract from controllers and DTOs. Add examples for submit success, validation failure, not found, and queue full.

### S5 - Production Dashboard

Dashboard panels:

- Submit rate by type.
- Execution latency p50/p95/p99.
- Terminal outcomes by status.
- Retry rate.
- DLQ rate by reason.
- Queue depth.
- Oldest pending age.
- Active workers.
- Executor saturation.
- Hikari active/idle connections.
- HTTP 4xx/5xx rate.
