# Persistence, JDBC, JPA, and Transactions

> Where this fits: Phase 2 makes the queue durable. The in-memory queue loses tasks on restart; this chapter moves task state into PostgreSQL, first with plain JDBC, then with Spring JDBC, then with a clear-eyed look at Spring Data JPA and transaction boundaries.

The goal is not to worship JPA. The goal is to persist `Task` lifecycle transitions correctly and understand every abstraction you choose.

---

## Why This Exists

An in-memory queue is a buffer, not a durable task queue. If the process dies after accepting a task but before executing it, the task is gone.

Durability means:

- `POST /tasks` writes a durable record before returning.
- Workers claim tasks without two workers executing the same row concurrently.
- Status changes are committed atomically.
- Failed tasks can be retried after a restart.
- Operators can query what happened.

Persistence is the difference between a demo and a backend service.

---

## Real-World Problem

The central operation is leasing due tasks:

```text
Find runnable task -> mark RUNNING -> commit -> worker executes
```

This must be safe with multiple workers. A naive `SELECT` followed by `UPDATE` can double-execute a task. A durable queue needs transactional polling:

```sql
SELECT id
FROM tasks
WHERE status IN ('PENDING', 'RETRYING', 'SCHEDULED')
  AND scheduled_at <= now()
ORDER BY priority DESC, created_at ASC
LIMIT ?
FOR UPDATE SKIP LOCKED;
```

`SKIP LOCKED` lets concurrent workers skip rows already claimed by another transaction.

---

## Manual Implementation: Plain JDBC

Start with a schema:

```sql
CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    attempts INT NOT NULL,
    max_attempts INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    priority INT NOT NULL,
    version BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_tasks_poll_due
    ON tasks (priority DESC, created_at ASC)
    WHERE status IN ('PENDING', 'RETRYING', 'SCHEDULED');
```

Plain JDBC is explicit:

```java
public final class JdbcTaskRepository implements TaskRepository {
    private final DataSource dataSource;

    public JdbcTaskRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Task task) {
        String sql = """
                INSERT INTO tasks
                  (id, type, payload, status, attempts, max_attempts,
                   created_at, scheduled_at, priority, version)
                VALUES (?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, 0)
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setObject(1, UUID.fromString(task.id()));
            ps.setString(2, task.type());
            ps.setString(3, task.payload());
            ps.setString(4, task.status().name());
            ps.setInt(5, task.attempts());
            ps.setInt(6, task.maxAttempts());
            ps.setObject(7, task.createdAt());
            ps.setObject(8, task.scheduledAt());
            ps.setInt(9, task.priority());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new TaskPersistenceException("failed to save task " + task.id(), e);
        }
    }
}
```

Plain JDBC teaches what is actually happening: connections, prepared statements, result sets, transactions, and SQL exceptions.

---

## Pain Points

Plain JDBC is useful but repetitive:

- Every method repeats connection and statement cleanup.
- Row mapping is manual.
- SQL exceptions need translation.
- Transactions are easy to start and hard to consistently close.
- Tests often repeat boilerplate setup.

Spring does not make SQL disappear. It removes the mechanics that distract from the SQL.

---

## Spring JDBC Implementation

`JdbcTemplate` handles resource management and exception translation:

```java
@Repository
public final class JdbcTaskRepository implements TaskRepository {
    private final JdbcTemplate jdbc;

    public JdbcTaskRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void save(Task task) {
        jdbc.update("""
                INSERT INTO tasks
                  (id, type, payload, status, attempts, max_attempts,
                   created_at, scheduled_at, priority, version)
                VALUES (?, ? , ?::jsonb, ?, ?, ?, ?, ?, ?, 0)
                """,
                UUID.fromString(task.id()),
                task.type(),
                task.payload(),
                task.status().name(),
                task.attempts(),
                task.maxAttempts(),
                task.createdAt(),
                task.scheduledAt(),
                task.priority());
    }

    @Override
    public Optional<Task> findById(String id) {
        return jdbc.query("""
                SELECT id, type, payload, status, attempts, max_attempts,
                       created_at, scheduled_at, priority
                FROM tasks
                WHERE id = ?
                """, taskRowMapper(), UUID.fromString(id)).stream().findFirst();
    }
}
```

The leasing operation must be transactional:

```java
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
            """, taskRowMapper(), limit);

    for (Task task : tasks) {
        jdbc.update("""
                UPDATE tasks
                SET status = 'RUNNING', version = version + 1
                WHERE id = ?
                """, UUID.fromString(task.id()));
    }
    return tasks.stream().map(t -> t.withStatus(TaskStatus.RUNNING)).toList();
}
```

The `SELECT FOR UPDATE` lock and the status update must happen in the same transaction.

---

## Transactions

A transaction is a boundary around a unit of consistency. For task submission:

```java
@Transactional
public Task submit(SubmitTaskRequest request) {
    Task task = factory.create(request);
    repository.save(task);
    return task;
}
```

For worker outcome handling:

```java
@Transactional
public void markSucceeded(String taskId) {
    repository.updateStatus(taskId, TaskStatus.SUCCEEDED);
}
```

For leasing:

```java
@Transactional
public List<Task> leaseBatch(int limit) {
    return repository.pollDue(limit);
}
```

Do not put a long-running handler execution inside the same database transaction that leases the task. Lease, commit, execute, then commit the outcome. Holding row locks during network calls destroys throughput.

---

## How Spring Transactions Work Internally

`@Transactional` is implemented with proxies:

1. Spring creates a proxy around the bean.
2. Callers call the proxy, not the raw object.
3. The proxy opens a transaction before the method.
4. The method runs.
5. The proxy commits on success or rolls back on configured exceptions.

This has two important consequences:

- Self-invocation does not start a transaction: `this.someTransactionalMethod()` bypasses the proxy.
- Transaction scope follows method boundaries on Spring-managed beans.

Keep transaction boundaries on application services or repository methods that are called from outside the bean.

---

## Spring Data JPA

JPA maps tables to entity objects:

```java
@Entity
@Table(name = "tasks")
public class TaskEntity {
    @Id
    private UUID id;

    private String type;

    @Column(columnDefinition = "jsonb")
    private String payload;

    @Enumerated(EnumType.STRING)
    private TaskStatus status;

    private int attempts;
    private int maxAttempts;
    private Instant createdAt;
    private Instant scheduledAt;
    private int priority;

    @Version
    private long version;
}
```

A repository becomes concise:

```java
public interface TaskJpaRepository extends JpaRepository<TaskEntity, UUID> {
    Optional<TaskEntity> findById(UUID id);
}
```

JPA is useful for aggregate-style CRUD, relationships, and optimistic locking. It is not automatically the best fit for a queue polling loop where exact SQL and locking behavior matter. For `pollDue`, plain SQL through JDBC is often clearer.

---

## Lazy Loading, N+1, and Queue Design

Lazy loading means related data is fetched only when accessed. It can cause N+1 queries:

```text
1 query to load 100 tasks
100 more queries to load each task's handler metadata or audit records
```

For the Task Queue, avoid rich object graphs in the hot polling path. A task row should contain the fields needed to route and execute work. Fetch handler-specific data inside the handler using an explicit repository call.

---

## Optimistic Locking

Optimistic locking uses a version column:

```sql
UPDATE tasks
SET status = ?, version = version + 1
WHERE id = ? AND version = ?;
```

If zero rows update, someone else changed the task first. This is useful for status updates and admin operations. For leasing, row locks plus `SKIP LOCKED` are still the primary tool.

---

## Indexing

Indexes must match the query:

```sql
CREATE INDEX idx_tasks_due
ON tasks (priority DESC, created_at ASC)
WHERE status IN ('PENDING', 'RETRYING', 'SCHEDULED');

CREATE INDEX idx_tasks_status_created
ON tasks (status, created_at);

CREATE INDEX idx_tasks_type_status
ON tasks (type, status);
```

Do not add indexes blindly. Every index speeds reads but slows writes and consumes storage.

---

## Project Integration

Recommended approach for this project:

- Use Flyway for schema.
- Use `JdbcTemplate` or Spring Data JDBC for the queue hot path.
- Use JPA only where entity mapping helps and SQL shape is not critical.
- Keep `TaskRepository` as a port so persistence details do not leak into services.
- Use Testcontainers for integration tests against real PostgreSQL.

---

## Tradeoffs

| Option | Strength | Weakness |
| --- | --- | --- |
| Plain JDBC | Maximum control | Boilerplate |
| `JdbcTemplate` | SQL control with less boilerplate | Still manual mapping |
| Spring Data JDBC | Simple aggregate persistence | Less mature for complex ORM behavior |
| Spring Data JPA | Rich mapping, repositories, optimistic locking | Hidden queries, lazy loading, N+1 risk |
| Database queue | Simple operational model | Polling load and DB write ceiling |
| Broker queue | Better scale and delivery model | Another system to operate |

For the Task Queue, prefer SQL clarity over ORM magic in the leasing path.

---

## Production Considerations

- Keep transactions short.
- Tune HikariCP pool size based on DB capacity, not worker count alone.
- Use partial indexes for polling.
- Archive terminal tasks or tables will grow forever.
- Avoid unbounded JSON payload size.
- Use idempotency keys for submission.
- Monitor slow queries, lock waits, and connection pool saturation.
- Prove crash recovery with integration tests.

---

## Exercises

### Easy

1. Explain why in-memory queues lose accepted tasks on restart.
2. Write the `tasks` table schema.
3. Explain what `FOR UPDATE SKIP LOCKED` prevents.

### Medium

4. Implement `save` and `findById` with `JdbcTemplate`.
5. Implement `pollDue(limit)` in one transaction.
6. Add indexes for the polling query.

### Hard

7. Add optimistic locking with a `version` column.
8. Compare a JDBC queue implementation and a JPA repository implementation. State which methods should not use JPA and why.

---

## Solutions

### S1-S3

An in-memory queue stores tasks in heap. A process crash loses heap contents. `FOR UPDATE SKIP LOCKED` prevents two concurrent transactions from claiming the same row and avoids blocking behind rows another worker already locked.

### S4-S6

Use `JdbcTemplate.update` for writes and `JdbcTemplate.query` with a `RowMapper` for reads. The leasing query and status update must be inside one `@Transactional` method.

```java
private RowMapper<Task> taskRowMapper() {
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
```

### S7-S8

Optimistic locking:

```java
int updated = jdbc.update("""
        UPDATE tasks
        SET status = ?, version = version + 1
        WHERE id = ? AND version = ?
        """, status.name(), id, expectedVersion);
if (updated == 0) {
    throw new OptimisticLockingFailureException("stale task " + id);
}
```

Use JDBC for `pollDue` because the SQL lock shape matters. JPA is reasonable for admin CRUD, simple reads, and status pages if you control lazy loading.

---

## Project Refactoring Task

1. Add Flyway migration for `tasks`.
2. Add `TaskRepository` implementation with `JdbcTemplate`.
3. Add `PostgresTaskQueue`.
4. Replace `InMemoryTaskQueue` in the production profile.
5. Add Testcontainers integration tests proving restart survival and no double lease.

---

## Git Commit For This Chapter

```bash
git add src/main/resources/db/migration src/main/java src/test/java
git commit -m "feat: persist tasks with jdbc and transactional leasing"
```

---

## Architecture Impact

Persistence becomes an outbound adapter:

```text
TaskSubmissionService -> TaskRepository port -> JdbcTaskRepository -> PostgreSQL
WorkerPool -> TaskQueue port -> PostgresTaskQueue -> PostgreSQL
```

The domain does not know whether state lives in memory, PostgreSQL, or a broker.

---

## Interview Questions

1. Why is plain `SELECT` then `UPDATE` unsafe for workers?
2. What does `FOR UPDATE SKIP LOCKED` do?
3. Why should handler execution not happen inside the leasing transaction?
4. How does `@Transactional` work internally?
5. What is self-invocation and why does it matter for transactions?
6. When would you prefer `JdbcTemplate` over JPA?
7. What causes the N+1 query problem?
8. What does optimistic locking protect against?
9. How do indexes affect writes?
10. How would you test persistence correctly?

## Interview Takeaways

Persistence is not just saving objects. For a task queue, the important design is the transactional claim, status transition, retry visibility, indexing strategy, and failure behavior under concurrent workers.
