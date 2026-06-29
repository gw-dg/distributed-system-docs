# Validation, Exception Handling, and Testing

> Where this fits: after the Task Submission API exists, it needs guardrails. Bad payloads must be rejected before they become tasks, domain failures must become consistent HTTP errors, and the persistence and worker behavior must be proven with focused tests.

Spring's validation and testing support is useful only if it protects real project boundaries: API input, service rules, persistence behavior, and worker outcomes.

---

## Why This Exists

Without validation, the queue accepts nonsense:

```json
{"type":"","payload":null,"priority":999999,"maxAttempts":0}
```

Without consistent exception handling, clients see random errors:

```text
500 Internal Server Error
java.lang.IllegalArgumentException: maxAttempts must be > 0
```

Without tests, a refactor from memory to Postgres can silently break retry, leasing, or API behavior.

This chapter creates a quality boundary around the Spring Boot service.

---

## Real-World Problem

The Task Queue receives untrusted input. It must enforce:

- `type` is required and maps to a known handler type pattern.
- `payload` is valid JSON and within size limits.
- `priority` is inside allowed bounds.
- `maxAttempts` is positive and capped.
- `scheduledAt` is not absurdly far in the past or future.

Validation belongs at the edge. Domain invariants belong in domain factories or constructors. Persistence errors belong behind repository exceptions. HTTP error formatting belongs in a global handler.

---

## Manual Implementation

A plain Java validator is the baseline:

```java
public final class SubmitTaskValidator {
    public void validate(SubmitTaskRequest request) {
        List<String> errors = new ArrayList<>();
        if (request.type() == null || request.type().isBlank()) {
            errors.add("type is required");
        }
        if (request.payload() == null) {
            errors.add("payload is required");
        }
        if (request.maxAttempts() != null && request.maxAttempts() < 1) {
            errors.add("maxAttempts must be >= 1");
        }
        if (!errors.isEmpty()) {
            throw new ValidationException(errors);
        }
    }
}
```

A manual controller catches exceptions:

```java
try {
    validator.validate(request);
    Task task = service.submit(request);
    return response(202, task);
} catch (ValidationException e) {
    return response(400, new ErrorResponse("validation_failed", e.errors()));
} catch (TaskQueueFullException e) {
    return response(429, new ErrorResponse("queue_full", List.of(e.getMessage())));
}
```

This works, but every controller repeats the same error-handling code.

---

## Pain Points

- Validators become scattered across controllers.
- Error responses drift in shape.
- Tests must check many hand-written branches.
- Custom validation rules are mixed with HTTP code.
- Unexpected exceptions leak stack traces or become unhelpful 500s.

Spring solves this with Bean Validation, method argument validation, and `@ControllerAdvice`.

---

## Spring Boot Validation

Add constraints to DTOs:

```java
public record SubmitTaskRequest(
        @NotBlank
        @Pattern(regexp = "^[a-z][a-z0-9]*(\\.[a-z][a-z0-9]*)*$")
        String type,

        @NotNull
        JsonNode payload,

        @Min(0)
        @Max(100)
        Integer priority,

        @Min(1)
        @Max(20)
        Integer maxAttempts,

        Instant scheduledAt) {
}
```

Trigger validation with `@Valid`:

```java
@PostMapping
public ResponseEntity<TaskResponse> submit(@Valid @RequestBody SubmitTaskRequest request) {
    Task task = submissionService.submit(request);
    return ResponseEntity.accepted()
            .location(URI.create("/tasks/" + task.id()))
            .body(TaskResponse.from(task));
}
```

Use a custom validator when the rule needs project knowledge:

```java
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = KnownTaskTypeValidator.class)
public @interface KnownTaskType {
    String message() default "unknown task type";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
```

```java
public final class KnownTaskTypeValidator implements ConstraintValidator<KnownTaskType, String> {
    private final HandlerRegistry handlers;

    public KnownTaskTypeValidator(HandlerRegistry handlers) {
        this.handlers = handlers;
    }

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        return value != null && handlers.supports(value);
    }
}
```

Use custom validation carefully. A DTO format rule is cheap. A validator that calls a database can turn validation into hidden I/O.

---

## Global Exception Handling

Define one error shape:

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

Handle exceptions centrally:

```java
@RestControllerAdvice
public final class ApiExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ErrorResponse> validation(MethodArgumentNotValidException ex,
                                             HttpServletRequest request) {
        List<ErrorResponse.FieldError> fields = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(e -> new ErrorResponse.FieldError(e.getField(), e.getDefaultMessage()))
                .toList();

        return ResponseEntity.badRequest().body(new ErrorResponse(
                "validation_failed",
                "Request validation failed",
                fields,
                Instant.now(),
                request.getRequestURI()));
    }

    @ExceptionHandler(TaskQueueFullException.class)
    ResponseEntity<ErrorResponse> queueFull(TaskQueueFullException ex,
                                            HttpServletRequest request) {
        return ResponseEntity.status(429).body(new ErrorResponse(
                "queue_full",
                ex.getMessage(),
                List.of(),
                Instant.now(),
                request.getRequestURI()));
    }
}
```

Controllers stay focused on the happy path. Error translation is one policy.

---

## Testing Pyramid For This Project

| Test type | Tool | What it proves |
| --- | --- | --- |
| Unit | JUnit 5, AssertJ, Mockito | Service rules, worker outcome logic, retry decisions |
| Controller slice | `@WebMvcTest`, MockMvc | HTTP mapping, validation, status codes, error bodies |
| Repository integration | `@JdbcTest` or `@DataJpaTest`, Testcontainers | SQL, migrations, row mapping, locks |
| Application integration | `@SpringBootTest`, Testcontainers | Bean graph, config, API plus DB |
| Concurrency/stress | JUnit harness | No double lease, no lost tasks under load |

Do not use `@SpringBootTest` for everything. It is slower and less focused.

---

## Unit Tests

Unit-test services without Spring:

```java
class TaskSubmissionServiceTest {
    private final FakeTaskRepository repository = new FakeTaskRepository();
    private final Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    private final TaskSubmissionService service = new TaskSubmissionService(repository, clock);

    @Test
    void createsPendingTask() {
        Task task = service.submit(new SubmitTaskRequest("email.send", json("{}"), 5, 3, null));

        assertThat(task.status()).isEqualTo(TaskStatus.PENDING);
        assertThat(repository.findById(task.id())).contains(task);
    }
}
```

Use Mockito when the dependency is interaction-oriented:

```java
@Test
void exhaustedRetryGoesToDeadLetterQueue() {
    DeadLetterQueue dlq = mock(DeadLetterQueue.class);
    RetryPolicy retry = attempt -> Optional.empty();
    RetryHandler handler = new RetryHandler(retry, dlq);

    handler.handleFailure(task, "boom");

    verify(dlq).send(eq(task), contains("boom"));
}
```

---

## Controller Tests

```java
@WebMvcTest(TaskController.class)
class TaskControllerValidationTest {
    @Autowired MockMvc mvc;
    @MockBean TaskSubmissionService submissionService;
    @MockBean TaskQueryService queryService;

    @Test
    void rejectsBlankType() throws Exception {
        mvc.perform(post("/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"type\":\"\",\"payload\":{}}"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.code").value("validation_failed"));
    }
}
```

This test starts the MVC slice, not the database or worker pool.

---

## Integration Tests With Testcontainers

Use a real database for repository behavior:

```java
@SpringBootTest
@Testcontainers
class TaskRepositoryIntegrationTest {
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired TaskRepository repository;

    @Test
    void savesAndReadsTask() {
        Task task = sampleTask("email.send");
        repository.save(task);

        assertThat(repository.findById(task.id())).contains(task);
    }
}
```

Do not mock Postgres for SQL behavior. The locking, JSONB, indexes, and timestamp behavior are exactly what you need to test.

---

## How Spring Testing Works Internally

Spring tests cache application contexts. If two test classes use the same configuration, Spring reuses the context to save startup time. Changing profiles, properties, mocks, or dirtied context state can force a new context.

`@MockBean` replaces a bean in the Spring context with a Mockito mock. Use it in slice tests. Avoid using it to hide broken application wiring in full integration tests.

---

## Project Integration

Add quality gates:

- `@Valid` on `POST /tasks`.
- `@RestControllerAdvice` for error responses.
- Unit tests for `TaskSubmissionService`, `RetryHandler`, `Worker`.
- Controller tests for HTTP behavior.
- Testcontainers tests for repository and leasing.
- A concurrency test proving two workers cannot lease the same task.

---

## Tradeoffs

| Tool | Benefit | Cost |
| --- | --- | --- |
| Bean Validation | Declarative edge rules | Can hide complex logic in annotations |
| Custom validators | Reusable project rules | Risk of slow validation if they call infrastructure |
| `@ControllerAdvice` | Consistent errors | One more indirection while debugging |
| Mockito | Fast interaction tests | Over-mocking implementation details |
| Testcontainers | Real infrastructure behavior | Slower, needs Docker |
| `@SpringBootTest` | Proves full context | Slow if overused |

---

## Production Considerations

- Never return stack traces or raw SQL errors to clients.
- Log validation failures at low severity; they are usually client mistakes.
- Log unexpected exceptions with correlation id and task id where available.
- Keep error codes stable because clients may depend on them.
- Cap request payload size before validation.
- Run integration tests in CI with Docker available.

---

## Exercises

### Easy

1. Add Bean Validation constraints to `SubmitTaskRequest`.
2. Explain why validation belongs at the API edge.
3. Define a stable `ErrorResponse` shape.

### Medium

4. Add `ApiExceptionHandler`.
5. Write a `@WebMvcTest` that rejects an invalid task request.
6. Write a unit test for `TaskSubmissionService` with a fixed `Clock`.

### Hard

7. Write a Testcontainers test proving `pollDue` does not double-lease a task.
8. Add a custom `@KnownTaskType` validator and discuss whether it belongs in validation or service logic.

---

## Solutions

### S1-S3

Use `@NotBlank`, `@NotNull`, `@Min`, `@Max`, and a stable error response with `code`, `message`, `fields`, `timestamp`, and `path`.

### S4-S6

`@RestControllerAdvice` centralizes validation and domain exceptions. Unit tests should instantiate services directly when possible:

```java
TaskSubmissionService service = new TaskSubmissionService(fakeRepository, fixedClock);
```

That test is faster and clearer than starting Spring for pure business logic.

### S7-S8

The no-double-lease test should insert one pending task, run two concurrent `pollDue(1)` calls, and assert only one receives the task. Use a real PostgreSQL container because row locks are database behavior.

`@KnownTaskType` is reasonable if it checks an in-memory registry. If it requires database calls or feature flags, keep that rule in service logic and return a domain error.

---

## Project Refactoring Task

1. Add DTO validation.
2. Add `ApiExceptionHandler`.
3. Add unit, controller, repository, and full-context tests.
4. Add Testcontainers for PostgreSQL.
5. Add a no-double-lease test.

---

## Git Commit For This Chapter

```bash
git add src/main/java src/test/java pom.xml
git commit -m "test: add validation error handling and spring boot test coverage"
```

---

## Architecture Impact

Validation and error handling strengthen the API adapter:

```text
HTTP request -> DTO validation -> service use case -> repository
             -> centralized error response on failure
```

Tests now map to architecture layers instead of one slow full-stack test for every behavior.

---

## Interview Questions

1. What is Bean Validation?
2. Where should validation live?
3. What is `@RestControllerAdvice`?
4. Why use a stable error response shape?
5. What is the difference between a unit test and an integration test?
6. When should you use Testcontainers?
7. What does `@WebMvcTest` start?
8. Why should you avoid `@SpringBootTest` for every test?
9. When is Mockito helpful, and when is it harmful?
10. How would you test `FOR UPDATE SKIP LOCKED` behavior?

## Interview Takeaways

Validation protects the edge, exception handling protects the contract, and tests protect the architecture. Spring gives useful tools, but the testing strategy should mirror the system boundaries.
