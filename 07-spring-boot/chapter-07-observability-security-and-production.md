# Observability, Security, and Production

> Where this fits: the service now accepts tasks, persists them, validates input, runs workers, schedules retries, and emits events. This chapter turns that Spring Boot application into something an operator can run: observable, secure at the API edge, gracefully shutting down, containerized, and configured for production.

Production readiness is not one feature. It is the set of behaviors that make a service debuggable and survivable when it is slow, overloaded, attacked, misconfigured, or being deployed.

---

## Why This Exists

A task queue fails quietly. A client receives `202 Accepted`, then work happens later. If workers stall, retries explode, a downstream rejects requests, or the DLQ grows, users may not know until much later.

The service needs:

- Logs that explain what happened to a task.
- Metrics that show throughput, latency, queue depth, retries, and DLQ growth.
- Health checks for orchestration.
- Graceful shutdown so in-flight work is not abandoned blindly.
- Security on task submission and status endpoints.
- Thread pool and connection pool tuning.
- Docker packaging and production configuration.

---

## Real-World Problem

Production questions must be answerable:

- Is the API accepting tasks?
- Are workers draining tasks?
- How old is the oldest pending task?
- Are retries rising?
- Is the DLQ growing?
- Are database connections saturated?
- Did a deploy interrupt in-flight tasks?
- Who is allowed to submit tasks?

Spring Boot gives strong defaults through Actuator, Micrometer, Spring Security, and configuration. You still need to choose the signals and policies that fit the queue.

---

## Manual Implementation

Manual logging:

```java
System.out.println("task " + task.id() + " succeeded");
```

Manual metrics:

```java
public final class InMemoryMetrics {
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicInteger queueDepth = new AtomicInteger();
}
```

Manual health endpoint:

```java
if (database.ping() && workerPool.isRunning()) {
    return 200;
}
return 503;
```

This is useful for learning, but production needs standardized endpoints, scrapeable metrics, structured logs, and integration with deployment platforms.

---

## Pain Points

- `println` logs are hard to query.
- In-memory metrics disappear on restart and are not scrapeable.
- Health checks are inconsistent.
- Shutdown behavior is often accidental.
- Security is bolted on late.
- Thread pools and connection pools are left at defaults.
- Docker images include too much and configure too little.

Spring Boot production features solve the infrastructure shape; you still own the operational design.

---

## Logging

Use structured logging with correlation fields:

```java
log.info("task submitted taskId={} type={} status={}", task.id(), task.type(), task.status());
```

For request correlation, add a filter:

```java
@Component
public final class CorrelationIdFilter extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain)
            throws ServletException, IOException {
        String correlationId = Optional.ofNullable(request.getHeader("X-Correlation-Id"))
                .filter(s -> !s.isBlank())
                .orElse(UUID.randomUUID().toString());
        MDC.put("correlationId", correlationId);
        response.setHeader("X-Correlation-Id", correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove("correlationId");
        }
    }
}
```

Carry `taskId` and `correlationId` into worker logs where possible.

---

## Actuator and Health Checks

Add Actuator and expose safe endpoints:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      probes:
        enabled: true
      show-details: when_authorized
```

Spring Boot provides:

- `/actuator/health`
- `/actuator/health/liveness`
- `/actuator/health/readiness`
- `/actuator/metrics`
- `/actuator/prometheus` when the Prometheus registry is present

Add a custom health indicator for the queue:

```java
@Component
public final class TaskQueueHealthIndicator implements HealthIndicator {
    private final TaskRepository repository;

    public Health health() {
        long oldestAgeSeconds = repository.oldestPendingAgeSeconds();
        if (oldestAgeSeconds > 3600) {
            return Health.down()
                    .withDetail("oldestPendingAgeSeconds", oldestAgeSeconds)
                    .build();
        }
        return Health.up()
                .withDetail("oldestPendingAgeSeconds", oldestAgeSeconds)
                .build();
    }
}
```

Use readiness to decide whether the service should receive traffic. Use liveness to decide whether the platform should restart the process.

---

## Metrics With Micrometer and Prometheus

Define queue-specific meters:

```java
@Component
public final class TaskMetrics {
    private final MeterRegistry registry;

    public TaskMetrics(MeterRegistry registry, TaskRepository repository) {
        this.registry = registry;
        Gauge.builder("taskqueue_oldest_pending_age_seconds", repository,
                TaskRepository::oldestPendingAgeSeconds)
                .register(registry);
    }

    public void submitted(String type) {
        Counter.builder("taskqueue_tasks_submitted_total")
                .tag("type", type)
                .register(registry)
                .increment();
    }

    public Timer.Sample startExecution() {
        return Timer.start(registry);
    }

    public void stopExecution(Timer.Sample sample, String type, String outcome) {
        sample.stop(Timer.builder("taskqueue_task_execution_seconds")
                .tag("type", type)
                .tag("outcome", outcome)
                .publishPercentileHistogram()
                .register(registry));
    }
}
```

Keep labels bounded. `type` and `outcome` are fine. `taskId`, `userId`, and raw error messages are not metric labels.

Prometheus scrapes:

```yaml
scrape_configs:
  - job_name: taskqueue
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ["api:8080"]
```

---

## Graceful Shutdown

Configure shutdown:

```yaml
server:
  shutdown: graceful
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```

Worker shutdown policy:

1. Stop accepting new HTTP traffic.
2. Stop polling for new tasks.
3. Finish in-flight task if it can complete quickly.
4. If interrupted, release or let the lease expire.
5. Close database and executor resources.

The queue's lease timeout is part of shutdown safety. If the process dies mid-task, another worker should reclaim after visibility timeout.

---

## Thread Pools and HikariCP

Worker threads and database connections must be sized together. A worker pool of 100 with a Hikari pool of 10 means 90 workers can block waiting for DB access if every task touches the database.

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 2s
      max-lifetime: 30m

taskqueue:
  workers:
    count: 16
```

Tune from load tests and downstream limits, not guesses.

---

## Docker and Docker Compose

Dockerfile:

```dockerfile
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/taskqueue.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/app.jar"]
```

Compose for local production-like development:

```yaml
services:
  api:
    build: .
    environment:
      SPRING_PROFILES_ACTIVE: prod
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/taskqueue
      SPRING_DATASOURCE_USERNAME: taskqueue
      SPRING_DATASOURCE_PASSWORD: taskqueue
    ports:
      - "8080:8080"
    depends_on:
      - postgres

  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: taskqueue
      POSTGRES_USER: taskqueue
      POSTGRES_PASSWORD: taskqueue
```

For Phase 3 and Phase 4, add Redis, Prometheus, Grafana, and the broker.

---

## Basic JWT Authentication

Protect task submission:

```java
@Configuration
@EnableWebSecurity
public class SecurityConfig {
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
}
```

JWT does not make handlers safe. It authenticates callers. Authorization decides which callers can submit or read tasks.

---

## CORS

CORS is a browser policy. Configure it only for browser clients:

```java
@Bean
CorsConfigurationSource corsConfigurationSource() {
    CorsConfiguration config = new CorsConfiguration();
    config.setAllowedOrigins(List.of("https://app.example.com"));
    config.setAllowedMethods(List.of("GET", "POST"));
    config.setAllowedHeaders(List.of("Authorization", "Content-Type", "X-Correlation-Id"));
    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", config);
    return source;
}
```

Do not use wildcard origins with credentials in production.

---

## Production Configuration

```yaml
spring:
  profiles:
    active: prod
  datasource:
    hikari:
      maximum-pool-size: 20
  jpa:
    open-in-view: false

server:
  shutdown: graceful

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics

taskqueue:
  queue:
    type: postgres
    poll-batch-size: 50
  workers:
    count: 16
    shutdown-timeout: 30s
  retry:
    max-attempts: 5
    base-delay: 1s
    max-delay: 5m
```

Use environment variables for secrets and deployment-specific values.

---

## Project Integration

Add production capabilities in this order:

1. Structured logs and correlation id.
2. Actuator health/readiness/liveness.
3. Micrometer counters, timers, and gauges.
4. Graceful shutdown for API and workers.
5. HikariCP and worker pool tuning.
6. Docker Compose for API and Postgres.
7. JWT authentication and CORS.
8. Prometheus and Grafana.

---

## Tradeoffs

| Concern | Decision | Cost |
| --- | --- | --- |
| Actuator exposure | Expose health, metrics, prometheus | Must secure sensitive endpoints |
| Metrics | Bounded labels only | Less per-task detail in metrics |
| Graceful shutdown | Finish/release in-flight work | Deploys may take longer |
| JWT | Stateless auth | Token validation and key rotation needed |
| CORS | Explicit allowed origins | Environment-specific config |
| Docker Compose | Reproducible local stack | Not a substitute for production orchestration |

---

## Production Considerations

- Secure Actuator endpoints; health can be public, detailed metrics should not be.
- Alert on oldest pending task age, DLQ growth, retry rate, 5xx rate, and DB pool saturation.
- Keep metric cardinality low.
- Use JSON logging in production if your log platform supports it.
- Disable JPA open-in-view for backend APIs.
- Use readiness checks during deploys.
- Rotate JWT keys and validate issuer, audience, and expiration.
- Keep Docker images small and rebuildable.

---

## Exercises

### Easy

1. Add Actuator and expose health and Prometheus endpoints.
2. Define five Task Queue metrics.
3. Explain liveness vs readiness.

### Medium

4. Add `TaskMetrics` with counters and timers.
5. Add graceful shutdown configuration and worker stop behavior.
6. Write a Docker Compose file for API and Postgres.

### Hard

7. Add JWT-based endpoint authorization for `POST /tasks` and `GET /tasks/{id}`.
8. Build a production runbook: what do you check when oldest pending task age rises?

---

## Solutions

### S1-S3

Expose:

```yaml
management.endpoints.web.exposure.include: health,info,metrics,prometheus
```

Metrics: submitted count, terminal count by status, retry count, DLQ count, execution duration, queue depth, oldest pending age. Liveness asks "should the process be restarted?" Readiness asks "should this instance receive traffic?"

### S4-S6

Use Micrometer `Counter`, `Timer`, and `Gauge`; configure graceful shutdown; Docker Compose should include API and Postgres with datasource environment variables.

### S7-S8

Configure Spring Security as an OAuth2 resource server and protect endpoints by scopes. A runbook for old pending age should check worker health, DB locks, downstream latency, retry rate, executor saturation, queue depth by type, and recent deploys.

---

## Project Refactoring Task

1. Add Actuator and Prometheus registry.
2. Add `TaskMetrics`.
3. Add correlation-id logging.
4. Add graceful shutdown config.
5. Add Dockerfile and Docker Compose.
6. Add Spring Security JWT resource server config.
7. Add production profile configuration.

---

## Git Commit For This Chapter

```bash
git add pom.xml Dockerfile docker-compose.yml src/main/java src/main/resources
git commit -m "feat: productionize task queue spring boot service"
```

---

## Architecture Impact

The service is now operable:

```text
Client -> secured API -> task persistence -> workers
                 |             |             |
              logs/metrics/health/readiness/graceful shutdown
```

Production readiness wraps the same domain and queue model with observability, security, lifecycle, and deployment controls.

---

## Interview Questions

1. What does Spring Boot Actuator provide?
2. What is the difference between liveness and readiness?
3. Why is oldest pending task age a better alert than queue depth alone?
4. What is Micrometer?
5. Why should metric labels be bounded?
6. How does graceful shutdown protect workers?
7. How do worker pool size and HikariCP pool size interact?
8. What does JWT authentication prove?
9. What is CORS, and when does it matter?
10. What belongs in production configuration?

## Interview Takeaways

A production Spring Boot service is not just a running controller. It has health, metrics, logs, security, shutdown behavior, pool tuning, and deployment configuration that match the failure modes of the system it runs.
