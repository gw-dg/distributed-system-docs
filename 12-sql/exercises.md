# SQL Exercises

> Work through these in order. Each exercise is designed to be solved without looking at the solutions file. If you are stuck for more than 15 minutes on a medium exercise or 30 minutes on a hard one, re-read the relevant chapter section — not the solution.

---

## Schema Reference

Use this schema for all exercises. It is the actual schema from the project, extended with a delivery domain for analytics exercises.

```sql
-- Core project tables (Phases 2-4)
CREATE TABLE tasks (
    id            UUID         PRIMARY KEY,
    type          TEXT         NOT NULL,
    payload       JSONB        NOT NULL,
    status        TEXT         NOT NULL CHECK (status IN ('PENDING','SCHEDULED','RUNNING','SUCCEEDED','FAILED','RETRYING','DEAD')),
    attempts      INT          NOT NULL DEFAULT 0,
    max_attempts  INT          NOT NULL DEFAULT 5,
    priority      INT          NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    scheduled_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_error    TEXT
);

CREATE TABLE dead_letter (
    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id     UUID         NOT NULL,
    type        TEXT         NOT NULL,
    payload     JSONB        NOT NULL,
    reason      TEXT         NOT NULL,
    failed_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID         NOT NULL,   -- task id
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ             -- NULL until relay ships it
);

-- Delivery domain (for analytics exercises)
CREATE TABLE delivery_partners (
    id      UUID  PRIMARY KEY DEFAULT gen_random_uuid(),
    name    TEXT  NOT NULL,
    city    TEXT  NOT NULL,
    phone   TEXT  NOT NULL,
    rating  NUMERIC(3,2)
);

CREATE TABLE orders (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_partner_id UUID         REFERENCES delivery_partners(id),
    order_type          TEXT         NOT NULL CHECK (order_type IN ('express', 'standard', 'scheduled')),
    status              TEXT         NOT NULL CHECK (status IN ('placed','assigned','picked','delivered','cancelled')),
    city                TEXT         NOT NULL,
    amount              NUMERIC(10,2) NOT NULL,
    weight_kg           NUMERIC(5,2),
    placed_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    delivered_at        TIMESTAMPTZ,
    delivery_cost       NUMERIC(10,2)
);
```

---

## Chapter 1 Exercises — How SQL Executes

### Easy

**E1.1** — Execution order recall

Write the SQL execution order from memory (8 steps). Then explain in one sentence why you cannot use a `SELECT` alias in a `WHERE` clause.

---

**E1.2** — Fix the broken query

The following query errors. Identify the error, explain why it happens based on execution order, and fix it.

```sql
SELECT 
    type,
    COUNT(*) AS task_count,
    AVG(attempts) AS avg_attempts
FROM tasks
WHERE task_count > 10
GROUP BY type;
```

---

**E1.3** — WHERE vs HAVING

Rewrite this query to make it as efficient as possible. Explain what was wrong.

```sql
SELECT type, COUNT(*) AS cnt
FROM tasks
GROUP BY type
HAVING type = 'email';
```

---

### Medium

**E1.4** — Translate English to SQL

Write a query for: *"Find all tasks of type 'email' that have been attempted at least 2 times but not yet succeeded, ordered by most recent update first."*

Use the 7-step translation method from Chapter 1. Write the steps in comments before the query.

---

**E1.5** — NOT IN trap

The following query is supposed to find tasks that have NOT been dead-lettered. It returns zero rows. Explain why and fix it.

```sql
SELECT id, type, status
FROM tasks
WHERE id NOT IN (
    SELECT task_id FROM dead_letter
);
```

*Hint: check what `dead_letter.task_id` contains and what `NOT IN` does with NULLs.*

---

**E1.6** — The SELECT alias rule in ORDER BY

Write a query that:
1. Selects tasks with `status = 'FAILED'`
2. Computes a column `days_since_failure` = number of days between `updated_at` and now
3. Orders by `days_since_failure` descending (oldest failures first)
4. Uses the alias in `ORDER BY` (prove this is valid)

---

### Hard

**E1.7** — Reconstruct the SKIP LOCKED query

Without looking at any project files, write the full dequeue query for `PostgresTaskQueue` from memory. It must:
- Select the required columns
- Filter to only runnable statuses
- Filter to tasks due by now
- Order by priority (high first), then creation time (earliest first)
- Limit to a configurable batch size
- Lock selected rows and skip already-locked rows

Then explain in plain English what `FOR UPDATE SKIP LOCKED` does physically.

---

## Chapter 2 Exercises — Querying and Joins

### Easy

**E2.1** — JOIN type selection

For each of these requirements, state whether you would use `INNER JOIN` or `LEFT JOIN` and why:

a) Find all orders with their delivery partner's name (every order has a partner)
b) Find all delivery partners, including those with no orders yet
c) Find all tasks that have an outbox entry (not all tasks are in the outbox)
d) Find all tasks with their dead-letter reason (most tasks are NOT in dead_letter)

---

**E2.2** — COUNT(*) vs COUNT(column)

Predict the output of each query for a `delivery_partners` table where 3 partners exist but only 2 have placed orders:

```sql
-- Query A
SELECT COUNT(*) FROM delivery_partners dp
LEFT JOIN orders o ON o.delivery_partner_id = dp.id;

-- Query B
SELECT COUNT(o.id) FROM delivery_partners dp
LEFT JOIN orders o ON o.delivery_partner_id = dp.id;
```

Explain the difference.

---

**E2.3** — Fix the LEFT JOIN

This query is supposed to return all delivery partners with their express order count (0 if none). It returns wrong results. Fix it and explain what was wrong.

```sql
SELECT dp.name, COUNT(o.id) AS express_count
FROM delivery_partners dp
LEFT JOIN orders o ON o.delivery_partner_id = dp.id
WHERE o.order_type = 'express'
GROUP BY dp.id, dp.name;
```

---

### Medium

**E2.4** — Multi-table join

Write a query that returns:
- Task id and type
- Whether it has an outbox entry (show `TRUE`/`FALSE`)
- Whether it has a dead-letter entry (show `TRUE`/`FALSE`)

For ALL tasks, including those with neither.

---

**E2.5** — Zomato-style join

Write a query for: *"Find the top 5 delivery partners in Mumbai who delivered the most 'express' orders in the last 30 days. Show their name, total express deliveries, and total delivery cost earned."*

Write the 7 translation steps in comments before the query.

---

**E2.6** — EXISTS vs IN

Rewrite this query using `EXISTS` instead of `IN`. Then explain which performs better and why.

```sql
SELECT * FROM tasks
WHERE id IN (
    SELECT aggregate_id FROM outbox WHERE published_at IS NULL
);
```

---

### Hard

**E2.7** — The N+1 SQL problem

The following two approaches return the same data. Identify which has an N+1 problem, explain exactly what queries run in each case (with N=1000 tasks), and rewrite the bad version.

```sql
-- Approach A: one query per task
SELECT name FROM handlers WHERE type = ?;  -- called 1000 times in application code

-- Approach B: one query
SELECT t.id, t.type, h.name AS handler_name
FROM tasks t
LEFT JOIN handlers h ON h.type = t.type;
```

Now: write the `JOIN`-based query for fetching all `FAILED` tasks from the last 24 hours along with their dead-letter reason (if one exists).

---

## Chapter 3 Exercises — Aggregations and Analytics

### Easy

**E3.1** — Basic aggregation

Write a query that returns, for each task `type`:
- Total tasks
- Tasks currently pending
- Tasks that succeeded
- Tasks that failed or are dead
- Average attempts per task

---

**E3.2** — Conditional aggregation

Write a query returning one row per `order_type` showing:
- The order type
- Total order count
- Count of delivered orders
- Count of cancelled orders
- Delivery rate (delivered / total, as a percentage rounded to 2 decimals)

Use conditional aggregation — do not use subqueries.

---

**E3.3** — HAVING vs WHERE

Write two versions of this query:
*"Find task types that have more than 50 failed tasks."*

Version A: using `HAVING`
Version B: using a subquery with `WHERE` on the outer query

Which is more readable? Which might be more efficient?

---

### Medium

**E3.4** — Top-N per group

Write a query for: *"Find the top 3 delivery partners in each city by total orders delivered. Show city, partner name, order count, and their rank within their city."*

Requirements:
- Use `DENSE_RANK()`
- Partners with equal order counts should share a rank
- Include all cities, including those with fewer than 3 active partners

---

**E3.5** — Running total

Write a query that shows daily task submission count and a running cumulative total since the beginning of the dataset.

Output columns: `day`, `daily_submitted`, `cumulative_submitted`

---

**E3.6** — Failure rate by handler type (project query)

Write the Phase 3 dashboard query that shows:
- Handler type (`type`)
- Total tasks in last 24 hours
- Succeeded count
- Failed + Dead count combined
- Failure rate % (rounded to 2 decimal places)
- Average attempts for failed tasks

Order by failure rate descending. Handle division by zero.

---

### Hard

**E3.7** — Delivery partner performance report

Write a single query (no application-side processing) that produces a performance report for all delivery partners with at least 20 orders. For each partner show:
- Name, city
- Total deliveries
- Express delivery count + percentage
- Average delivery cost
- Their rank within their city by total deliveries
- Whether they are "above average" or "below average" compared to the city average delivery count (use a window function)

---

**E3.8** — Day-over-day task processing

Write a query showing, for each day in the last 7 days:
- Date
- Tasks submitted that day
- Tasks completed (SUCCEEDED) that day
- Tasks failed that day
- The previous day's submission count (using LAG)
- Day-over-day change in submissions (current minus previous)

---

## Chapter 4 Exercises — Indexes and Performance

### Easy

**E4.1** — Index identification

For each query, state whether an index would help and what the ideal index would be:

a) `SELECT * FROM tasks WHERE id = '123e4567-e89b-12d3-a456-426614174000'`
b) `SELECT * FROM tasks WHERE status = 'PENDING' ORDER BY created_at ASC`
c) `SELECT * FROM tasks WHERE lower(type) = 'email'`
d) `SELECT * FROM tasks WHERE attempts > 3`
e) `SELECT COUNT(*) FROM tasks` (no WHERE clause)

---

**E4.2** — Leftmost prefix rule

Given this composite index: `CREATE INDEX idx ON orders (city, order_type, delivered_at DESC);`

For each query, state whether the index will be used (fully, partially, or not at all):

a) `WHERE city = 'Delhi'`
b) `WHERE order_type = 'express'`
c) `WHERE city = 'Delhi' AND order_type = 'express'`
d) `WHERE city = 'Delhi' ORDER BY delivered_at DESC`
e) `WHERE city = 'Delhi' AND order_type = 'express' ORDER BY delivered_at DESC`

---

**E4.3** — Partial vs full index

The `tasks` table has 10 million rows. 9.9 million have `status = 'SUCCEEDED'` or `status = 'FAILED'`. 100,000 are in runnable states.

Compare the size and performance of:
- A full index on `(priority DESC, created_at ASC)`
- A partial index on `(priority DESC, created_at ASC) WHERE status IN ('PENDING', 'SCHEDULED', 'RETRYING')`

Which would you choose for the dequeue query and why?

---

### Medium

**E4.4** — EXPLAIN output analysis

Read this `EXPLAIN ANALYZE` output and answer the questions below:

```
Limit  (cost=8920.33..8920.37 rows=16 width=200) (actual time=145.22..145.23 rows=16 loops=1)
  ->  Sort  (cost=8920.33..8932.83 rows=5000 width=200) (actual time=145.21..145.22 rows=16 loops=1)
        Sort Key: priority DESC, created_at ASC
        Sort Method: external merge  Disk: 12048kB
        ->  Seq Scan on tasks  (cost=0.00..8650.00 rows=5000 width=200) (actual time=0.018..120.14 rows=5000 loops=1)
              Filter: ((status = ANY ('{PENDING,SCHEDULED,RETRYING}'::text[])) AND (scheduled_at <= now()))
              Rows Removed by Filter: 995000
Planning Time: 0.312 ms
Execution Time: 145.31 ms
```

Answer:
1. What does `Seq Scan` tell you?
2. What does `Rows Removed by Filter: 995000` tell you?
3. What does `Sort Method: external merge Disk: 12048kB` tell you?
4. What single change would most dramatically improve this query?
5. After the fix, what would you expect to see in the `EXPLAIN` output?

---

**E4.5** — Design indexes for the project

Design all indexes needed for these queries to be fast. Justify each index choice.

```sql
-- Query A: dequeue (most critical)
SELECT ... FROM tasks
WHERE status IN ('PENDING','SCHEDULED','RETRYING') AND scheduled_at <= now()
ORDER BY priority DESC, created_at ASC LIMIT 16 FOR UPDATE SKIP LOCKED;

-- Query B: ops dashboard
SELECT status, COUNT(*) FROM tasks GROUP BY status;

-- Query C: find by idempotency key
SELECT id FROM tasks WHERE idempotency_key = ?;

-- Query D: stuck task reaper
UPDATE tasks SET status='RETRYING' WHERE status='RUNNING' AND updated_at < now() - INTERVAL '5 minutes';

-- Query E: outbox relay
SELECT * FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED;
```

---

### Hard

**E4.6** — The slow dashboard query

A dashboard query runs every 30 seconds and takes 4 seconds to execute. The table has 50 million rows. Here is the query:

```sql
SELECT 
    DATE_TRUNC('day', created_at) AS day,
    type,
    COUNT(*) AS total,
    COUNT(CASE WHEN status = 'SUCCEEDED' THEN 1 END) AS succeeded
FROM tasks
WHERE created_at >= NOW() - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', created_at), type
ORDER BY day DESC, type;
```

Diagnose the performance problem, propose at least two different solutions (indexing, query restructuring, materialized view, etc.), and discuss the tradeoffs of each.

---

**E4.7** — Dead tuple investigation

After running the task queue for 1 week with 10,000 tasks/hour processed, you run:

```sql
SELECT n_live_tup, n_dead_tup, last_autovacuum
FROM pg_stat_user_tables
WHERE tablename = 'tasks';
```

Result: `n_live_tup = 50000`, `n_dead_tup = 2800000`, `last_autovacuum = '2 hours ago'`

Explain:
1. Why are there 2.8 million dead tuples for only 50k live rows?
2. Why hasn't autovacuum kept up?
3. What are the performance consequences?
4. What are your immediate and long-term fixes?

---

## Stretch Challenges

**S1** — The recursive CTE

Find the "retry chain" for a specific task: all attempts of the same task, in order, showing the status at each attempt and the time between attempts.

*Hint: This requires either a self-join on task_id (if you store attempt history) or a recursive CTE. Design the schema addition you would need.*

**S2** — Schema design question

Design a schema for a multi-tenant version of the task queue where:
- Each tenant has a rate limit (max tasks per minute)
- Tasks must be isolated between tenants (tenant A cannot see tenant B's tasks)
- The dequeue query must still be fast
- The dashboard must show per-tenant throughput

Write the schema, all indexes, and the modified dequeue query.

**S3** — The full Zomato analytics query

Write a single SQL query that produces a leaderboard for the weekly delivery partner awards:
- Top partner by total deliveries in their city
- Top partner by express delivery percentage (minimum 20 total deliveries)
- Top partner by average rating
- A partner can win multiple categories

Output should show: category, partner name, city, relevant metric value.
