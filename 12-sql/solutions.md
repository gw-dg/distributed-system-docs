# SQL Exercise Solutions

> Read only after a genuine attempt. Solutions include the answer, explanation of why it works, common wrong approaches, and what the solution proves about the underlying concept.

---

## Chapter 1 Solutions — How SQL Executes

### E1.1

```
Execution order:
1. FROM          (load tables, execute JOINs)
2. WHERE         (filter individual rows)
3. GROUP BY      (collapse rows into groups)
4. HAVING        (filter groups)
5. SELECT        (compute output columns, aliases defined HERE)
6. DISTINCT      (remove duplicate rows)
7. ORDER BY      (sort — can use SELECT aliases)
8. LIMIT/OFFSET  (trim to page)
```

**Why you cannot use a SELECT alias in WHERE:**
WHERE runs at step 2. SELECT aliases are defined at step 5. At step 2, the alias does not exist yet — the database has not yet computed what the alias refers to.

---

### E1.2

```sql
-- The error: WHERE task_count > 10 uses the alias 'task_count'
-- Aliases are defined in SELECT (step 5); WHERE runs at step 2
-- Fix A: use HAVING (runs after GROUP BY at step 4, can filter on aggregates)
SELECT 
    type,
    COUNT(*) AS task_count,
    AVG(attempts) AS avg_attempts
FROM tasks
GROUP BY type
HAVING COUNT(*) > 10;

-- Fix B: wrap in a subquery/CTE and filter in the outer WHERE
WITH stats AS (
    SELECT type, COUNT(*) AS task_count, AVG(attempts) AS avg_attempts
    FROM tasks
    GROUP BY type
)
SELECT * FROM stats WHERE task_count > 10;
```

**Fix A is correct and efficient.** Fix B is equally correct and sometimes more readable for complex conditions, but has the same performance profile for this case.

---

### E1.3

```sql
-- WRONG (original): filters in HAVING — groups ALL types, then throws away all except 'email'
SELECT type, COUNT(*) AS cnt
FROM tasks
GROUP BY type
HAVING type = 'email';

-- CORRECT: filter in WHERE — only groups 'email' rows, much less work
SELECT type, COUNT(*) AS cnt
FROM tasks
WHERE type = 'email'
GROUP BY type;
```

**Why HAVING is wrong here:** `HAVING` runs after `GROUP BY`. The original query groups every task by type (potentially millions of rows across hundreds of types) and then throws away all groups except 'email'. The `WHERE` version filters to only 'email' rows before grouping — the GROUP BY has only email rows to process.

**Correct use of HAVING:** filter on the result of an aggregate function, e.g., `HAVING COUNT(*) > 10`. You cannot put `COUNT(*) > 10` in WHERE because WHERE runs before COUNT is computed.

---

### E1.4

```sql
-- 7-step translation:
-- 1. FROM: tasks table
-- 2. WHERE: type='email', attempts >= 2, status != 'SUCCEEDED'
-- 3. GROUP BY: not needed (individual rows, no aggregation)
-- 4. HAVING: not needed
-- 5. SELECT: all relevant columns
-- 6. ORDER BY: updated_at DESC (most recent first)
-- 7. LIMIT: not specified, so none

SELECT 
    id,
    type,
    status,
    attempts,
    max_attempts,
    last_error,
    updated_at
FROM tasks
WHERE type = 'email'
  AND attempts >= 2
  AND status != 'SUCCEEDED'
ORDER BY updated_at DESC;
```

**Alternative for "not yet succeeded":** `AND status NOT IN ('SUCCEEDED', 'DEAD')` — depending on whether you also want to exclude dead-lettered tasks, which are permanently failed. Always clarify the requirement.

---

### E1.5

```sql
-- WHY IT RETURNS ZERO ROWS:
-- NOT IN (SELECT task_id FROM dead_letter) would return zero rows
-- if ANY value in the subquery is NULL.
-- 
-- NULL IN (1, 2, NULL) = NULL (not FALSE, not TRUE)
-- NOT NULL = NULL
-- WHERE NULL does not match any row
-- 
-- If dead_letter.task_id has even one NULL, the entire NOT IN returns nothing.

-- FIX A: Use NOT EXISTS (handles NULLs correctly)
SELECT id, type, status
FROM tasks t
WHERE NOT EXISTS (
    SELECT 1 FROM dead_letter dl WHERE dl.task_id = t.id
);

-- FIX B: Add IS NOT NULL to the subquery (if task_id can genuinely be NULL)
SELECT id, type, status
FROM tasks
WHERE id NOT IN (
    SELECT task_id FROM dead_letter WHERE task_id IS NOT NULL
);

-- FIX C: Use LEFT JOIN + IS NULL (the "anti-join" pattern)
SELECT t.id, t.type, t.status
FROM tasks t
LEFT JOIN dead_letter dl ON dl.task_id = t.id
WHERE dl.task_id IS NULL;
```

**Best practice:** always use `NOT EXISTS` for "does not exist in another table" queries. It is correct regardless of NULLs and is often more efficient.

---

### E1.6

```sql
SELECT 
    id,
    type,
    status,
    updated_at,
    EXTRACT(DAY FROM (NOW() - updated_at)) AS days_since_failure
FROM tasks
WHERE status = 'FAILED'
ORDER BY days_since_failure DESC;  -- alias works here: ORDER BY runs after SELECT

-- Alternative using INTERVAL for cleaner output:
SELECT 
    id,
    type,
    NOW() - updated_at AS time_since_failure
FROM tasks
WHERE status = 'FAILED'
ORDER BY time_since_failure DESC;
```

**Why the alias works in ORDER BY:** ORDER BY runs at step 7, after SELECT at step 5. The alias `days_since_failure` is defined in SELECT and is visible to ORDER BY. This is one of the few places WHERE aliases can be used — the other is in subsequent CTEs.

---

### E1.7

```sql
SELECT 
    id,
    type,
    payload,
    status,
    attempts,
    max_attempts,
    created_at,
    scheduled_at,
    priority
FROM tasks
WHERE status IN ('PENDING', 'SCHEDULED', 'RETRYING')
  AND scheduled_at <= now()
ORDER BY priority DESC, created_at ASC
LIMIT ?
FOR UPDATE SKIP LOCKED;
```

**What FOR UPDATE SKIP LOCKED does physically:**

`FOR UPDATE` acquires a row-level exclusive lock on each selected row. No other transaction can read-for-update, update, or delete those rows until this transaction commits.

`SKIP LOCKED` modifies this: instead of *waiting* for a locked row (which would serialize all workers behind one another), the engine skips any row already locked by another transaction and takes the next unlocked row.

The result: N workers calling `dequeue()` concurrently each get a *different* set of tasks. Worker A locks tasks 1-16. Worker B skips tasks 1-16 and locks tasks 17-32. No blocking. No double-execution. This is the entire point of Phase 2.

---

## Chapter 2 Solutions — Querying and Joins

### E2.1

```
a) INNER JOIN — every order has a partner (relationship is required); you want only matched rows
b) LEFT JOIN  — you want ALL partners, even those with no orders; partners without orders get NULL for order columns
c) INNER JOIN — "tasks that HAVE an outbox entry" — only matched rows make sense
d) LEFT JOIN  — "tasks WITH THEIR dead-letter reason" — most tasks don't have one, you still want to see them
```

**The decision rule:** If the right side is optional ("if it exists, show it; if not, still show the left row"), use LEFT JOIN. If both sides must be present for the result to be meaningful, use INNER JOIN.

---

### E2.2

```
Setup: 3 delivery partners, 2 have orders (1 order each), 1 has no orders

Query A: SELECT COUNT(*)
LEFT JOIN produces 3 rows (2 matched, 1 with NULLs)
COUNT(*) counts ALL rows including the NULL row
Result: 3

Query B: SELECT COUNT(o.id)
LEFT JOIN produces 3 rows (2 matched, 1 with NULLs)
COUNT(o.id) counts non-NULL values of o.id
For the partner with no orders, o.id is NULL — not counted
Result: 2
```

**Rule:** When using `COUNT` with a `LEFT JOIN`, use `COUNT(right_table.pk)` to count matches, not `COUNT(*)`.

---

### E2.3

```sql
-- ORIGINAL (wrong): WHERE filters out NULL rows from LEFT JOIN, converting it to INNER JOIN
SELECT dp.name, COUNT(o.id) AS express_count
FROM delivery_partners dp
LEFT JOIN orders o ON o.delivery_partner_id = dp.id
WHERE o.order_type = 'express'  -- NULL rows fail this condition → dropped → INNER JOIN behavior
GROUP BY dp.id, dp.name;

-- FIX A: Move the filter to the ON clause (true LEFT JOIN)
SELECT dp.name, COUNT(o.id) AS express_count
FROM delivery_partners dp
LEFT JOIN orders o ON  o.delivery_partner_id = dp.id
                   AND o.order_type = 'express'  -- filter applied during join, not after
GROUP BY dp.id, dp.name;
-- Partners with no express orders appear with express_count = 0

-- FIX B: Use COALESCE to make the 0 explicit
SELECT dp.name, COALESCE(COUNT(o.id), 0) AS express_count
FROM delivery_partners dp
LEFT JOIN orders o ON o.delivery_partner_id = dp.id AND o.order_type = 'express'
GROUP BY dp.id, dp.name;
```

**The rule:** Filters on the right table of a LEFT JOIN belong in the `ON` clause, not the `WHERE` clause. `WHERE` filters are applied after the JOIN, at which point the NULL rows from LEFT JOIN are eliminated.

---

### E2.4

```sql
SELECT 
    t.id,
    t.type,
    t.status,
    CASE WHEN ob.aggregate_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_outbox,
    CASE WHEN dl.task_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_dead_letter
FROM tasks t
LEFT JOIN outbox ob    ON ob.aggregate_id = t.id
LEFT JOIN dead_letter dl ON dl.task_id = t.id
ORDER BY t.created_at DESC;

-- Cleaner with BOOLEAN cast:
SELECT 
    t.id,
    t.type,
    t.status,
    (ob.aggregate_id IS NOT NULL) AS has_outbox,
    (dl.task_id IS NOT NULL)      AS has_dead_letter
FROM tasks t
LEFT JOIN outbox ob    ON ob.aggregate_id = t.id
LEFT JOIN dead_letter dl ON dl.task_id = t.id;
```

**Note:** If a task has multiple outbox entries (e.g., the relay published it multiple times due to a bug), this query will return duplicate rows for that task. To avoid that, you would need `EXISTS` or a `DISTINCT ON`.

---

### E2.5

```sql
-- 7-step translation:
-- 1. FROM: delivery_partners JOIN orders
-- 2. WHERE: city = 'Mumbai', order_type = 'express', last 30 days, delivered
-- 3. GROUP BY: delivery_partner
-- 4. HAVING: (none)
-- 5. SELECT: name, count, total cost
-- 6. ORDER BY: count DESC
-- 7. LIMIT: 5

SELECT 
    dp.id,
    dp.name,
    COUNT(o.id)           AS express_deliveries,
    SUM(o.delivery_cost)  AS total_cost_earned
FROM delivery_partners dp
INNER JOIN orders o 
    ON  o.delivery_partner_id = dp.id
    AND o.order_type           = 'express'
    AND o.status               = 'delivered'
    AND o.delivered_at        >= NOW() - INTERVAL '30 days'
WHERE dp.city = 'Mumbai'
GROUP BY dp.id, dp.name
ORDER BY express_deliveries DESC
LIMIT 5;
```

**Follow-up — with total cost ranking instead of count:**
Change `ORDER BY express_deliveries DESC` to `ORDER BY total_cost_earned DESC`.

---

### E2.6

```sql
-- Original with IN:
SELECT * FROM tasks
WHERE id IN (SELECT aggregate_id FROM outbox WHERE published_at IS NULL);

-- Rewritten with EXISTS:
SELECT * FROM tasks t
WHERE EXISTS (
    SELECT 1 FROM outbox ob 
    WHERE ob.aggregate_id = t.id 
      AND ob.published_at IS NULL
);
```

**Which performs better?**

For large result sets, `EXISTS` is often faster because:
1. It short-circuits — stops as soon as the first matching row is found
2. It handles NULLs correctly (if `aggregate_id` could be NULL, `IN` would break)
3. The `IN` subquery must fully materialize its result (all matching aggregate_ids) before comparing

For this specific query where `aggregate_id` is a UUID foreign key (no NULLs), the performance difference may be negligible — the planner often rewrites `IN` as an equivalent join. But `EXISTS` is safer and more expressive.

---

### E2.7

**Approach A has the N+1 problem.** With N=1000 tasks:
- Application makes 1 query to get 1000 tasks
- For each task, application makes 1 query to get the handler name
- Total: 1 + 1000 = 1001 database queries

**Approach B:** 1 query, joining both tables. 1 database query total.

```sql
-- FAILED tasks from last 24 hours with dead-letter reason (if exists)
SELECT 
    t.id,
    t.type,
    t.status,
    t.attempts,
    t.max_attempts,
    t.last_error,
    t.updated_at,
    dl.reason AS dead_letter_reason,
    dl.failed_at AS dead_lettered_at
FROM tasks t
LEFT JOIN dead_letter dl ON dl.task_id = t.id
WHERE t.status IN ('FAILED', 'DEAD')
  AND t.updated_at >= NOW() - INTERVAL '24 hours'
ORDER BY t.updated_at DESC;
```

Uses LEFT JOIN because most FAILED tasks may not have a dead-letter entry (they failed but retries aren't exhausted yet, so they're in RETRYING → FAILED state).

---

## Chapter 3 Solutions — Aggregations and Analytics

### E3.1

```sql
SELECT 
    type,
    COUNT(*)                                                          AS total_tasks,
    COUNT(CASE WHEN status = 'PENDING'   THEN 1 END)                  AS pending,
    COUNT(CASE WHEN status = 'SUCCEEDED' THEN 1 END)                  AS succeeded,
    COUNT(CASE WHEN status IN ('FAILED', 'DEAD') THEN 1 END)          AS failed_or_dead,
    ROUND(AVG(attempts), 2)                                           AS avg_attempts
FROM tasks
GROUP BY type
ORDER BY total_tasks DESC;
```

---

### E3.2

```sql
SELECT 
    order_type,
    COUNT(*)                                                               AS total_orders,
    COUNT(CASE WHEN status = 'delivered'  THEN 1 END)                     AS delivered_count,
    COUNT(CASE WHEN status = 'cancelled'  THEN 1 END)                     AS cancelled_count,
    ROUND(
        COUNT(CASE WHEN status = 'delivered' THEN 1 END) * 100.0 
        / NULLIF(COUNT(*), 0), 
    2)                                                                     AS delivery_rate_pct
FROM orders
GROUP BY order_type
ORDER BY delivery_rate_pct DESC;
```

---

### E3.3

```sql
-- Version A: HAVING (most readable, semantically correct)
SELECT type, COUNT(*) AS failed_count
FROM tasks
WHERE status = 'FAILED'
GROUP BY type
HAVING COUNT(*) > 50
ORDER BY failed_count DESC;

-- Version B: CTE + WHERE on outer query
WITH failed_counts AS (
    SELECT type, COUNT(*) AS failed_count
    FROM tasks
    WHERE status = 'FAILED'
    GROUP BY type
)
SELECT type, failed_count
FROM failed_counts
WHERE failed_count > 50
ORDER BY failed_count DESC;
```

**Readability:** Version A is more concise. Version B is more debuggable (you can run the CTE alone first).

**Performance:** Both generate the same execution plan in Postgres. The planner sees through both and applies the filter at the most efficient point. `HAVING` is not inherently slower than the subquery approach when filtering on aggregate results.

---

### E3.4

```sql
WITH partner_deliveries AS (
    SELECT 
        dp.id,
        dp.name,
        dp.city,
        COUNT(o.id) AS delivery_count
    FROM delivery_partners dp
    LEFT JOIN orders o 
        ON  o.delivery_partner_id = dp.id
        AND o.status = 'delivered'
    GROUP BY dp.id, dp.name, dp.city
),
ranked AS (
    SELECT 
        *,
        DENSE_RANK() OVER (
            PARTITION BY city
            ORDER BY delivery_count DESC
        ) AS city_rank
    FROM partner_deliveries
)
SELECT city, name, delivery_count, city_rank
FROM ranked
WHERE city_rank <= 3
ORDER BY city, city_rank;
```

**Why DENSE_RANK not ROW_NUMBER?** If two partners in Delhi both have 45 deliveries, `ROW_NUMBER` arbitrarily picks one as rank 2 and the other as rank 3 — you get exactly 3 results but one tied partner is excluded. `DENSE_RANK` gives them both rank 2, includes both, and there is no rank 3 (goes 1, 2, 2, 4). The requirement says "top 3" — if two partners are tied for third, you should include both.

---

### E3.5

```sql
SELECT 
    DATE(created_at)                                          AS day,
    COUNT(*)                                                  AS daily_submitted,
    SUM(COUNT(*)) OVER (ORDER BY DATE(created_at))            AS cumulative_submitted
FROM tasks
GROUP BY DATE(created_at)
ORDER BY day;
```

**The nested aggregate explained:** `COUNT(*)` is the GROUP BY aggregate that produces one count per day. `SUM(COUNT(*)) OVER (ORDER BY ...)` applies a window function to those per-day counts, summing them cumulatively. The window `ORDER BY DATE(created_at)` means "sum from the earliest day up to and including the current row's day."

---

### E3.6

```sql
SELECT 
    type                                                                      AS handler_type,
    COUNT(*)                                                                  AS total_tasks,
    COUNT(CASE WHEN status = 'SUCCEEDED'              THEN 1 END)             AS succeeded,
    COUNT(CASE WHEN status IN ('FAILED', 'DEAD')      THEN 1 END)             AS failed_or_dead,
    ROUND(
        COUNT(CASE WHEN status IN ('FAILED', 'DEAD') THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
    2)                                                                        AS failure_rate_pct,
    ROUND(
        AVG(CASE WHEN status IN ('FAILED', 'DEAD') THEN attempts END),
    2)                                                                        AS avg_attempts_on_failure
FROM tasks
WHERE created_at >= NOW() - INTERVAL '24 hours'
GROUP BY type
ORDER BY failure_rate_pct DESC NULLS LAST;
```

`ORDER BY failure_rate_pct DESC NULLS LAST` — when a handler has zero tasks, `failure_rate_pct` is NULL (from NULLIF). `NULLS LAST` pushes these to the end of the result instead of the beginning.

---

### E3.7

```sql
WITH partner_stats AS (
    SELECT 
        dp.id,
        dp.name,
        dp.city,
        COUNT(o.id)                                                            AS total_deliveries,
        COUNT(CASE WHEN o.order_type = 'express' THEN 1 END)                   AS express_count,
        ROUND(
            COUNT(CASE WHEN o.order_type = 'express' THEN 1 END) * 100.0 
            / NULLIF(COUNT(o.id), 0), 2
        )                                                                      AS express_pct,
        ROUND(AVG(o.delivery_cost), 2)                                         AS avg_delivery_cost
    FROM delivery_partners dp
    LEFT JOIN orders o 
        ON  o.delivery_partner_id = dp.id
        AND o.status = 'delivered'
    GROUP BY dp.id, dp.name, dp.city
    HAVING COUNT(o.id) >= 20
),
ranked AS (
    SELECT 
        *,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY total_deliveries DESC)   AS city_rank,
        AVG(total_deliveries) OVER (PARTITION BY city)                         AS city_avg_deliveries
    FROM partner_stats
)
SELECT 
    name,
    city,
    total_deliveries,
    express_count,
    express_pct,
    avg_delivery_cost,
    city_rank,
    CASE 
        WHEN total_deliveries >= city_avg_deliveries THEN 'above average'
        ELSE 'below average'
    END AS vs_city_average
FROM ranked
ORDER BY city, city_rank;
```

---

### E3.8

```sql
SELECT 
    DATE(created_at)                                                   AS day,
    COUNT(*)                                                           AS submitted,
    COUNT(CASE WHEN status = 'SUCCEEDED' THEN 1 END)                   AS completed,
    COUNT(CASE WHEN status IN ('FAILED', 'DEAD') THEN 1 END)           AS failed,
    LAG(COUNT(*)) OVER (ORDER BY DATE(created_at))                     AS prev_day_submitted,
    COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE(created_at))          AS day_over_day_change
FROM tasks
WHERE created_at >= NOW() - INTERVAL '7 days'
GROUP BY DATE(created_at)
ORDER BY day;
```

**The nested window in aggregate:** `LAG(COUNT(*)) OVER (ORDER BY DATE(created_at))` — `COUNT(*)` is evaluated for each day by GROUP BY, and then `LAG` is the window function operating on those per-day counts. Window functions are applied after GROUP BY.

---

## Chapter 4 Solutions — Indexes and Performance

### E4.1

```
a) WHERE id = '...'
   → Primary key is already indexed (UUID PK). No additional index needed. Index scan on PK.

b) WHERE status = 'PENDING' ORDER BY created_at ASC
   → Composite index on (status, created_at ASC) would help.
   → Or a partial index on (created_at ASC) WHERE status = 'PENDING' if PENDING is the only queried status.

c) WHERE lower(type) = 'email'
   → Regular index on (type) will NOT be used — the function call breaks index lookup.
   → Need an expression index: CREATE INDEX ON tasks (lower(type));

d) WHERE attempts > 3
   → Index on (attempts) might help if few rows have attempts > 3 (high selectivity).
   → If most rows have attempts > 0 (low selectivity), Seq Scan may be faster.

e) SELECT COUNT(*) FROM tasks (no WHERE)
   → Index cannot help — you need to count all rows.
   → Options: accept slow query, use pg_class for approximate count, or maintain a counter table.
```

---

### E4.2

Given index: `(city, order_type, delivered_at DESC)`

```
a) WHERE city = 'Delhi'
   → USED (leftmost column constrained) — partial index scan on city='Delhi' entries

b) WHERE order_type = 'express'
   → NOT USED (leftmost column city is not constrained — B-tree is sorted by city first)

c) WHERE city = 'Delhi' AND order_type = 'express'
   → USED (both leftmost columns constrained)

d) WHERE city = 'Delhi' ORDER BY delivered_at DESC
   → PARTIALLY USED — city prefix reduces rows, but delivered_at sort requires order_type 
      to be constrained first (B-tree sorted city → order_type → delivered_at). 
      The planner may use index for city filter but still sort.

e) WHERE city = 'Delhi' AND order_type = 'express' ORDER BY delivered_at DESC
   → FULLY USED — all three columns used in the index order. Index provides both 
      filtering and ordering — no sort step needed.
```

---

### E4.3

```
Full index on (priority DESC, created_at ASC):
- Contains ALL 10M rows
- Size: approximately 300-500MB depending on tuple overhead
- Dequeue query: index scan finds runnable rows, but 9.9M index entries are useless
- Every status transition (PENDING→RUNNING→SUCCEEDED) causes 3 index writes

Partial index on (priority DESC, created_at ASC) WHERE status IN ('PENDING','SCHEDULED','RETRYING'):
- Contains ONLY the ~100,000 runnable rows
- Size: approximately 3-5MB (100x smaller)
- Dequeue query: entire B-tree fits in RAM → cache-resident → sub-millisecond scans
- Status transitions OUT of runnable states remove entries from index (cheap)
- INSERT of new PENDING tasks adds entries (cheap)

CHOOSE: Partial index, always.
```

The partial index is 100x smaller and stays cache-resident. The dequeue query runs every 200ms × N workers. At 8 workers, that is 40 dequeue queries per second. On a full index, those 40 queries are 40 sequential scans of a 300MB structure. On the partial index, they hit a 5MB in-RAM B-tree. The performance difference is measured in orders of magnitude.

---

### E4.4

```
1. Seq Scan on tasks
   → The planner chose to read the ENTIRE tasks table.
   → No index was used (or usable) for the WHERE clause.
   → For 1M rows, this is inherently slow.

2. Rows Removed by Filter: 995000
   → 995,000 rows were read and thrown away.
   → Only 5,000 rows matched the WHERE condition.
   → This is extremely low selectivity — the index would have been 200x more efficient.
   → Root cause: no index on (status + scheduled_at), or index exists but statistics 
      led planner to choose Seq Scan incorrectly.

3. Sort Method: external merge Disk: 12048kB
   → The sort spilled to disk because the result set (5000 rows × 200 bytes = ~1MB) 
      exceeded work_mem.
   → An index on (priority DESC, created_at ASC) would eliminate the Sort step entirely.

4. Most impactful single change:
   CREATE INDEX idx_tasks_lease
       ON tasks (priority DESC, created_at ASC)
       WHERE status IN ('PENDING', 'SCHEDULED', 'RETRYING');
   (partial index — eliminates Seq Scan AND Sort in one index)

5. After fix, EXPLAIN should show:
   - Index Scan using idx_tasks_lease (Seq Scan → Index Scan)
   - No Sort step (index already provides the ORDER BY order)
   - Rows Removed by Filter: ~0 (index only contains matching rows)
   - Execution time: < 1ms vs current 145ms
```

---

### E4.5

```sql
-- Query A: dequeue (partial composite index — covers WHERE and ORDER BY)
CREATE INDEX idx_tasks_lease
    ON tasks (priority DESC, created_at ASC)
    WHERE status IN ('PENDING', 'SCHEDULED', 'RETRYING');
-- The partial WHERE matches the query's WHERE exactly.
-- The ORDER BY matches the index column order exactly.
-- LIMIT 16 + SKIP LOCKED means the engine stops after 16 index entries.

-- Query B: ops dashboard GROUP BY status
CREATE INDEX idx_tasks_status ON tasks (status);
-- Enables index scan for WHERE status = '...' queries.
-- COUNT(*) per status can sometimes use index-only scan if covering index.

-- Query C: find by idempotency key
CREATE UNIQUE INDEX uq_tasks_idem
    ON tasks (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
-- Unique + partial: enforces uniqueness, used by ON CONFLICT clause.
-- Partial: NULL idempotency_key rows not indexed (most tasks have no key).

-- Query D: stuck task reaper (UPDATE WHERE status='RUNNING' AND updated_at < ...)
CREATE INDEX idx_tasks_stuck
    ON tasks (updated_at ASC)
    WHERE status = 'RUNNING';
-- Partial index on RUNNING rows only (usually very few rows).
-- updated_at range scan is fast on this tiny partial index.

-- Query E: outbox relay (published_at IS NULL)
CREATE INDEX idx_outbox_unpublished
    ON outbox (created_at ASC)
    WHERE published_at IS NULL;
-- Partial: only unpublished rows. Once published, row leaves the index.
-- ORDER BY created_at covered by index (process in submission order).
-- FOR UPDATE SKIP LOCKED works with the index scan.
```

---

### E4.6

**Diagnosis:**
- 50M rows, query scans all rows from last 30 days (potentially millions)
- `DATE_TRUNC('day', created_at)` is a function call — prevents index use on `created_at`
- `GROUP BY` on two columns across millions of rows is expensive
- Runs every 30 seconds — constant load on the table

**Solution 1: Expression index**
```sql
CREATE INDEX idx_tasks_day_type 
    ON tasks (DATE_TRUNC('day', created_at), type);
-- Tradeoff: index updates on every INSERT. Faster reads for this specific query.
```

**Solution 2: Partial index with time range**
```sql
CREATE INDEX idx_tasks_recent 
    ON tasks (created_at, type, status)
    WHERE created_at >= NOW() - INTERVAL '31 days';
-- Problem: partial index condition uses NOW() — Postgres doesn't support this directly.
-- Alternative: use a scheduled job to rebuild the index periodically.
```

**Solution 3: Materialized view (best for dashboard queries)**
```sql
CREATE MATERIALIZED VIEW daily_task_stats AS
SELECT 
    DATE_TRUNC('day', created_at) AS day,
    type,
    COUNT(*) AS total,
    COUNT(CASE WHEN status = 'SUCCEEDED' THEN 1 END) AS succeeded
FROM tasks
WHERE created_at >= NOW() - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', created_at), type;

CREATE INDEX ON daily_task_stats (day DESC, type);

-- Refresh every 5 minutes (not every query):
REFRESH MATERIALIZED VIEW CONCURRENTLY daily_task_stats;
```

**Tradeoffs:**
- Expression index: simple, always fresh, ~2x slower writes
- Materialized view: 4-second query becomes <1ms; data is stale by up to 5 minutes; adds operational complexity (refresh job)
- For a dashboard refreshing every 30 seconds, stale-by-5-minutes is acceptable → materialized view wins

---

### E4.7

```
1. Why 2.8M dead tuples for 50K live rows?
   Each task goes through multiple status transitions:
   PENDING → RUNNING → SUCCEEDED (2 UPDATEs = 2 dead tuples per task)
   + RETRYING cycles: RUNNING → RETRYING → PENDING → RUNNING → SUCCEEDED (4+ dead tuples)
   10,000 tasks/hour × 24 hours × 7 days = 1.68M tasks processed
   At average 1.5 dead tuples per task: ~2.5M dead tuples. Matches observation.

2. Why hasn't autovacuum kept up?
   Default autovacuum triggers when dead tuples > 20% of live tuples.
   At 50K live tuples, trigger threshold = 10K dead tuples.
   With 10K tasks/hour, you cross the threshold every ~6 minutes.
   But autovacuum has a sleep between runs and multiple tables compete for vacuum workers.
   For a high-churn table, autovacuum needs to be tuned specifically.

3. Performance consequences:
   - Index bloat: dead tuples remain in B-tree pages, making indexes larger and slower
   - Table bloat: heap pages fill with dead rows, Seq Scan reads more pages
   - Planner statistics skew: stale stats lead to poor plan choices
   - Potential transaction ID wraparound (if extreme — emergency vacuum kicks in, halts all writes)

4. Fixes:

   IMMEDIATE: manual vacuum
   VACUUM ANALYZE tasks;

   SHORT-TERM: tune autovacuum for this table
   ALTER TABLE tasks SET (
       autovacuum_vacuum_scale_factor = 0.01,   -- trigger at 1% dead tuples (vs 20% default)
       autovacuum_vacuum_cost_delay = 2          -- run faster (less pause between vacuum pages)
   );

   LONG-TERM: reduce dead tuples at source
   ALTER TABLE tasks SET (fillfactor = 70);     -- leave 30% space per page for HOT updates
   VACUUM FULL tasks;                            -- one-time compaction (locks table — do in maintenance window)
   
   HOT updates (Heap Only Tuple): if an UPDATE only changes columns not covered by any index,
   Postgres can reuse the same heap page without updating the index. fillfactor=70 makes this 
   more likely by leaving space in each page for the new version.
```

---

## Stretch Challenge Solutions

### S1 — Retry chain

```sql
-- Requires an attempt_history table (add to schema):
CREATE TABLE task_attempt_history (
    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id     UUID         NOT NULL REFERENCES tasks(id),
    attempt_num INT          NOT NULL,
    status      TEXT         NOT NULL,
    error       TEXT,
    started_at  TIMESTAMPTZ  NOT NULL,
    ended_at    TIMESTAMPTZ
);

-- Query: full retry chain for a task
SELECT 
    attempt_num,
    status,
    error,
    started_at,
    ended_at,
    ended_at - started_at                           AS duration,
    started_at - LAG(ended_at) OVER (ORDER BY attempt_num) AS gap_since_last_attempt
FROM task_attempt_history
WHERE task_id = ?
ORDER BY attempt_num;
```

### S2 — Multi-tenant schema

```sql
CREATE TABLE tenants (
    id          UUID  PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT  NOT NULL,
    rate_limit  INT   NOT NULL DEFAULT 100  -- max tasks per minute
);

-- Add tenant_id to tasks
ALTER TABLE tasks ADD COLUMN tenant_id UUID NOT NULL REFERENCES tenants(id);

-- Row-level security for tenant isolation
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tasks
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

-- Updated indexes (include tenant_id in partial index for dequeue isolation)
CREATE INDEX idx_tasks_tenant_lease
    ON tasks (tenant_id, priority DESC, created_at ASC)
    WHERE status IN ('PENDING', 'SCHEDULED', 'RETRYING');

-- Tenant throughput dashboard
SELECT 
    t.name AS tenant_name,
    COUNT(tk.id)                                           AS total_tasks_24h,
    COUNT(CASE WHEN tk.status='SUCCEEDED' THEN 1 END)      AS succeeded,
    ROUND(COUNT(tk.id) / 24.0, 1)                         AS avg_tasks_per_hour
FROM tenants t
LEFT JOIN tasks tk ON tk.tenant_id = t.id AND tk.created_at >= NOW() - INTERVAL '24 hours'
GROUP BY t.id, t.name
ORDER BY total_tasks_24h DESC;
```

### S3 — Weekly delivery awards

```sql
WITH base_stats AS (
    SELECT 
        dp.id,
        dp.name,
        dp.city,
        dp.rating,
        COUNT(o.id)                                                               AS total_deliveries,
        COUNT(CASE WHEN o.order_type='express' THEN 1 END)                        AS express_deliveries,
        ROUND(
            COUNT(CASE WHEN o.order_type='express' THEN 1 END) * 100.0 
            / NULLIF(COUNT(o.id), 0), 2
        )                                                                         AS express_pct
    FROM delivery_partners dp
    LEFT JOIN orders o 
        ON  o.delivery_partner_id = dp.id
        AND o.status = 'delivered'
        AND o.delivered_at >= NOW() - INTERVAL '7 days'
    GROUP BY dp.id, dp.name, dp.city, dp.rating
),
city_leaders AS (
    SELECT id, name, city, total_deliveries,
           DENSE_RANK() OVER (PARTITION BY city ORDER BY total_deliveries DESC) AS rnk
    FROM base_stats
),
express_leaders AS (
    SELECT id, name, city, express_pct,
           DENSE_RANK() OVER (ORDER BY express_pct DESC) AS rnk
    FROM base_stats
    WHERE total_deliveries >= 20
),
rating_leaders AS (
    SELECT id, name, city, rating,
           DENSE_RANK() OVER (ORDER BY rating DESC) AS rnk
    FROM base_stats
    WHERE rating IS NOT NULL
)
SELECT 'Top Deliveries in City' AS category, name, city, 
       total_deliveries::TEXT AS metric
FROM city_leaders WHERE rnk = 1

UNION ALL

SELECT 'Top Express Rate', name, city, 
       express_pct::TEXT || '%'
FROM express_leaders WHERE rnk = 1

UNION ALL

SELECT 'Top Rating', name, city, 
       rating::TEXT
FROM rating_leaders WHERE rnk = 1

ORDER BY category, name;
```
