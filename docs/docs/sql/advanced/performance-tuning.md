---
title: Performance Tuning
sidebar_position: 3
---

# SQL Performance Tuning

## Optimizer Hints

The query optimizer tries to execute the fastest execution plan. However, you can know about the data design, application design or data distribution in your cluster better. SQL hints can help the optimizer to make optimizations more rationally or build execution plan faster.

:::note
SQL hints are optional to apply and might be skipped in some cases.
:::

### Hints format

SQL hints are defined by a special comment `/*+ HINT */`, referred to as a _hint block_. Spaces before and after the
hint name are required. The hint block must be placed right after the operator. Several hints for one relation operator are not supported.

Example:

```sql
SELECT /*+ NO_INDEX */ T1.* FROM TBL1 where T1.V1=? and T1.V2=?
```

#### Hint parameters

Hint parameters, if required, are placed in brackets after the hint name and separated by commas.

The hint parameter can be quoted. Quoted parameter is case-sensitive. The quoted and unquoted parameters cannot be
defined for the same hint.

Example:

```sql
SELECT /*+ FORCE_INDEX(TBL1_IDX2,TBL2_IDX1) */ T1.V1, T2.V1 FROM TBL1 T1, TBL2 T2 WHERE T1.V1 = T2.V1 AND T1.V2 > ? AND T2.V2 > ?;

SELECT /*+ FORCE_INDEX('TBL2_idx1') */ T1.V1, T2.V1 FROM TBL1 T1, TBL2 T2 WHERE T1.V1 = T2.V1 AND T1.V2 > ? AND T2.V2 > ?;
```

### Hints errors

The optimizer tries to apply every hint and its parameters, if possible. But it skips the hint or hint parameter if:

* The hint is not supported.
* Required hint parameters are not passed.
* The hint parameters have been passed, but the hint does not support any parameter.
* The hint parameter is incorrect or refers to a nonexistent object, such as a nonexistent index or table.
* The current hints or current parameters are incompatible with the previous ones, such as forcing the use and disabling of the same index.

If a `FORCE_INDEX` hint references an index that does not exist, the following error will be thrown:

```java
Hints mentioned indexes "IDX_NOT_FOUND1", "IDX_NOT_FOUND2" were not found.
```

### Supported hints

#### FORCE_INDEX / NO_INDEX

Forces or disables index scan.

##### Parameters:

* Empty. To force an index scan for every underlying table. Optimizer will choose any available index. Or to disable all indexes.
* Single index name to use or skip exactly this index.
* Several index names. They can relate to different tables. The optimizer will choose indexes for scanning or skip them all.

##### Examples:

```sql
SELECT /*+ FORCE_INDEX */ T1.* FROM TBL1 T1 WHERE T1.V1 = T2.V1 AND T1.V2 > ?;

SELECT /*+ FORCE_INDEX(TBL1_IDX2, TBL2_IDX1) */ T1.V1, T2.V1 FROM TBL1 T1, TBL2 T2 WHERE T1.V1 = T2.V1 AND T1.V2 > ? AND T2.V2 > ?;

SELECT /*+ NO_INDEX */ T1.* FROM TBL1 T1 WHERE T1.V1 = T2.V1 AND T1.V2 > ?;

SELECT /*+ NO_INDEX(TBL1_IDX2, TBL2_IDX1) */ T1.V1, T2.V1 FROM TBL1 T1, TBL2 T2 WHERE T1.V1 = T2.V1 AND T1.V2 > ? AND T2.V2 > ?;
```

:::note
The query cannot have both `FORCE_INDEX` and `NO_INDEX` hints at the same time.
:::

## Using EXPLAIN Statement

### EXPLAIN PLAN FOR Statement

Apache Ignite supports the [`EXPLAIN PLAN FOR`](/3.1.0/sql/reference/data-types-and-functions/operational-commands) statement that can be used to read the execution plan of a query.

Use this command to analyse your queries for possible optimization, for example:

```sql
EXPLAIN PLAN FOR SELECT name FROM Person WHERE age = 26;
```

Here is how the results of the explanation may look like:

```text
╔═══════════════════════════════╗
║ PLAN                          ║
╠═══════════════════════════════╣
║ Exchange                      ║
║     distribution: single      ║
║     est. row count: 333000    ║
║   TableScan                   ║
║       table: [PUBLIC, PERSON] ║
║       filters: =(AGE, 26)     ║
║       fields: [$f0]           ║
║       projects: [NAME]        ║
║       est. row count: 333000  ║
╚═══════════════════════════════╝
```

### EXPLAIN MAPPING FOR Statement

Apache Ignite supports the [`EXPLAIN MAPPING FOR`](/3.1.0/sql/reference/data-types-and-functions/operational-commands) statement that can be used to track how the query is split and what nodes the subqueries are executed on.

Use this command if you need an insight into how the query is broken down and executed across multiple nodes in the distributed cluster.

```sql
EXPLAIN MAPPING FOR SELECT name FROM Person WHERE age = 26;
```

Here is how the results of the query may look like:

```text
╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║ PLAN                                                                                                                                                                                              ║
╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣
║ Fragment#0 root                                                                                                                                                                                   ║
║   executionNodes: [defaultNode]                                                                                                                                                                   ║
║   remoteFragments: [1]                                                                                                                                                                            ║
║   exchangeSourceNodes: {1=[defaultNode]}                                                                                                                                                          ║
║   tree:                                                                                                                                                                                           ║
║     Receiver(sourceFragment=1, exchange=1, distribution=single)                                                                                                                                   ║
║                                                                                                                                                                                                   ║
║ Fragment#1                                                                                                                                                                                        ║
║   targetNodes: [defaultNode]                                                                                                                                                                      ║
║   executionNodes: [defaultNode]                                                                                                                                                                   ║
║   tables: [PERSON]                                                                                                                                                                                ║
║   partitions: {defaultNode=[0:12, 1:12, 2:12, 3:12, 4:12, 5:12, 6:12, 7:12, 8:12, 9:12, 10:12, 11:12, 12:12, 13:12, 14:12, 15:12, 16:12, 17:12, 18:12, 19:12, 20:12, 21:12, 22:12, 23:12, 24:12]} ║
║   tree:                                                                                                                                                                                           ║
║     Sender(targetFragment=0, exchange=1, distribution=single)                                                                                                                                     ║
║       TableScan(name=PUBLIC.PERSON, source=2, partitions=25, distribution=random)                                                                                                                 ║
╚═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
```

## Query Batching

Apache Ignite handles batched requests faster than individual requests, so we recommend using multi-statement execution when possible.

When executing multiple queries in a single call, similar requests are automatically batched together. When writing large scripts that perform multiple different kinds of operations, we recommend the following order:

- All required [DDL operations](/3.1.0/sql/reference/language-definition/ddl);
- Assigning [access permissions](/3.1.0/sql/reference/data-types-and-functions/operational-commands);
- Loading data into the tables.

As execution of each statement is considered complete when the first page is ready to be returned, when working with large data sets, `SELECT` statements may be affected by later statements in the same script.

## Performance Consideration For Correlated Subqueries

Apache Ignite supports correlated subqueries, but the performance of certain complex correlated subqueries may be insufficient, especially when used in high-volume transactional or analytical workloads.

### What Are Correlated Subqueries

A correlated subquery is a subquery that depends on values from the outer query for execution. It is evaluated once for every row of the outer query.

For example, for a schema that is defined in the following way:

```sql
CREATE TABLE projects (id INT PRIMARY KEY, name VARCHAR);
CREATE TABLE employees (id INT PRIMARY KEY, department_id INT, name VARCHAR, salary DECIMAl);
CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR);
CREATE TABLE assignments (project_id INT, employee_id INT, PRIMARY KEY (project_id, employee_id));
```

The correlated subquery may look like this:

```sql
SELECT e.name,
       (SELECT COUNT(*)
          FROM assignments a
         WHERE a.employee_id = e.id
       ) AS project_count
FROM employees e;
```

Here, the subquery references `e.id` from the outer query, meaning it's re-evaluated for every employee row, leading to N separate subquery executions for N employees.

### Performance Impact

In Apache Ignite 3, repeated subquery executions are not automatically optimized. As a result:

- Scalar subqueries may become bottlenecks.
- Even small tables can cause high CPU and memory consumption when repeatedly queried.
- Certain queries may perform slower than expected.

### Improving Performance

In general, highly-selective outer queries with cheap scalar subqueries (like single-row index lookup) will perform just fine. Here is an example:

```sql
-- This query returns an employee along with the name of the department they belong to.
-- It uses a correlated scalar subquery to resolve the department name.
--
-- Note the predicate `e.id = ?`, which filters by the employee's primary key.
-- This makes the outer query highly selective -- typically returning only a single row.
--
-- Because the subquery is evaluated only once (or a very small number of times),
-- using a correlated scalar subquery is safe and has negligible performance impact
-- in this case. There's no need to rewrite it using a join.
SELECT e.*,
       (SELECT name
          FROM departments
         WHERE id = e.department_id
       ) AS employees_department
  FROM employees e
 WHERE e.id = ?;
```

Similar query but without predicate may result in lower performance. If the query with predicate finishes in `0.007s`, similar query without predicate could take up to `2.4s`.

Here is another example:

```sql
-- This query returns all employees along with the name of the department they
-- belong to.
SELECT e.*,
       (SELECT name
          FROM departments
         WHERE id = e.department_id
       ) AS employees_department
  FROM employees e;
```

Query like the one above may easily be rewritten with regular `JOIN`:

```sql
-- Equivalent query to the previous example, but uses a `LEFT JOIN` instead of a
-- correlated subquery. This rewrite is valid as long as the subquery in the original
-- version would return at most one row.
--
-- If multiple rows exist in the `departments` table for the same `id`, the original
-- scalar subquery would result in a runtime error (due to a non-scalar result), while
-- the join version would produce duplicated rows in the output.
--
-- In our case, `departments.id` is a primary key, so the join is safe and will return
-- at most one matching department per employee.
--
-- A `LEFT JOIN` is used to ensure that employees with no matching department are still
-- returned. If it's guaranteed that every employee has a valid department reference,
-- an `INNER JOIN` may be used instead, which is slightly more efficient.
SELECT e.*,
       d.name AS employees_department
  FROM employees e
  LEFT JOIN departments d ON d.id = e.department_id;
```

Rewritten query on the same environment finishes significantly faster.

### Examples of Improved Queries

The first example shows how you can correctly query the database without evaluating each row:

```sql
-- This query returns all employees without assigned projects.
--
-- Finishes in 3.2s (assuming there is an index on `assignments(employee_id)`;
-- without the index, execution time increases significantly -- up to 12s).
SELECT e.id, e.name
FROM employees e
WHERE NOT EXISTS (
    SELECT 1
    FROM assignments a
    WHERE a.employee_id = e.id
);

-- Equivalent query without correlated subqueries.
-- Instead of evaluating a subquery for each row, we join the tables and compute
-- the number of assignments using aggregation. It is important to include all
-- columns that form a unique key from the outer table in the `GROUP BY` clause.
-- Otherwise, multiple rows may be grouped together incorrectly, potentially
-- affecting the result. If you're unsure about the uniqueness of specific columns,
-- include all columns from the table's `PRIMARY KEY`.
--
-- A `LEFT JOIN` is used because we want to retain employees even when there is
-- no matching assignment. An `INNER JOIN` would exclude those employees.
--
-- The `HAVING COUNT(a.employee_id) = 0` clause checks for the absence of matches.
-- You must count a column from the right-hand side of the join that is guaranteed
-- to be non-null. In this case, `a.employee_id` is suitable because the `JOIN`
-- condition (`a.employee_id = e.id`) ensures that only non-null `employee_id`s
-- are matched; nulls are excluded during the join phase.
--
-- Finishes in 0.04s.
SELECT e.id, e.name
  FROM employees e
  LEFT JOIN assignments a ON a.employee_id = e.id
 GROUP BY e.id, e.name
HAVING COUNT(a.employee_id) = 0;

-- Similar query, but returns only employees who have at least one project assigned.
-- Note the use of `INNER JOIN`: since we are only interested in employees with a
-- matching assignment, an inner join is both sufficient and more efficient in this case.
--
-- The `HAVING COUNT(a.employee_id) > 0` condition ensures that only employees
-- with one or more matching rows in the `assignments` table are returned.
-- As with the previous example, `a.employee_id` is safe to count because it cannot be null
-- due to the join condition (`a.employee_id = e.id`) filtering out nulls.
--
-- Finishes in 0.03s.
SELECT e.id, e.name
  FROM employees e
  JOIN assignments a ON a.employee_id = e.id
 GROUP BY e.id, e.name
HAVING COUNT(a.employee_id) > 0;
```

This example demonstrates drastic performance improvement you can gain by improving your queries:

```sql
-- This query returns all employees whose salary is the minimum within their department.
--
-- Finishes in 18s.
SELECT e.*
  FROM employees e
 WHERE e.salary = (SELECT MIN(salary) FROM employees WHERE department_id = e.department_id);

-- Equivalent query without a correlated subquery.
-- Instead of comparing each employee's salary with a scalar subquery result,
-- we precompute the minimum salary per department using a grouped subquery,
-- and then join it back to the employees table.
--
-- This rewrite is safe because:
--   - For each department, we compute the minimum salary exactly once.
--   - The join condition ensures we only return employees whose salary matches
--     the minimum salary for their department.
--   - No grouping is needed on the outer query because we're performing an equality match
--     on both `department_id` and the computed minimum salary.
--
-- This approach avoids per-row subquery evaluation and leverages set-based operations,
-- which are significantly faster.
--
-- Finishes in 0.02s.
SELECT e.*
  FROM employees e
  JOIN (
      SELECT department_id, MIN(salary) AS min_salary
        FROM employees
       GROUP BY department_id
  ) AS min_salaries_by_department
    ON e.department_id = min_salaries_by_department.department_id
   AND e.salary = min_salaries_by_department.min_salary;
```

## Dropping Cached Plans

:::warning
This is an experimental API.
:::

As optimizing the query plan is a resource-intensive operation, Apache Ignite caches the plan and reuses it for subsequent related queries. As data is updated, the plan may be outdated and require recalculation. By default, the plans expire after the period specified in the  `ignite.planner.planCacheExpiresAfterSeconds` parameter (1800 seconds by default).

To force the update earlier, you can use the `sql planner invalidate-cache` CLI tool command.

```text
sql planner invalidate-cache --tables=PUBLIC.Person
```
