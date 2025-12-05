---
id: using-explain
title: Using EXPLAIN Command
sidebar_position: 1
---

# How to Improve Queries With EXPLAIN Command

The SQL `EXPLAIN` command is a powerful tool used to analyze and understand the execution plan of a query without actually executing it.

When you use the EXPLAIN command, it returns the query execution plan, which includes details such as:

- The order in which tables are accessed.
- The type of join operations used (for example, nested loops, hash joins, or merge joins).
- Any indexes that are used to speed up data retrieval.
- Estimated costs and row counts for different parts of the query.

This information is crucial for optimizing query performance, identifying bottlenecks, and making informed decisions about database schema design and indexing strategies.

## EXPLAIN Command Syntax

Apache Ignite supports two variations of the `EXPLAIN` command:

```sql
EXPLAIN [PLAN | MAPPING] FOR <query>
```

If neither `PLAN` nor `MAPPING` is specified, then `PLAN` is implicit.

Parameters:

- `PLAN` - explains query in terms of relational operators tree. This representation is suitable for investigation of performance issues related to the optimizer.
- `MAPPING` - explains query in terms of mapping of query fragment to a particular node of the cluster. This representation is suitable for investigation of performance issues related to the data colocation.

Examples:

```sql
EXPLAIN SELECT * FROM lineitem;
EXPLAIN PLAN FOR SELECT * FROM lineitem;
EXPLAIN MAPPING FOR SELECT * FROM lineitem;
```

## Understanding The Output

Each query plan is represented as a tree-like structure composed of relational operators.

A node in the plan includes:

- A **name**, indicating the relational operator (e.g., `TableScan`, `IndexScan`, `Sort`, `Join` types)
- A set of **attributes**, relevant to that specific operator

```text
OperatorName
    attribute1: value1
    attribute2: value2
```

### Operator Naming

The operator name reflects the specific algorithm or strategy used. For example:

- `TableScan` – Full scan of a base table.
- `IndexScan` – Access via index, possibly sorted.
- `Sort` – Explicit sorting step.
- `HashJoin`, `MergeJoin`, `NestedLoopJoin` – Types of join algorithms.
- `Limit`, `Project`, `Exchange` – Execution-related transformations and controls.

### Hierarchical Plan Structure

The plan is structured as a **tree**, where:

- **Leaf nodes** represent data sources (e.g., `TableScan`)
- **Internal nodes** represent data transformations (e.g., `Join`, `Sort`)
- **The root node** (topmost) is the final operator that produces the result

## Common Query Optimization Issues

SQL EXPLAIN output analysis can help you optimize slow query execution. You can avoid common bottlenecks in SQL execution by following these guidelines:

- Avoid scanning an entire table.
- Avoid scanning non-optimal indexes.
- Avoid suboptimal join ordering or join algorithm.
- Ensure optimal data colocation for your queries.

In the following sections, we will see some common issues with queries and ways to identify and fix them.

## Full Scan Instead of Index Scan

Suppose related sql execution flow looks like:

```sql
CREATE TABLE t (id INT PRIMARY KEY, col1 VARCHAR);
CREATE INDEX t_col1_idx ON t(col1);

SELECT id FROM t WHERE col1 = '1';
```

And possible EXPLAIN output:

```sql
   TableScan
       table: PUBLIC.T
       predicate: =(COL1, _UTF-8'1')
       fieldNames: [ID]
       est: (rows=1)
```

:::note
For simplicity, here and below, information that is not related to the example is omitted from the EXPLAIN output.
:::

We can see a full scan (*TableScan* operator) with predicate. The execution planner chooses which scan implementation (**TableScan** or **IndexScan**) to use. If you expect that index scan is preferable, you can use the `FORCE_INDEX` hint to manually force `IndexScan` approach:

```sql
SELECT /*+ FORCE_INDEX(t_col1_idx) */ id FROM t WHERE col1 = '1';
```

Will show a different plan, like:

```sql
   IndexScan
       table: PUBLIC.T
       index: T_COL1_IDX
       type: SORTED
       predicate: =(COL1, _UTF-8'1')
       searchBounds: [ExactBounds [bound=_UTF-8'1']]
       fieldNames: [ID]
       collation: []
       est: (rows=1)
```

## Suboptimal Indexes

Indexes with less prediction can be chosen, for example schema and query may look as follows:

```sql
CREATE TABLE t (id INT PRIMARY KEY, col1 VARCHAR, col2 VARCHAR);
CREATE INDEX t_col1_col2_idx ON T(col1, col2);
CREATE INDEX t_col1_idx ON t(col1);

SELECT id FROM t WHERE col1 = '1' AND col2 = '2';
```

and a possible plan would be:

```sql
   IndexScan
       table: PUBLIC.T
       index: T_COL1_IDX
       ...
```

We can see that the execution uses the **T_COL1_IDX** index, through both predicates **COL1 = '1' AND COL2 = '2'** are involved and the **T_COL1_COL2_IDX** is preferable. In this case, the optimal plan would be:

```sql
   IndexScan
       table: PUBLIC.T
       index: T_COL1_COL2_IDX
       ...
```

You can also use the `FORCE_INDEX` hint to achieve this:

```sql
SELECT /*+ FORCE_INDEX(t_col1_col2_idx) */ id FROM t WHERE col1 = '1' AND col2 = '2';
```

## Unexpected Sort Operation

By default, sorted indexes store their entries in ascending order. You can adjust the ordering of a sorted index by including the options ASC or DESC.

Let's suppose the schema and related query look like this:

```sql
CREATE TABLE t1 (id INT PRIMARY KEY, col1 VARCHAR);
CREATE TABLE t2 (id INT PRIMARY KEY, col1 VARCHAR);
CREATE INDEX t1_col1_desc_idx ON t1(col1 DESC);
CREATE INDEX t2_col1_idx ON t2(col1);

SELECT t1.id as t1id, t2.id as t2id FROM t1 JOIN t2 USING (col1);
```

And the possible execution plan looks like this:

```sql
   MergeJoin
         ...
       Sort
           collation: [COL1 ASC]
           ...
       IndexScan
           index: T2_COL1_IDX
           ...
```

In the example above, the planner adds the **Sort** operation before performing the **IndexScan**, as the index used is sorted in descending order, while ascending order is required.

Extra **Sort** operations adds performance costs, and we can avoid it by creating an index with the appropriate sort ordering:

```sql
CREATE INDEX t1_col1_idx ON t1(col1);
```

And plan will no longer display the **Sort** operation, improving query execution speed:

```sql
   MergeJoin
         ...
       IndexScan
           index: T1_COL1_IDX
           ...
       IndexScan
           index: T2_COL1_IDX
           ...
```

## Performance Impact of Correlated Subqueries

The SQL-99 standard allows for nested subqueries at nearly all places within a query, so Ignite 3 supports nested subqueries, both correlated and not. Performance of certain complex correlated subqueries may be insufficient. Let's consider a correlated query:

```sql
CREATE TABLE emp(dept_id INTEGER PRIMARY KEY, name VARCHAR, salary INTEGER);
CREATE TABLE dept(id INTEGER PRIMARY KEY, name VARCHAR);

SELECT emp.name, (SELECT dept.name FROM dept WHERE emp.dept_id=dept.id)
FROM emp
WHERE emp.salary > 1000;
```

We can see nested correlated subquery here, lets check the plan:

```sql
   CorrelatedNestedLoopJoin
         ...
       TableScan
           table: PUBLIC.EMP
           predicate: >(SALARY, 1000)
           ...
     ColocatedHashAggregate
         ...
         TableScan
             table: PUBLIC.DEPT
             predicate: =($cor1.DEPT_ID, ID)
             ...
```

The example above shows the slow **CorrelatedNestedLoopJoin** operation. Queries with this operation may cause a number of issues:

- Such subqueries may become bottlenecks.
- Queries can cause high CPU load.
- Certain queries may perform slower than expected.

If performance issues are found in similar queries, it would be more efficient to rewrite the query without nested subqueries, for example:

```sql
SELECT emp.name, dept.name
FROM emp, dept
WHERE emp.salary > 1000 AND emp.dept_id=dept.id;
```

And new plan becomes:

```sql
     HashJoin
         predicate: =(DEPT_ID, ID)
         ...
       TableScan
           table: PUBLIC.EMP
           predicate: >(SALARY, 1000)
           ...
         TableScan
             table: PUBLIC.DEPT
             ...
```

Without the **CorrelatedNestedLoopJoin** operation, the query should perform much better than the previous one.

## Excessive Sorting

Let's explain we have an index involved two columns one of them is participate in predicate and other in ordering, or in sql terms:

```sql
CREATE TABLE emp(dept_id INTEGER PRIMARY KEY, name VARCHAR, salary INTEGER);
CREATE INDEX emp_salary_name_idx ON emp(salary, name);

SELECT dept_id FROM emp WHERE salary = 1 ORDER BY name;
```

Expectations:

- Index need to be used here.
- No additional sort is needed because index is ordered by **name** column is satisfies initial query ordering.

But the real execution plan shows a different result:

```sql
     Sort
         collation: [NAME ASC]
         ...
       TableScan
           table: PUBLIC.EMP
           predicate: =(SALARY, 1)
           ...
```

We can see a redundant **Sort** operator. A bit query refactoring can help to avoid excessive sorting:

```sql
SELECT dept_id FROM emp WHERE salary = 1 ORDER BY salary, name;
```

And the plan becomes as follows:

```sql
     IndexScan
         table: PUBLIC.EMP
         index: EMP_SALARY_NAME_IDX
         predicate: =(SALARY, 1)
         ...
```

## Select Count Optimization

Some queries can be optimized to use more optimal plans which brings performance speed up. For example, plan for:

```sql
SELECT COUNT(*) FROM emp;
```

Can look like this:

```sql
 SelectCount
     table: PUBLIC.EMP
     est: (rows=43)
     ...
```

But there are numerous cases where such optimization is not applicable. In such a cases, a plan can be different and the execution may require more time.

The same query as above, but with explicit transaction may produce a different plan, for example:

```sql
   ReduceSortAggregate
       ...
       MapSortAggregate
           ...
         TableScan
             table: PUBLIC.EMP
             est: (rows=43)
             ...
```

## Index Scan Without Exact Search Bounds

Table scans are available in two implementations: direct table scan and scan through index. Index scans contain predicate and search bounds. Predicate provides final rows comparison. If search bounds are absent, the query degenerates into table scan through index scan (requiring an additional store look up), with further predicate comparison, that incurs additional performance overhead costs.

Let's suppose we have schema and query like this:

```sql
CREATE TABLE t (id INTEGER PRIMARY KEY, col1 DECIMAL(5, 3));
CREATE INDEX t_col1_idx ON t(col1);

SELECT id FROM t WHERE col1 = 43;
```

And possible plan would look like this:

```sql
   IndexScan
       table: PUBLIC.T
       index: T_COL1_IDX
       predicate: =(CAST(COL1):DECIMAL(13, 3), 43.000)
       ...
```

We can see here only **predicate** (and no **searchBounds**) which means that **all** rows from index will go through predicate and bring additional performance penalty.

Two type of solutions are possible here:

- You can prohibit suboptimal index usage.
- You can explicitly help the planner with type derivation.

### Prohibit Index Usage

For the first approach, use the **NO_INDEX** hint to prohibit index usage:

```sql
SELECT /*+ NO_INDEX */ id FROM t WHERE col1 = 43;

-- or with direct index mention:

SELECT /*+ NO_INDEX(t_col1_idx) */ id FROM t WHERE col1 = 43;
```

As a result, you will have a plan similar to this:

```sql
   TableScan
       table: PUBLIC.T
       predicate: =(CAST(COL1):DECIMAL(13, 3), 43.000)
       ...
```

### Manual Type Casting

You can append additional cast to the same query to explicitly cast data as a specific type:

```sql
SELECT id FROM t WHERE col1 = 43::DECIMAL(5, 3);
```

```sql
   IndexScan
       table: PUBLIC.T
       index: T_COL1_IDX
       predicate: =(COL1, 43.000)
       searchBounds: [ExactBounds [bound=43.000:DECIMAL(5, 3)]]
       ...
```

We can see here both **searchBounds** and **predicate** which means that only exact lookup through index will be involved.

The same case as above but for a bit complicated query:

```sql
CREATE TABLE t (id INT PRIMARY KEY, col1 INT);
CREATE INDEX t_col1_asc_idx ON t (col1);

SELECT * FROM t WHERE col1::varchar = SUBSTR(CURRENT_DATE::varchar, 4);
```

Possible plan:

```sql
   IndexScan
       table: PUBLIC.T
       index: T_COL1_IDX
       predicate: =(CAST(COL1):VARCHAR CHARACTER SET "UTF-8", SUBSTR(CAST(CURRENT_DATE):VARCHAR CHARACTER SET "UTF-8" NOT NULL, 4))
       ...
```

And we also can see that no **search bounds** are involved here.

Try to change it like:

```sql
SELECT * FROM t WHERE col1 = SUBSTR(CURRENT_DATE::varchar, 4)::int;
```

And the possible plan will become:

```sql
   IndexScan
       table: PUBLIC.T
       index: T_COL1_ASC_IDX
       predicate: =(COL1, CAST(SUBSTR(CAST(CURRENT_DATE):VARCHAR CHARACTER SET "UTF-8" NOT NULL, 4)):INTEGER NOT NULL)
       searchBounds: [ExactBounds [bound=CAST(SUBSTR(CAST(CURRENT_DATE):VARCHAR CHARACTER SET "UTF-8" NOT NULL, 4)):INTEGER]]
       ...
```

We can see that **searchBounds** are present, thus more productive execution flow is expected here.

## Colocation Usage

As mentioned above, right colocated columns choice plays a significant role in query execution performance. For example, if initially tables are created without any thoughts about further usage columns colocation you can have the following scenario:

```sql
-- by default, the table is implicitly colocated by PRIMARY KEY
CREATE TABLE emp(dept_id INTEGER, name VARCHAR, salary INTEGER, PRIMARY KEY(dept_id, name));

-- implicitly colocated by PRIMARY KEY
CREATE TABLE dept(id INTEGER, name VARCHAR, PRIMARY KEY(name, id));
```

And query as follows:

```sql
SELECT emp.name, dept.name FROM emp JOIN dept ON emp.dept_id = dept.id AND emp.salary > 1000;
```

Bring plan like:

```sql
   HashJoin
       predicate: =(DEPT_ID, ID)
       ...
     Exchange
         ...
       TableScan
           table: PUBLIC.EMP
           ...
     Exchange
         ...
       TableScan
           table: PUBLIC.DEPT
           ...
```

We can see two **Exchange** operators, which means that all rows are transferred into a single node and then are joined. This execution flow brings a performance cost and slows down query execution.

Let's try to improve it by adding explicit colocation for the **dept** table by the **ID** column:

```sql
-- implicitly colocated by PRIMARY KEY
CREATE TABLE emp(dept_id INTEGER, name VARCHAR, salary INTEGER, PRIMARY KEY(dept_id, name));
-- explicitly colocated by ID
CREATE TABLE dept(id INTEGER, name VARCHAR, PRIMARY KEY(name, id)) COLOCATE BY (id);
```

Now the dependent rows from **emp** table are transferred into the appropriate node where **dept** holds the rows according to **DEPT.ID** distribution:

```sql
     HashJoin
         predicate: =(DEPT_ID, ID)
         ...
       Exchange
           distribution: table PUBLIC.DEPT in zone "Default" by [DEPT_ID]
           ...
         TableScan
             table: PUBLIC.EMP
             ...
       TableScan
           table: PUBLIC.DEPT
           ...
```

Only one **Exchange** operator for now, which, once again, mean only rows transferring from **emp** table to appropriate **dept** one.

And finally, both join predicate related columns are colocated:

:::note
The following colocation example will only work if the **emp** and **dept** tables belong to the same distribution zone.
:::

```sql
-- explicitly colocated by DEPT_ID
CREATE TABLE emp(dept_id INTEGER, name VARCHAR, salary INTEGER, PRIMARY KEY(dept_id, name)) COLOCATE BY(dept_id);
-- explicitly colocated by ID
CREATE TABLE dept(id INTEGER, name VARCHAR, PRIMARY KEY(id, name)) COLOCATE BY(id);
```

Now, the **emp** and **dept** tables are both colocated.

And the final plan will look like this:

```sql
     HashJoin
        predicate: =(DEPT_ID, ID)
         ...
       TableScan
           table: PUBLIC.EMP
           ...
       TableScan
           table: PUBLIC.DEPT
           ...
```

No **Exchange** operators are involved in the explanation, which means that no excessive rows transfer has occurred.

## Additional EXPLAIN Examples

### Example: Complex Join Query

```sql
EXPLAIN PLAN FOR
 SELECT
      U.UserName, P.ProductName, R.ReviewText, R.Rating
   FROM Users U, Reviews R, Products P
  WHERE U.UserID = R.UserID
    AND R.ProductID = P.ProductID
    AND P.ProductName = 'Product_' || ?::varchar
```

The resulting output is:

```text
Project
    fieldNames: [USERNAME, PRODUCTNAME, REVIEWTEXT, RATING]
    projection: [USERNAME, PRODUCTNAME, REVIEWTEXT, RATING]
    est: (rows=16650)
  HashJoin
      predicate: =(USERID$0, USERID)
      fieldNames: [PRODUCTID, USERID, REVIEWTEXT, RATING, PRODUCTID$0, PRODUCTNAME, USERID$0, USERNAME]
      type: inner
      est: (rows=16650)
    HashJoin
        predicate: =(PRODUCTID, PRODUCTID$0)
        fieldNames: [PRODUCTID, USERID, REVIEWTEXT, RATING, PRODUCTID$0, PRODUCTNAME]
        type: inner
        est: (rows=16650)
      Exchange
          distribution: single
          est: (rows=50000)
        TableScan
            table: PUBLIC.REVIEWS
            fieldNames: [PRODUCTID, USERID, REVIEWTEXT, RATING]
            est: (rows=50000)
      Exchange
          distribution: single
          est: (rows=1665)
        TableScan
            table: PUBLIC.PRODUCTS
            predicate: =(PRODUCTNAME, ||(_UTF-8'Product_', CAST(?0):VARCHAR CHARACTER SET "UTF-8"))
            fieldNames: [PRODUCTID, PRODUCTNAME]
            est: (rows=1665)
    Exchange
        distribution: single
        est: (rows=10000)
      TableScan
          table: PUBLIC.USERS
          fieldNames: [USERID, USERNAME]
          est: (rows=10000)
```

This execution plan represents a query that joins three tables: `USERS`, `REVIEWS`, and `PRODUCTS`, and selects four fields after filtering by product name.

* **Project** (root node): Outputs the final selected fields (USERNAME, PRODUCTNAME, REVIEWTEXT, and RATING).
* **HashJoins** (two levels): Perform the inner joins.
  * The first (bottom-most) joins REVIEWS with PRODUCTS on PRODUCTID.
  * The second joins the result with USERS on USERID.
* **TableScans**: Each table is scanned:
  * REVIEWS is fully scanned.
  * PRODUCTS is scanned with a filter on PRODUCTNAME.
  * USERS is fully scanned.
* **Exchange** nodes: Indicate data redistribution between operators.

Each node includes:

- `fieldNames`: Output columns at that stage.
- `predicate`: Join or filter condition.
- `est`: Estimated number of rows at that point in the plan.

### Example: Query Mapping

A result of EXPLAIN MAPPING command includes additional metadata providing insight at how the query is mapped on cluster topology. So, for the command like below:

```sql
EXPLAIN MAPPING FOR
 SELECT
      U.UserName, P.ProductName, R.ReviewText, R.Rating
   FROM Users U, Reviews R, Products P
  WHERE U.UserID = R.UserID
    AND R.ProductID = P.ProductID
    AND P.ProductName = 'Product_' || ?::varchar
```

The resulting output is:

```text
Fragment#0 root
  distribution: single
  executionNodes: [node_1]
  tree:
    Project
        fieldNames: [USERNAME, PRODUCTNAME, REVIEWTEXT, RATING]
        projection: [USERNAME, PRODUCTNAME, REVIEWTEXT, RATING]
        est: (rows=1)
      HashJoin
          predicate: =(USERID$0, USERID)
          fieldNames: [PRODUCTID, USERID, REVIEWTEXT, RATING, PRODUCTID$0, PRODUCTNAME, USERID$0, USERNAME]
          type: inner
          est: (rows=1)
        HashJoin
            predicate: =(PRODUCTID, PRODUCTID$0)
            fieldNames: [PRODUCTID, USERID, REVIEWTEXT, RATING, PRODUCTID$0, PRODUCTNAME]
            type: inner
            est: (rows=1)
          Receiver
              fieldNames: [PRODUCTID, USERID, REVIEWTEXT, RATING]
              sourceFragmentId: 1
              est: (rows=1)
          Receiver
              fieldNames: [PRODUCTID, PRODUCTNAME]
              sourceFragmentId: 2
              est: (rows=1)
        Receiver
            fieldNames: [USERID, USERNAME]
            sourceFragmentId: 3
            est: (rows=1)

Fragment#1
  distribution: random
  executionNodes: [node_1, node_2, node_3]
  partitions: [REVIEWS=[node_1={0, 2, 5, 6, 7, 8, 9, 10, 12, 13, 20}, node_2={1, 3, 11, 19, 21, 22, 23, 24}, node_3={4, 14, 15, 16, 17, 18}]]
  tree:
    Sender
        distribution: single
        targetFragmentId: 0
        est: (rows=50000)
      TableScan
          table: PUBLIC.REVIEWS
          fieldNames: [PRODUCTID, USERID, REVIEWTEXT, RATING]
          est: (rows=50000)

Fragment#2
  distribution: table PUBLIC.PRODUCTS in zone "Default"
  executionNodes: [node_1, node_2, node_3]
  partitions: [PRODUCTS=[node_1={0, 2, 5, 6, 7, 8, 9, 10, 12, 13, 20}, node_2={1, 3, 11, 19, 21, 22, 23, 24}, node_3={4, 14, 15, 16, 17, 18}]]
  tree:
    Sender
        distribution: single
        targetFragmentId: 0
        est: (rows=1665)
      TableScan
          table: PUBLIC.PRODUCTS
          predicate: =(PRODUCTNAME, ||(_UTF-8'Product_', CAST(?0):VARCHAR CHARACTER SET "UTF-8"))
          fieldNames: [PRODUCTID, PRODUCTNAME]
          est: (rows=1665)

Fragment#3
  distribution: table PUBLIC.USERS in zone "Default"
  executionNodes: [node_1, node_2, node_3]
  partitions: [USERS=[node_1={0, 2, 5, 6, 7, 8, 9, 10, 12, 13, 20}, node_2={1, 3, 11, 19, 21, 22, 23, 24}, node_3={4, 14, 15, 16, 17, 18}]]
  tree:
    Sender
        distribution: single
        targetFragmentId: 0
        est: (rows=10000)
      TableScan
          table: PUBLIC.USERS
          fieldNames: [USERID, USERNAME]
          est: (rows=10000)
```

Where:

- **Fragment#0** means fragment with id=0
- A **root** marks a fragment which is considered as root fragment, i.e. a fragment which represents user's cursor
- A **distribution** attribute provides an insight into which mapping strategy was applied to this particular fragment
- A **executionNodes** attribute provides a list of nodes this fragment will be executed on
- A **partitions** attribute provides an insight into which partitions of which tables will be read from which nodes
- A **tree** attribute specifies which part of the relational tree corresponds to this fragment

The output above shows how the query is broken into multiple execution fragments and distributed across the cluster. It gives insight into both the logical execution plan and how it maps to the physical topology.

The query starts execution in *Fragment#0*, which serves as the root of the plan (this is where the final result is produced). It runs on a single node (`node_1`) and contains the main logic of the query, including the projection and two nested hash joins. Instead of scanning tables directly, it receives data from other fragments through `Receiver` operators. These incoming streams correspond to the `REVIEWS`, `PRODUCTS`, and `USERS` tables.

The actual table scans happen in *Fragments 1 through 3*, each responsible for one of the involved tables. These fragments operate in parallel across the cluster. Each performs a scan on its respective table and then sends the results back to Fragment#0.

- *Fragment#1* handles the `REVIEWS` table. It runs on all nodes and uses a random distribution strategy. Data is partitioned across nodes, and after scanning the table, results are sent upstream.
- *Fragment#2* is in charge of the `PRODUCTS` table. It also spans all nodes but follows a zone-based distribution linked to the table's partitioning. There's a filter applied to `PRODUCTNAME`, which limits the amount of data sent to the root.
- *Fragment#3* covers the `USERS` table. Like the others, it's distributed and reads from table partitions spread across the cluster.

Each fragment includes metadata such as the nodes it's executed on, how data is partitioned, and how results are sent between fragments. This layout provides a clear view of not only how the query is logically processed, but also how the workload is split and coordinated in a distributed environment.
