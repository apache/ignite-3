---
title: SQL Conformance
sidebar_label: SQL Conformance
---

{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}

Apache Ignite supports most of the major features of ANSI SQL 2016 standard out-of-the-box. The following table shows Apache Ignite compliance to the aforementioned standard.

| Feature ID | Feature Name | Subfeature ID | Subfeature Name | Supported | Limitations |
|---|---|---|---|---|---|
| E011 | Numeric data types | E011 | | Yes | |
| E011 | Numeric data types | E011-01 | INTEGER and SMALLINT data types | Yes | |
| E011 | Numeric data types | E011-02 | REAL, DOUBLE PRECISION, and FLOAT data types | Yes | |
| E011 | Numeric data types | E011-03 | DECIMAL and NUMERIC data types | Yes | DEC and NUMERIC types are not supported |
| E011 | Numeric data types | E011-04 | Arithmetic operators | Yes | |
| E011 | Numeric data types | E011-05 | Numeric comparison | Yes | |
| E011 | Numeric data types | E011-06 | Implicit casting among the numeric data types | Yes | |
| E021 | Character data types | E021 | | Yes | CHARACTER type cannot be used in table definition |
| E021 | Character string types | E021-01 | CHARACTER data type | Yes | |
| E021 | Character string types | E021-02 | CHARACTER VARYING data type | Yes | |
| E021 | Character string types | E021-03 | Character literals | Yes | |
| E021 | Character string types | E021-04 | CHARACTER_LENGTH function | Yes | |
| E021 | Character string types | E021-05 | OCTET_LENGTH function | Yes | |
| E021 | Character string types | E021-06 | SUBSTRING function | Yes | |
| E021 | Character string types | E021-07 | Character concatenation | Yes | |
| E021 | Character string types | E021-08 | UPPER and LOWER functions | Yes | |
| E021 | Character string types | E021-09 | TRIM function | Yes | |
| E021 | Character string types | E021-10 | Implicit casting among the character string types | Yes | |
| E021 | Character string types | E021-11 | POSITION function | Yes | |
| E021 | Character string types | E021-12 | Character comparison | Yes | |
| E031 | Identifiers | E031 | | Yes | |
| E031 | Identifiers | E031-01 | Delimited identifiers | Yes | |
| E031 | Identifiers | E031-02 | Lower case identifiers | Yes | |
| E031 | Identifiers | E031-03 | Trailing underscore | Yes | |
| E051 | Basic query specification | E051 | | Yes | |
| E051 | Basic query specification | E051-01 | SELECT DISTINCT | Yes | |
| E051 | Basic query specification | E051-02 | GROUP BY clause | Yes | Supports GROUPING SETS. ROLLUP and CUBE are not supported |
| E051 | Basic query specification | E051-04 | GROUP BY can contain columns not in select list | Yes | |
| E051 | Basic query specification | E051-05 | Select list items can be renamed | Yes | |
| E051 | Basic query specification | E051-06 | HAVING clause | Yes | |
| E051 | Basic query specification | E051-07 | Qualified * in select list | Yes | |
| E051 | Basic query specification | E051-08 | Correlation names in the FROM clause | Yes | |
| E051 | Basic query specification | E051-09 | Rename columns in the FROM clause | Yes | |
| E061 | Basic predicates and search conditions | E061 | | Yes | |
| E061 | Basic predicates and search conditions | E061-01 | Comparison predicate | Yes | |
| E061 | Basic predicates and search conditions | E061-02 | BETWEEN predicate | Yes | |
| E061 | Basic predicates and search conditions | E061-03 | IN predicate with list of values | Yes | |
| E061 | Basic predicates and search conditions | E061-04 | LIKE predicate | Yes | |
| E061 | Basic predicates and search conditions | E061-05 | LIKE predicate ESCAPE clause | Yes | |
| E061 | Basic predicates and search conditions | E061-06 | NULL predicate | Yes | |
| E061 | Basic predicates and search conditions | E061-07 | Quantified comparison predicate | Yes | |
| E061 | Basic predicates and search conditions | E061-08 | EXISTS predicate | Yes | |
| E061 | Basic predicates and search conditions | E061-09 | Subqueries in comparison predicate | Yes | |
| E061 | Basic predicates and search conditions | E061-11 | Subqueries in IN predicate | Yes | |
| E061 | Basic predicates and search conditions | E061-12 | Subqueries in quantified comparison predicate | Yes | |
| E061 | Basic predicates and search conditions | E061-13 | Correlated subqueries | Yes | |
| E061 | Basic predicates and search conditions | E061-14 | Search condition | Yes | |
| E071 | Basic query expressions | E071 | | Yes | |
| E071 | Basic query expressions | E071-01 | UNION DISTINCT table operator | Yes | |
| E071 | Basic query expressions | E071-02 | UNION ALL table operator | Yes | |
| E071 | Basic query expressions | E071-03 | EXCEPT DISTINCT table operator | Yes | |
| E071 | Basic query expressions | E071-05 | Columns combined via table operators need not have exactly the same data type | Yes | |
| E071 | Basic query expressions | E071-06 | Table operators in subqueries | Yes | |
| E081 | Basic Privileges | E081 | | No | |
| E081 | Basic Privileges | E081-01 | SELECT privilege | No | |
| E081 | Basic Privileges | E081-02 | DELETE privilege | No | |
| E081 | Basic Privileges | E081-03 | INSERT privilege at the table level | No | |
| E081 | Basic Privileges | E081-04 | UPDATE privilege at the table level | No | |
| E091 | Set functions | E091 | | Yes | |
| E091 | Set functions | E091-01 | AVG | Yes | |
| E091 | Set functions | E091-02 | COUNT | Yes | |
| E091 | Set functions | E091-03 | MAX | Yes | |
| E091 | Set functions | E091-04 | MIN | Yes | |
| E091 | Set functions | E091-05 | SUM | Yes | |
| E091 | Set functions | E091-06 | ALL quantifier | Yes | |
| E091 | Set functions | E091-07 | DISTINCT quantifier | Yes | |
| E101 | Basic data manipulation | E101 | | Yes | |
| E101 | Basic data manipulation | E101-01 | INSERT statement | Yes | |
| E101 | Basic data manipulation | E101-03 | Searched UPDATE statement | Yes | |
| E101 | Basic data manipulation | E101-04 | Searched DELETE statement | Yes | |
| E111 | Single row SELECT statement | E111 | | Yes | |
| E131 | Null value support (nulls in lieu of values) | E131 | | Yes | |
| E141 | Basic integrity constraints | E141 | | Partially | NOT NULL and PRIMARY KEY constraints. |
| E141 | Basic integrity constraints | E141-01 | NOT NULL constraints | Yes | |
| E141 | Basic integrity constraints | E141-03 | PRIMARY KEY constraints | Yes | |
| E141 | Basic integrity constraints | E141-07 | Column defaults | Partially | Only literals and RAND_UUID function |
| E141 | Basic integrity constraints | E141-08 | NOT NULL inferred on PRIMARY KEY | Yes | |
| E151 | Transaction support | E151 | | Partially | |
| E151 | Transaction support | E151-01 | COMMIT statement | Partially | Only in SQL scripts. No options. |
| E151 | Transaction support | E151-02 | ROLLBACK statement | Partially | Only in SQL scripts. No options. Savepoints are not supported |
| E153 | Updatable queries with subqueries | E153 | | Yes | |
| E161 | SQL comments using leading double minus | E161 | | Yes | |
| E171 | SQLSTATE support | E171 | | No | |
| F031 | Basic schema manipulation | F031 | | Partially | CREATE TABLE, ALTER TABLE, DROP TABLE |
| F031 | Basic schema manipulation | F031-01 | CREATE TABLE statement to create persistent base tables | Partially | CREATE TABLE must always specify primary key |
| F031 | Basic schema manipulation | F031-03 | GRANT statement | No | |
| F031 | Basic schema manipulation | F031-04 | ALTER TABLE statement: ADD COLUMN clause | Yes | |
| F033 | ALTER TABLE statement: DROP COLUMN clause | F033 | | Partially | DROP behaviour is not supported |
| F041 | Basic joined table | F041 | | Yes | |
| F041 | Basic joined table | F041-01 | Inner join (but not necessarily the INNER keyword) | Yes | |
| F041 | Basic joined table | F041-02 | INNER keyword | Yes | |
| F041 | Basic joined table | F041-03 | LEFT OUTER JOIN | Yes | |
| F041 | Basic joined table | F041-04 | RIGHT OUTER JOIN | Yes | |
| F041 | Basic joined table | F041-05 | Outer joins can be nested | Yes | |
| F041 | Basic joined table | F041-07 | The inner table in a left or right outer join can also be used in an inner join | Yes | |
| F041 | Basic joined table | F041-08 | All comparison operators are supported (rather than just =) | Yes | |
| F051 | Basic date and time | F051 | | Yes | |
| F051 | Basic date and time | F051-01 | DATE data type (including support of DATE literal) | Yes | |
| F051 | Basic date and time | F051-02 | TIME data type (including support of TIME literal) with fractional seconds precision of at least 0 | Partially | TIME WITH TIME ZONE type is not supported. Does not support sub-ms precision |
| F051 | Basic date and time | F051-03 | TIMESTAMP data type (including support of TIMESTAMP literal) with fractional seconds precision of at least 0 and 6 | Partially | TIMESTAMP WITH TIME ZONE is not supported. Does not support sub-ms precision |
| F051 | Basic date and time | F051-04 | Comparison predicate on DATE, TIME, and TIMESTAMP data types | Yes | |
| F051 | Basic date and time | F051-05 | Explicit CAST between datetime types and character string types | Yes | |
| F051 | Basic date and time | F051-06 | CURRENT_DATE | Yes | |
| F051 | Basic date and time | F051-07 | LOCALTIME | Yes | |
| F051 | Basic date and time | F051-08 | LOCALTIMESTAMP | Yes | |
| F052 | Intervals and datetime arithmetic | F052 | | Yes | |
| F171 | Multiple schemas per user | F171 | | Yes | |
| F201 | CAST function | F201 | | Yes | |
| F221 | Explicit defaults | F221 | | Yes | |
| F261 | CASE expression | F261 | | Yes | |
| F261 | CASE expression | F261-01 | Simple CASE | Yes | |
| F261 | CASE expression | F261-02 | Searched CASE | Yes | |
| F261 | CASE expression | F261-03 | NULLIF | Yes | |
| F261 | CASE expression | F261-04 | COALESCE | Yes | |
| F302 | INTERSECT table operator | F302 | | Yes | |
| F302 | INTERSECT table operator | F302-01 | INTERSECT DISTINCT table operator | Yes | |
| F302 | INTERSECT table operator | F302-02 | INTERSECT ALL table operator | Yes | |
| F304 | EXCEPT ALL table operator | F304 | | Yes | |
| F311 | Schema definition statement | F311 | | Partially | |
| F311 | Schema definition statement | F311-01 | CREATE SCHEMA | Yes | Schema elements are not supported |
| F381 | Extended schema manipulation | F381-01 | ALTER TABLE statement: ALTER COLUMN clause | Partially | Default can not be set to non-constant in most cases. See DDL docs |
| F391 | Long identifiers | F391 | | Yes | Up to 128 characters |
| F392 | Unicode escapes in identifiers | F392 | | Partially | Partial support of unicode escapes |
| F401 | Extended joined table | F401 | | Yes | |
| F401 | Extended joined table | F401-01 | NATURAL JOIN | Yes | |
| F401 | Extended joined table | F401-02 | FULL OUTER JOIN | Yes | |
| F401 | Extended joined table | F401-04 | CROSS JOIN | Yes | |
| F404 | Range variable for common column names | F404 | | Yes | |
| F411 | Time zone specification | F411 | | Yes | |
| F471 | Scalar subquery values | F471 | | Yes | |
| F561 | Full value expressions | F561 | | Yes | |
| F571 | Truth value tests | F571 | | Partially | UNKNOWN is not supported |
| F591 | Derived tables | F591 | | Yes | |
| F661 | Simple tables | F661 | | Yes | |
| F781 | Self-referencing operations | F781 | | Yes | |
| F850 | Top-level order by clause in query expression | F850 | | Yes | |
| F851 | order by clause in subqueries | F851 | | Yes | |
| F855 | Nested order by clause in query expression | F855 | | Yes | |
| F861 | Top-level result offset clause in query expression | F861 | | Yes | |
| F862 | result offset clause in subqueries | F862 | | Yes | |
| F863 | Nested result offset clause in query expression | F863 | | Yes | |
| T021 | BINARY and VARBINARY data types | T021 | | Yes | BINARY type cannot be used in table definition |
| T031 | BOOLEAN data type | T031 | | Yes | |
| T071 | BIGINT data type | T071 | | Yes | |
| T121 | WITH (excluding RECURSIVE) in query expression | T121 | | Yes | |
| T122 | WITH (excluding RECURSIVE) in subquery | T122 | | Yes | |
| T141 | SIMILAR predicate | T141 | | Yes | |
| T151 | DISTINCT predicate | T151 | | Yes | |
| T152 | DISTINCT predicate with negation | T152 | | Yes | |
| T285 | Enhanced derived column names | T285 | | Yes | |
| T312 | OVERLAY function | T312 | | Yes | |
| T351 | Bracketed SQL comments (/*...*/ comments) | T351 | | Yes | |
| T434 | GROUP BY DISTINCT | T434 | | Yes | |
| T441 | ABS and MOD functions | T441 | | Yes | |
| T501 | Enhanced EXISTS predicate | T501 | | Yes | |
| T551 | Optional key words for default syntax | T551 | | Yes | |
| T621 | Enhanced numeric functions | T621 | | Yes | |
| T622 | Trigonometric functions | T622 | | Yes | |
| T623 | General logarithm functions | T623 | | Yes | |
| T624 | Common logarithm functions | T624 | | Yes | |
| T631 | IN predicate with one list element | T631 | | Yes | |
| T828 | JSON_QUERY | T828 | | Yes | |
| T829 | JSON_QUERY: array wrapper options | T829 | | Yes | |
| T839 | Formatted cast of datetimes to/from character strings | T839 | | Yes | |

## Proposed Alternatives for Unsupported Features

Apache Ignite provides alternative solutions for some unsupported features, listed below:

| Feature ID | Feature Name | Subfeature ID | Subfeature Name | Alternative |
|---|---|---|---|---|
| E171 | SQLSTATE support | E171 | | JDBC error codes, ODBC error codes |
