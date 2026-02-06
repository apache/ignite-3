---
id: colocation
title: Data Colocation
sidebar_label: Colocation
---

In many cases you may want to store related data on the same node. This way multi-entry queries do not need to pull data from other nodes and are thus executed faster.

When the table is created, you can choose the key that will be used to colocate data.

For example, if you have `Person` and `Company` objects, and each person has the companyId field that indicates the company the person works for. By specifying the `Person.companyId` and `Company.ID` as colocation keys, you ensure that all the persons working for the same company are stored on the same node, where the company object is stored as well. Queries that request persons working for a specific company are processed on a single node.

## Configuring Colocation Key

Data colocation is configured during table creation by using the `COLOCATE BY` clause. The columns used to colocate data must be in the primary key and must be specified in the same order as the `PRIMARY KEY` of the main table.

For example, the table below will colocate data for people based on the `city_id` column:

```sql
CREATE TABLE IF NOT EXISTS Person (
  id int,
  city_id int primary key,
  name varchar,
  age int,
  company varchar
) COLOCATE BY (city_id)
```

When using composite primary keys, you can specify multiple columns to colocate data by:

```sql
CREATE TABLE Company (
  company_id int,
  department_id int,
  city_id int,
  company_name timestamp,
  PRIMARY KEY (company_id, city_id)
)

CREATE TABLE IF NOT EXISTS Person (
  id int,
  city_id int,
  name varchar,
  age int,
  company_id int,
  PRIMARY KEY (id, company_id, city_id)
)
COLOCATE BY (company_id, city_id)
```

In this case, Ignite will try to colocate these tables together for storage.

:::note
The `COLOCATE BY` clause of colocated table (`Person` table in the example above) must contain the same set of columns and in the same order as the `PRIMARY KEY` clause of the main table (Company table in the example above) to colocate the data.
:::
