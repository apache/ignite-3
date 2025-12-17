---
id: engine-architecture
title: SQL Engine Architecture
sidebar_label: Engine Architecture
---

# SQL Engine Architecture

Apache Ignite 3 uses Apache Calcite as an SQL engine of choice. Apache Calcite is a dynamic data management framework, which mainly serves for mediating between applications and one or more data storage locations and data processing engines. For more information on Apache Calcite, see the [Calcite documentation](https://calcite.apache.org/docs/).

Apache Ignite 3 SQL engine has the following advantages:

- **SQL Optimized for Distributed Environments**: Apache Ignite 3 distributed queries are not limited to a single map-reduce phase, allowing more complex data gathering.
- **Transactional SQL**: All tables in Apache Ignite 3 support SQL transactions with transactional guarantees.
- **Cluster-wide System Views**: [System views](/3.1.0/configure-and-operate/monitoring/metrics-system-views) in Apache Ignite 3 provide cluster-wide information, dynamically updated.
- **Multi-Index Queries**: With Apache Ignite 3, you can perform queries that use multiple indexes at the same time to speed up queries.
- **Standard-Adherent SQL**: Apache Ignite 3 SQL closely adheres to modern SQL standard.
- **Improved optimization algorithms**: SQL in Apache Ignite 3 optimizes queries by repeatedly applying planner rules to a relational expression.
- **High overall performance**: Apache Ignite 3 offers high levels of execution flexibility, as well as high efficiency in terms of both memory and CPU consumption.
