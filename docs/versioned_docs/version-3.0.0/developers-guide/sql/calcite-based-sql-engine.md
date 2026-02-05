---
title: Introduction
sidebar_label: Introduction
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

Apache Ignite 3 uses Apache Calcite as an SQL engine of choice. Apache Calcite is a dynamic data management framework, which mainly serves for mediating between applications and one or more data storage locations and data processing engines. For more information on Apache Calcite, see the [Calcite documentation](https://calcite.apache.org/docs/).

Apache Ignite 3 SQL engine has the following advantages:

* **SQL Optimized for Distributed Environments**: Apache Ignite 3 distributed queries are not limited to a single map-reduce phase, allowing more complex data gathering;
* **Transactional SQL**: All tables in Apache Ignite 3 support SQL transactions with transactional guarantees;
* **Cluster-wide System Views**: [System views](../../administrators-guide/metrics/system-views.md) in Apache Ignite 3 provide cluster-wide information, dynamically updated;
* **Multi-Index Queries**: With Apache Ignite 3, you can perform queries that use multiple indexes at the same time to speed up queries;
* **Standard-Adherent SQL**: Apache Ignite 3 SQL closely adheres to modern SQL standard;
* **Improved optimization algorithms**: SQL in Apache Ignite 3 optimizes queries by repeatedly applying planner rules to a relational expression;
* **High overall performance**: Apache Ignite 3 offers high levels of execution flexibility, as well as high efficiency in terms of both memory and CPU consumption.
