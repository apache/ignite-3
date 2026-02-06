---
title: CLI Configuration Parameters
sidebar_label: CLI Configuration
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

## CLI Configuration Parameters

```bash
ignite.jdbc.key-store.path=
ignite.cluster-endpoint-url=http://localhost:10300
ignite.jdbc.client-auth=
ignite.rest.key-store.password=
ignite.jdbc.key-store.password=
ignite.cli.sql.multiline=true
ignite.cli.syntax-highlighting=true
ignite.rest.trust-store.path=
ignite.jdbc.trust-store.password=
ignite.auth.basic.username=
ignite.jdbc-url=jdbc:ignite:thin://127.0.0.1:10800
ignite.rest.key-store.path=
ignite.rest.trust-store.password=
ignite.jdbc.trust-store.path=
ignite.auth.basic.password=
```

| Property | Default | Description |
|----------|---------|-------------|
| ignite.jdbc.key-store.path |  | Path to the JDBC keystore file. |
| ignite.cluster-endpoint-url | http://127.0.1.1:10300 |  |
| ignite.jdbc.client-auth |  | If JDBC client authorization is enabled in CLI. |
| ignite.rest.key-store.password |  |  |
| ignite.jdbc.key-store.password |  |  |
| ignite.cli.sql.multiline | true | Enables multiline input mode for SQL commands in the CLI. |
| ignite.cli.syntax-highlighting | true | Enables syntax highlighting in CLI output. |
| ignite.rest.trust-store.path |  |  |
| ignite.jdbc.trust-store.password |  |  |
| ignite.auth.basic.username |  |  |
| ignite.jdbc-url | jdbc:ignite:thin://127.0.0.1:10800 |  |
| ignite.rest.key-store.path |  |  |
| ignite.rest.trust-store.password |  |  |
| ignite.jdbc.trust-store.path |  |  |
| ignite.auth.basic.password |  |  |

## Configuration Profiles

Apache Ignite [CLI](../../ignite-cli-tool.md#interactive-cli-mode) supports configuration profiles to manage different sets of settings.

Use the following commands to create and manage profiles:

- Create a new configuration profile:

```bash
cli config create <profile_name>
```

- Switch to an existing profile:

```bash
cli config activate <profile_name>
```

- Display all available profiles:

```bash
cli config list
```

- Display the currently used profile with all custom settings:

```bash
cli config show
```

Each profile stores its own CLI-specific settings, allowing isolated configurations for different use cases.
