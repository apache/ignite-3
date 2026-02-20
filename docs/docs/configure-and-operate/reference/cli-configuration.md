---
id: cli-configuration
title: CLI Configuration Parameters
sidebar_label: CLI Configuration
---

## CLI Configuration Parameters

The Apache Ignite CLI supports various configuration parameters:

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
|---|---|---|
| ignite.jdbc.key-store.path | | Path to the JDBC keystore file. |
| ignite.cluster-endpoint-url | http://127.0.1.1:10300 | Default cluster endpoint URL. |
| ignite.jdbc.client-auth | | If JDBC client authorization is enabled in CLI. |
| ignite.rest.key-store.password | | REST keystore password. |
| ignite.jdbc.key-store.password | | JDBC keystore password. |
| ignite.cli.sql.multiline | true | Enables multiline input mode for SQL commands in the CLI. |
| ignite.cli.syntax-highlighting | true | Enables syntax highlighting in CLI output. |
| ignite.rest.trust-store.path | | REST truststore path. |
| ignite.jdbc.trust-store.password | | JDBC truststore password. |
| ignite.auth.basic.username | | Basic authentication username. |
| ignite.jdbc-url | jdbc:ignite:thin://127.0.0.1:10800 | Default JDBC URL. |
| ignite.rest.key-store.path | | REST keystore path. |
| ignite.rest.trust-store.password | | REST truststore password. |
| ignite.jdbc.trust-store.path | | JDBC truststore path. |
| ignite.auth.basic.password | | Basic authentication password. |

## Configuration Profiles

Apache Ignite [CLI](/3.1.0/tools/cli-commands#interactive-cli-mode) supports configuration profiles to manage different sets of settings.

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
