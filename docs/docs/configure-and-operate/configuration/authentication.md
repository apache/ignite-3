---
id: config-authentication
title: Authentication
sidebar_label: Authentication
---

Apache Ignite 3 provides basic Authentication capabilities.

## Authentication Configuration

### Basic Authentication

To start using basic authentication on the cluster, you need to enable it and create an initial administrator user. By default, the role that grants administrator permissions is called `admin`, but you can change it in cluster configuration.

Here is an example of configuration that initializes the cluster and enables security on it:

- Prepare cluster configuration file with security configuration:

```hocon
ignite {
  security {
    enabled:true
    authentication {
      providers=[
        {
          name=default
          type=basic
          users=[
            {
              displayName=administrator
              password="ignite"
              roles=[
                system
              ]
              username=ignite
            }
          ]
        }
      ]
    }
  }
}
```

- Initialize the cluster with the security configuration:

```shell
cluster init --name=sampleCluster --config-files=/cluster-config.conf
```

When the cluster has been initialized, it has basic authorization configured for `ignite` user name and `ignite` password with system level access. However, by default security is disabled. To enable it:

```shell
cluster config update ignite.security.enabled=true
```

:::warning
If you lose access to all accounts with system role, you will lose administrator access to the cluster.
:::

After authorization is enabled, you will be disconnected from the cluster and must reconnect to the cluster:

```shell
connect http://127.0.0.1:10300 --username ignite --password ignite
```

You can change the password for the default user by updating cluster configuration, for example:

```shell
cluster config update  ignite.security.authentication.providers.default.users.ignite.password=myPass
```
