---
title: SSL/TLS
sidebar_label: SSL/TLS
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

# SSL/TLS

This page explains how to configure SSL/TLS encryption between the cluster nodes (server and client) and the clients that connect to your cluster.

## Considerations

All internal connections in the cluster context, as well as cluster's user interaction interfaces, are SSL-enabled. The communication categories are as follows:

* Between the user and the cluster (node): REST
* Between the user and the platform clients
* Between nodes: Network (Messaging, Scalecube)

All SSL configurations activities are performed at the node level.

Apache Ignite does not support direct paths to SSL certificates. Instead, it utilizes PKCS12 and JKS keystore.

## REST

The standard implementation of SSL for REST involves configuring a secure connection on a separate port. Apache Ignite supports HTTP and HTTPS, each on its own port.

The Apache Ignite 3 REST security configuration in the JSON format is provided below.

:::note
In Apache Ignite 3, you can create and maintain the configuration in either JSON or HOCON format.
:::

```json
{
    "ignite" : {
        "rest" : {
            "dualProtocol" : false,
            "httpToHttpsRedirection" : false,
            "port" : 10300,
            "ssl" : {
                "ciphers" : "",
                "clientAuth" : "require",
                "enabled" : true,
                "keyStore" : {
                    "password" : "may be empty",
                    "path" : "must not be empty",
                    "type" : "PKCS12"
                },
                "port" : 10400,
                "trustStore" : {
                    "password" : "may be empty",
                    "path" : "must not be empty",
                    "type" : "PKCS12"
                }
            }
        }
    }
}
```

## Clients and JDBC

Apache Ignite 3 Client implementation is based on the Netty framework, which supports configuration for security connections via `SSLContextBuilder`.

### Server-side Configuration

The default way to configure SSL on the server side is to update the configuration with SSL properties. The example below is in the JSON format.

:::note
In Apache Ignite 3, you can create and maintain the configuration in either JSON or HOCON format.
:::

```json
{
    "ignite" : {
        "clientConnector" : {
            "ssl" : {
                "ciphers" : "",
                "clientAuth" : "require",
                "enabled" : true,
                "keyStore" : {
                    "type" : "PKCS12",
                    "path" : "must not be empty",
                    "password" : "may be empty"
                },
                "trustStore" : {
                    "type" : "PKCS12",
                    "path" : "must not be empty",
                    "password" : "may be empty"
                }
            }
        }
    }
}
```

If you have enabled SSL for `clientConnector`, and want to use JDBC, set the corresponding properties in your code:

```java
var url =
    "jdbc:ignite:thin://{address}:{port}"
        + "?sslEnabled=true"
        + "&trustStorePath=" + trustStorePath
        + "&trustStoreType=JKS"
        + "&trustStorePassword=" + password
        + "&clientAuth=require"
        + "&keyStorePath=" + keyStorePath
        + "&keyStoreType=PKCS12"
        + "&keyStorePassword=" + password;
        try (Connection conn = DriverManager.getConnection(url)) {
            // Other actions.
        }
```


## Client Configuration

## Java

To enable SSL in your Java clients, use the `IgniteClient` class and pass the ssl configuration to it:

```Java
var sslConfiguration = SslConfiguration.builder()
                        .enabled(true)
                        .ciphers("TLS_AES_256_GCM_SHA384")
                        .trustStorePath(trustStorePath)
                        .trustStorePassword(password)
                        .keyStorePath(keyStorePath)
                        .keyStorePassword(password)
                        .build();

try (IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800")
    .ssl(sslConfiguration)
    .build();
)
```


### .NET

Add the `IgniteClientConfiguration.SslStreamFactory` property of type `ISslStreamFactory`.

Provide a [predefined implementation](https://github.com/apache/ignite/blob/66f43a4bee163aadb3ad731f6eb9a6dfde9faa73/modules/platforms/dotnet/Apache.Ignite.Core/Client/SslStreamFactory.cs).

Use the base class library `SslStream`.

Basic usage without client authorization:

```csharp
var cfg = new IgniteClientConfiguration { SslStreamFactory = new() }
```

## CLI

To SSL on the CLI side, use the `cli config set` command:

```shell
cli config set cli.trust-store.type=<type>
cli config set cli.trust-store.path=<path>
cli config set cli.trust-store.password=<password>
```

Store the CLI security configuration in a separate file with permission settings that protect it from unauthorized read/write operations. This configuration file must match profiles from the common configuration file.


## Network Configuration

The node network is based on the Netty framework. The configuration is the same as described for the Apache Ignite Client part except for the part that addresses the Apache Ignite 3 configuration.

:::note
In Apache Ignite 3, you can create and maintain the configuration in either JSON or HOCON format.
:::

```json
{
    "ignite" : {
        "network" : {
            "ssl" : {
                "ciphers" : "",
                "enabled" : true,
                "keyStore" : {
                    "type" : "PKCS12",
                    "path" : "must not be empty",
                    "password" : "may be empty"
                },
                "trustStore" : {
                    "type" : "PKCS12",
                    "path" : "must not be empty",
                    "password" : "may be empty"
                }
            }
        }
    }
}
```

## SSL Client Authentication (mTLS Support)

Optionally, the connections you utilize can support the client authentication feature. Configure it separately for each connection on the server side.

Two-way authentication requires that both server and client have certificates they reciprocally trust. The client generates a private key, stores it in its keystore, and gets it signed by an entity the server's truststore trusts.

To support client authentication, a connection must include the `clientAuth`, `trustStore` and `keyStore` properties. Here is an example of a possible client configuration. The example below is in the JSON format.

:::note
In Apache Ignite 3, you can create and maintain the configuration in either JSON or HOCON format.
:::

```json
{
    "ignite" : {
        "clientConnector" : {
            "ssl" : {
                "ciphers" : "",
                "clientAuth" : "require",
                "enabled" : true,
                "keyStore" : {
                    "type" : "PKCS12",
                    "path" : "must not be empty",
                    "password" : "may be empty"
                },
                "trustStore" : {
                    "type" : "JKS",
                    "path" : "must not be empty",
                    "password" : "may be empty"
                }
            }
        }
    }
}
```
