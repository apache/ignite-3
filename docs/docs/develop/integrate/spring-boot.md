---
id: spring-boot
title: Spring Boot Integration
---

Apache Ignite 3 provides a Spring Boot starter that auto-configures an `IgniteClient` bean. The starter handles connection lifecycle, supports property-based configuration, and allows programmatic customization.

## Prerequisites

- Java 17 or later
- Spring Boot 3.x
- Running Ignite 3 cluster

## Installation

Add the starter dependency to your project. The starter version must match your Apache Ignite cluster version.

**Maven:**

```xml
<properties>
    <ignite.version>3.1.0</ignite.version>
</properties>

<dependency>
    <groupId>org.apache.ignite</groupId>
    <artifactId>spring-boot-starter-ignite-client</artifactId>
    <version>${ignite.version}</version>
</dependency>
```

**Gradle:**

```groovy
ext {
    igniteVersion = '3.1.0'
}

implementation "org.apache.ignite:spring-boot-starter-ignite-client:${igniteVersion}"
```

:::note Version Matching
The `spring-boot-starter-ignite-client` artifact is released as part of Apache Ignite, so its version matches the Ignite release version. For Ignite 3.1.0, use `spring-boot-starter-ignite-client:3.1.0`.
:::

## Basic Configuration

Configure the Ignite client connection in `application.properties`:

```properties
ignite.client.addresses=127.0.0.1:10800
```

The `IgniteClient` bean is automatically created and available for injection:

```java
@SpringBootApplication
public class MyApplication {

    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }

    @Bean
    ApplicationRunner runner(IgniteClient client) {
        return args -> {
            client.sql().execute(null, "CREATE TABLE IF NOT EXISTS Person (id INT PRIMARY KEY, name VARCHAR)");
            client.sql().execute(null, "INSERT INTO Person (id, name) VALUES (1, 'John')");
        };
    }
}
```

## Configuration Properties

All properties use the `ignite.client` prefix.

### Connection Properties

| Property | Type | Description |
|----------|------|-------------|
| `addresses` | String[] | Cluster node addresses in `host:port` format |
| `connectTimeout` | Long | Connection timeout in milliseconds |
| `operationTimeout` | Long | Operation timeout in milliseconds |
| `heartbeatInterval` | Long | Heartbeat message interval in milliseconds |
| `heartbeatTimeout` | Long | Heartbeat message timeout in milliseconds |
| `backgroundReconnectInterval` | Long | Background reconnect interval in milliseconds |
| `metricsEnabled` | Boolean | Enable client metrics |

**Example:**

```properties
ignite.client.addresses=node1:10800,node2:10800,node3:10800
ignite.client.connectTimeout=5000
ignite.client.operationTimeout=3000
ignite.client.heartbeatInterval=30000
ignite.client.heartbeatTimeout=5000
ignite.client.backgroundReconnectInterval=30000
ignite.client.metricsEnabled=true
```

### Authentication Properties

Configure basic authentication via properties:

| Property | Type | Description |
|----------|------|-------------|
| `auth.basic.username` | String | Authentication username |
| `auth.basic.password` | String | Authentication password |

**Example:**

```properties
ignite.client.addresses=127.0.0.1:10800
ignite.client.auth.basic.username=ignite
ignite.client.auth.basic.password=ignite
```

### SSL/TLS Properties

Configure SSL/TLS via the `sslConfiguration` nested property:

| Property | Type | Description |
|----------|------|-------------|
| `sslConfiguration.enabled` | Boolean | Enable SSL/TLS |
| `sslConfiguration.keyStorePath` | String | Path to key store file |
| `sslConfiguration.keyStorePassword` | String | Key store password |
| `sslConfiguration.trustStorePath` | String | Path to trust store file |
| `sslConfiguration.trustStorePassword` | String | Trust store password |
| `sslConfiguration.ciphers` | List | Allowed cipher suites |

**Example:**

```properties
ignite.client.addresses=127.0.0.1:10800
ignite.client.sslConfiguration.enabled=true
ignite.client.sslConfiguration.keyStorePath=/path/to/keystore.jks
ignite.client.sslConfiguration.keyStorePassword=changeit
ignite.client.sslConfiguration.trustStorePath=/path/to/truststore.jks
ignite.client.sslConfiguration.trustStorePassword=changeit
```

## Programmatic Customization

For configuration that cannot be expressed via properties, implement `IgniteClientPropertiesCustomizer`:

```java
@SpringBootApplication
public class MyApplication {

    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }

    @Bean
    public IgniteClientPropertiesCustomizer customizeClient() {
        return config -> {
            config.setRetryPolicy(new RetryLimitPolicy().retryLimit(5));
            config.setLoggerFactory(System::getLogger);
        };
    }
}
```

### Custom Authenticator

Override the default authentication by providing a custom authenticator:

```java
@Bean
public IgniteClientPropertiesCustomizer customizeClient() {
    return config -> config.setAuthenticator(
        BasicAuthenticator.builder()
            .username("ignite")
            .password("ignite")
            .build()
    );
}
```

When both property-based authentication (`auth.basic.*`) and a programmatic authenticator are configured, the programmatic authenticator takes precedence.

## Available Customization Options

The `IgniteClientPropertiesCustomizer` provides access to all `IgniteClientProperties` setters:

| Method | Description |
|--------|-------------|
| `setAddresses(String[])` | Cluster node addresses |
| `setRetryPolicy(RetryPolicy)` | Request retry policy |
| `setLoggerFactory(LoggerFactory)` | Custom logger factory |
| `setAddressFinder(IgniteClientAddressFinder)` | Dynamic address discovery |
| `setAuthenticator(IgniteClientAuthenticator)` | Custom authenticator |
| `setAsyncContinuationExecutor(Executor)` | Executor for async continuations |

## Using the Client

Inject `IgniteClient` anywhere in your application:

```java
@Service
public class PersonService {

    private final IgniteClient client;

    public PersonService(IgniteClient client) {
        this.client = client;
    }

    public void createPerson(int id, String name) {
        client.sql().execute(null,
            "INSERT INTO Person (id, name) VALUES (?, ?)",
            id, name);
    }

    public void createPersonInTransaction(int id, String name) {
        Transaction tx = client.transactions().begin();
        try {
            client.sql().execute(tx,
                "INSERT INTO Person (id, name) VALUES (?, ?)",
                id, name);
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            throw e;
        }
    }
}
```

## Next Steps

- [Spring Data Integration](./spring-data) - Repository-based data access
- [Java Client](../ignite-clients/java-client) - Client API reference
- [Transactions](../work-with-data/transactions) - Transaction management
