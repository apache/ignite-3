---
title: Security API
id: security-api
sidebar_position: 11
---

# Security API

The Security API configures authentication for client connections. Applications provide credentials when establishing connections to secure Ignite clusters. The API supports basic username and password authentication with extensibility for custom authentication mechanisms.

## Key Concepts

Authentication occurs during client connection establishment. Clients configure authenticators through the builder pattern before creating connections. The authenticator provides identity and secret data to the server for validation.

Basic authentication transmits username and password credentials. The client includes authentication type and credentials in connection requests. Servers validate credentials before accepting connections.

## Basic Authentication

Configure basic authentication with username and password:

```java
IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800")
    .authenticator(BasicAuthenticator.builder()
        .username("admin")
        .password("password")
        .build())
    .build();

try {
    String nodeName = client.name();
    System.out.println("Authenticated to: " + nodeName);
} finally {
    client.close();
}
```

## Authentication Configuration

Set authenticator during client building:

```java
BasicAuthenticator authenticator = BasicAuthenticator.builder()
    .username("myUsername")
    .password("myPassword")
    .build();

IgniteClient client = IgniteClient.builder()
    .addresses("server1:10800", "server2:10800")
    .authenticator(authenticator)
    .build();
```

The authenticator applies to all connections the client establishes.

## Authentication Types

Access authentication type information:

```java
BasicAuthenticator authenticator = BasicAuthenticator.builder()
    .username("user")
    .password("pass")
    .build();

String type = authenticator.type();
System.out.println("Authentication type: " + type);
```

The BASIC type indicates username and password authentication.

## Authentication Failure Handling

Handle authentication errors:

```java
try {
    IgniteClient client = IgniteClient.builder()
        .addresses("localhost:10800")
        .authenticator(BasicAuthenticator.builder()
            .username("user")
            .password("wrongpass")
            .build())
        .build();
} catch (IgniteException e) {
    System.err.println("Authentication failed: " + e.getMessage());
}
```

Connection failures due to invalid credentials throw exceptions during client creation.

## No Authentication

Omit the authenticator for unauthenticated connections:

```java
IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800")
    .build();
```

Clients without authenticators connect to clusters that do not require authentication.

## Authentication with TLS

Combine authentication with TLS encryption:

```java
SslConfiguration ssl = SslConfiguration.builder()
    .enabled(true)
    .trustStorePath("/path/to/truststore.jks")
    .trustStorePassword("trustpass")
    .build();

IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800")
    .ssl(ssl)
    .authenticator(BasicAuthenticator.builder()
        .username("admin")
        .password("password")
        .build())
    .build();
```

TLS encrypts the connection while authentication validates identity.

## Custom Authenticators

Implement custom authentication mechanisms:

```java
public class TokenAuthenticator implements IgniteClientAuthenticator {
    private final String token;

    public TokenAuthenticator(String token) {
        this.token = token;
    }

    @Override
    public String type() {
        return "TOKEN";
    }

    @Override
    public Object identity() {
        return token;
    }

    @Override
    public Object secret() {
        return "";
    }
}
```

Custom authenticators provide identity and secret data in appropriate formats.

## Authentication Type Parsing

The type() method returns a string identifier:

```java
BasicAuthenticator authenticator = BasicAuthenticator.builder()
    .username("user")
    .password("pass")
    .build();

String typeString = authenticator.type();
System.out.println("Type: " + typeString);
```

## Credential Management

Store credentials securely outside application code:

```java
String username = System.getenv("IGNITE_USERNAME");
String password = System.getenv("IGNITE_PASSWORD");

if (username == null || password == null) {
    throw new IllegalStateException("Credentials not configured");
}

IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800")
    .authenticator(BasicAuthenticator.builder()
        .username(username)
        .password(password)
        .build())
    .build();
```

Retrieve credentials from environment variables, configuration files, or credential managers.

## Asynchronous Connection with Authentication

Build authenticated clients asynchronously:

```java
CompletableFuture<IgniteClient> clientFuture = IgniteClient.builder()
    .addresses("localhost:10800")
    .authenticator(BasicAuthenticator.builder()
        .username("admin")
        .password("password")
        .build())
    .buildAsync();

clientFuture.thenAccept(client -> {
    System.out.println("Authenticated to: " + client.name());
}).exceptionally(ex -> {
    System.err.println("Authentication failed: " + ex.getMessage());
    return null;
});
```

## Connection Retry with Authentication

Retry policies apply to authenticated connections:

```java
IgniteClient client = IgniteClient.builder()
    .addresses("localhost:10800")
    .authenticator(BasicAuthenticator.builder()
        .username("user")
        .password("pass")
        .build())
    .retryPolicy(new RetryReadPolicy())
    .build();
```

Failed operations retry according to the policy after successful authentication.

## Server-Side Authentication

Server configuration determines authentication requirements. Clients must match server authentication settings. Consult server configuration documentation for authentication setup.

## Authentication Interface

The IgniteClientAuthenticator interface defines authentication contracts:

```java
public interface IgniteClientAuthenticator {
    String type();
    Object identity();
    Object secret();
}
```

Implementations provide authentication type and credential data.

## Identity and Secret Data

Authenticators separate identity and secret information:

```java
BasicAuthenticator auth = BasicAuthenticator.builder()
    .username("username")
    .password("password")
    .build();

Object identity = auth.identity();
Object secret = auth.secret();
```

BasicAuthenticator returns username as identity and password as secret.

## Embedded Node Authentication

Embedded nodes use configuration files for authentication setup. Client authentication applies only to thin client connections.

## Reference

- Authenticator interface: `org.apache.ignite.client.IgniteClientAuthenticator`
- Basic authentication: `org.apache.ignite.client.BasicAuthenticator`
- Authentication types: `org.apache.ignite.security.AuthenticationType`

### IgniteClientAuthenticator Interface

- `String type()` - Get authentication type
- `Object identity()` - Get identity data (username, token, etc.)
- `Object secret()` - Get secret data (password, key, etc.)

### BasicAuthenticator

- `static Builder builder()` - Create builder for authenticator
- `Builder.username(String)` - Set username, returns Builder
- `Builder.password(String)` - Set password, returns Builder
- `Builder.build()` - Build BasicAuthenticator instance
- `String type()` - Returns "BASIC"
- `Object identity()` - Returns username
- `Object secret()` - Returns password

### AuthenticationType

- `static AuthenticationType parse(String)` - Parse from string
- `BASIC` - Basic username/password authentication

### Client Builder Authentication

- `authenticator(IgniteClientAuthenticator)` - Set authenticator for connections

### Authentication Best Practices

- Store credentials in secure configuration stores, not in code
- Use environment variables or credential management systems
- Combine authentication with TLS for complete security
- Validate credentials before distributing applications
- Rotate credentials periodically according to security policies
- Handle authentication failures gracefully with appropriate error messages
- Test authentication in development environments before production deployment
