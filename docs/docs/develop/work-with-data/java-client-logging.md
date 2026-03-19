---
id: java-client-logging
title: Java Client Logging
---

The Apache Ignite 3 Java thin client uses the `System.Logger` API introduced in Java 9, which allows integration with any logging framework while providing sensible defaults.

## How Client Logging Works

The Ignite 3 client accepts a `LoggerFactory` through its builder API. This factory creates `System.Logger` instances for each internal component. When no factory is provided, the client uses `System::getLogger`, which delegates to the JDK Platform Logging system.

```java
IgniteClient client = IgniteClient.builder()
    .addresses("127.0.0.1:10800")
    .loggerFactory(System::getLogger)  // Optional: this is the default
    .build();
```

The JDK Platform Logging system routes to `java.util.logging` (JUL) by default. This means log output appears on the console (standard error) at INFO level unless you configure it otherwise.

## Log Level Name Mapping

Different logging frameworks use different names for the same log levels. This distinction matters because using the wrong name in a configuration file causes parsing errors.

| Severity | System.Logger | java.util.logging | Logback / Log4j2 |
|----------|---------------|-------------------|------------------|
| Most verbose | `TRACE` | `FINER` | `TRACE` |
| Diagnostic | `DEBUG` | `FINE` | `DEBUG` |
| Informational | `INFO` | `INFO` | `INFO` |
| Potential issues | `WARNING` | `WARNING` | `WARN` |
| Failures | `ERROR` | `SEVERE` | `ERROR` |

**JUL accepts only these level names:** `SEVERE`, `WARNING`, `INFO`, `CONFIG`, `FINE`, `FINER`, `FINEST`, `ALL`, `OFF`

Using System.Logger names like `DEBUG` or `TRACE` in a `logging.properties` file throws `IllegalArgumentException: Bad level "DEBUG"`. This is a common source of configuration errors.

## Configuring java.util.logging (Default)

When using the default logger factory, configure logging through JUL. You can use either a properties file or programmatic configuration.

### Properties File Configuration

Create a file named `logging.properties`:

```properties
# Set the Ignite client package to FINE (equivalent to DEBUG)
# Both the logger and handler levels must be set for messages to appear
org.apache.ignite.internal.client.level = FINE

# The handler acts as a secondary filter on log output
java.util.logging.ConsoleHandler.level = FINE
java.util.logging.ConsoleHandler.formatter = java.util.logging.SimpleFormatter

# File output (optional)
java.util.logging.FileHandler.pattern = %h/ignite-client%u.log
java.util.logging.FileHandler.level = FINE
java.util.logging.FileHandler.formatter = java.util.logging.SimpleFormatter
handlers = java.util.logging.ConsoleHandler, java.util.logging.FileHandler
```

Load the configuration at JVM startup:

```bash
java -Djava.util.logging.config.file=/path/to/logging.properties -jar your-app.jar
```

**File output location:** The `%h` token resolves to the user home directory. The `%u` token adds a unique number to prevent file conflicts when multiple JVMs run simultaneously. Example path: `/home/user/ignite-client0.log`

### Programmatic Configuration

Configure JUL before creating the client. This approach works when you cannot modify JVM startup parameters.

```java
import java.util.logging.*;
import java.io.IOException;

public class IgniteClientApp {
    public static void main(String[] args) {
        configureLogging();

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {
            // Application code
        }
    }

    private static void configureLogging() {
        Logger igniteLogger = Logger.getLogger("org.apache.ignite.internal.client");
        igniteLogger.setLevel(Level.FINE);

        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINE);
        igniteLogger.addHandler(consoleHandler);

        try {
            FileHandler fileHandler = new FileHandler("ignite-client.log", true);
            fileHandler.setLevel(Level.FINE);
            fileHandler.setFormatter(new SimpleFormatter());
            igniteLogger.addHandler(fileHandler);
        } catch (IOException e) {
            System.err.println("Failed to create log file: " + e.getMessage());
        }
    }
}
```

**File output location:** The `FileHandler` writes to the current working directory when given a relative path. Use an absolute path like `/var/log/myapp/ignite-client.log` for predictable file placement.

## Configuring Logback

Logback and SLF4J use their own level names (DEBUG, TRACE, WARN, ERROR), not JUL names. Configure logging in `logback.xml`:

```xml
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>logs/ignite-client.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>logs/ignite-client.%d{yyyy-MM-dd}.log</fileNamePattern>
            <maxHistory>30</maxHistory>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n</pattern>
        </encoder>
    </appender>

    <logger name="org.apache.ignite.internal.client" level="DEBUG" additivity="false">
        <appender-ref ref="CONSOLE"/>
        <appender-ref ref="FILE"/>
    </logger>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

**File output location:** Logs write to `logs/ignite-client.log` relative to the working directory. The rolling policy archives files daily with names like `logs/ignite-client.2024-01-15.log`.

### Integrating SLF4J with System.Logger

The Ignite client requires a `System.Logger`, but Logback provides SLF4J loggers. Create an adapter to bridge the two:

```java
import org.apache.ignite.lang.LoggerFactory;

public class Slf4jLoggerFactory implements LoggerFactory {
    @Override
    public System.Logger forName(String name) {
        return new Slf4jSystemLoggerAdapter(
            org.slf4j.LoggerFactory.getLogger(name)
        );
    }
}
```

Pass the adapter to the client builder:

```java
IgniteClient client = IgniteClient.builder()
    .addresses("127.0.0.1:10800")
    .loggerFactory(new Slf4jLoggerFactory())
    .build();
```

## Configuring Log4j2

Log4j2 uses level names similar to Logback. Configure in `log4j2.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
    <Appenders>
        <Console name="Console" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
        <RollingFile name="File"
                     fileName="logs/ignite-client.log"
                     filePattern="logs/ignite-client-%d{yyyy-MM-dd}-%i.log.gz">
            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{50} - %msg%n"/>
            <Policies>
                <TimeBasedTriggeringPolicy/>
                <SizeBasedTriggeringPolicy size="100 MB"/>
            </Policies>
        </RollingFile>
    </Appenders>
    <Loggers>
        <Logger name="org.apache.ignite.internal.client" level="DEBUG" additivity="false">
            <AppenderRef ref="Console"/>
            <AppenderRef ref="File"/>
        </Logger>
        <Root level="INFO">
            <AppenderRef ref="Console"/>
        </Root>
    </Loggers>
</Configuration>
```

**File output location:** Logs write to `logs/ignite-client.log`. The rolling policy triggers on both date change and 100 MB file size, compressing archived files to gzip format.

## Log Categories

The client creates loggers using fully qualified class names. Target these categories to control verbosity for specific components:

| Category | Content |
|----------|---------|
| `org.apache.ignite.internal.client.TcpClientChannel` | Connection establishment, heartbeats, request/response cycles |
| `org.apache.ignite.internal.client.ReliableChannel` | Failover decisions, retry attempts, channel selection |
| `org.apache.ignite.internal.client.sql.ClientSql` | SQL query execution, partition awareness routing |
| `org.apache.ignite.internal.client.table.ClientTable` | Table operations, schema version management |
| `org.apache.ignite.internal.client.table.ClientDataStreamer` | Bulk data streaming, batch processing |
| `org.apache.ignite.internal.client.ClientTimeoutWorker` | Operation timeout detection and handling |

Use the parent category `org.apache.ignite.internal.client` to control all client logging with a single configuration entry.

## What Each Level Logs

### TRACE (JUL: FINER)

Every outbound request with operation code and destination address. Enable only when diagnosing specific request-level issues, as this generates substantial output.

### DEBUG (JUL: FINE)

- Connection established and closed events with remote addresses
- Schema loading completion with table ID and version
- Retry decisions showing attempt count and operation type
- SQL partition awareness routing outcomes

### INFO

- Partition assignment change notifications from the cluster

### WARN (JUL: WARNING)

- Connection close failures
- Request transmission failures
- Heartbeat timeouts triggering channel closure
- Handshake protocol failures
- Schema version not found during operations

### ERROR (JUL: SEVERE)

- Response deserialization failures
- Unexpected response ID mismatches
- Server notification processing failures
- Transaction commit and rollback failures

## Production Recommendations

Set the client logger to WARN in production to capture connection problems and failures without generating excessive log volume:

**Logback / Log4j2:**
```xml
<logger name="org.apache.ignite.internal.client" level="WARN"/>
```

**java.util.logging:**
```properties
org.apache.ignite.internal.client.level = WARNING
```

Enable DEBUG temporarily when troubleshooting connection or failover issues. Enable TRACE only for short diagnostic sessions when investigating specific request failures.

## Troubleshooting

### No log output appears

Verify both the logger level and handler level are set. In JUL, the handler filters messages independently of the logger. A logger set to FINE with a handler set to INFO produces no FINE-level output.

### "Bad level" exception at startup

You used a System.Logger or Logback level name in a JUL configuration file. Replace `DEBUG` with `FINE`, `TRACE` with `FINER`, `ERROR` with `SEVERE`, and `WARN` with `WARNING`.

### Logs appear on console but not in file

Check that the file handler is registered. In JUL, add it to the `handlers` property. Verify the application has write permission to the target directory.

### Duplicate log messages

Set `additivity="false"` on the Ignite logger in Logback/Log4j2 configuration. This prevents messages from propagating to the root logger and appearing twice.
