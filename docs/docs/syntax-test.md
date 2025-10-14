# Syntax Highlighting Test

This page tests Prism.js syntax highlighting for all languages used in Apache Ignite 3 documentation.

## Java

```java
package org.apache.ignite.examples;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;

public class QuickStartExample {
    public static void main(String[] args) {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            KeyValueView<Long, String> kvView = client.tables()
                .table("my_table")
                .keyValueView(Long.class, String.class);

            kvView.put(null, 1L, "Hello, Ignite!");
            String value = kvView.get(null, 1L);
            System.out.println("Retrieved value: " + value);
        }
    }
}
```

## C# / .NET

```csharp
using Apache.Ignite;
using System;
using System.Threading.Tasks;

namespace Apache.Ignite.Examples
{
    public class QuickStartExample
    {
        public static async Task Main()
        {
            var cfg = new IgniteClientConfiguration("127.0.0.1:10800");

            using var client = await IgniteClient.StartAsync(cfg);
            var table = await client.Tables.GetTableAsync("my_table");
            var kvView = table.GetKeyValueView<long, string>();

            await kvView.PutAsync(transaction: null, key: 1L, val: "Hello, Ignite!");
            (string? value, bool hasValue) = await kvView.GetAsync(transaction: null, key: 1L);

            Console.WriteLine($"Retrieved value: {value}");
        }
    }
}
```

## C++

```cpp
#include <ignite/client/ignite_client.h>
#include <ignite/client/ignite_client_configuration.h>
#include <iostream>

using namespace ignite;

int main() {
    ignite_client_configuration cfg{"127.0.0.1:10800"};
    auto client = ignite_client::start(cfg, std::chrono::seconds(5));

    auto table = client.get_tables().get_table("my_table");
    auto kv_view = table->get_key_value_view<std::int64_t, std::string>();

    kv_view->put(nullptr, 1L, "Hello, Ignite!");
    auto value = kv_view->get(nullptr, 1L);

    if (value) {
        std::cout << "Retrieved value: " << *value << std::endl;
    }

    return 0;
}
```

## Python

```python
from pyignite import Client

def main():
    client = Client()
    client.connect('127.0.0.1', 10800)

    try:
        table = client.get_table('my_table')
        kv_view = table.get_key_value_view()

        kv_view.put(None, 1, 'Hello, Ignite!')
        value = kv_view.get(None, 1)

        print(f'Retrieved value: {value}')
    finally:
        client.close()

if __name__ == '__main__':
    main()
```

## SQL

```sql
-- Create table
CREATE TABLE IF NOT EXISTS person (
    id INT PRIMARY KEY,
    name VARCHAR NOT NULL,
    age INT,
    salary DECIMAL(10, 2)
);

-- Insert data
INSERT INTO person (id, name, age, salary)
VALUES (1, 'John Doe', 30, 50000.00);

-- Select with joins
SELECT p.name, p.age, d.department_name
FROM person p
INNER JOIN department d ON p.dept_id = d.id
WHERE p.age > 25
ORDER BY p.salary DESC
LIMIT 10;

-- Update statement
UPDATE person
SET salary = salary * 1.10
WHERE age > 30;

-- Delete statement
DELETE FROM person
WHERE age < 18;
```

## Bash / Shell

```bash
#!/bin/bash

# Download Ignite binary
wget https://archive.apache.org/dist/ignite/3.1.0/ignite3-3.1.0.zip

# Extract archive
unzip ignite3-3.1.0.zip
cd ignite3-3.1.0

# Start node
./bin/ignite3 node start my-first-node \
    --config=/path/to/ignite-config.conf

# Check status
./bin/ignite3 node status

# Connect to cluster
./bin/ignite3 sql \
    --jdbc-url jdbc:ignite:thin://127.0.0.1:10800

# Stop node
./bin/ignite3 node stop my-first-node
```

## JSON

```json
{
  "network": {
    "port": 10800,
    "portRange": 100,
    "shutdownQuietPeriod": 0,
    "shutdownTimeout": 15000
  },
  "clientConnector": {
    "ssl": {
      "enabled": true,
      "clientAuth": "require",
      "keyStore": {
        "path": "/path/to/keystore.jks",
        "password": "changeit"
      },
      "trustStore": {
        "path": "/path/to/truststore.jks",
        "password": "changeit"
      }
    }
  },
  "rest": {
    "port": 10300,
    "dualProtocol": false
  }
}
```

## YAML

```yaml
ignite:
  network:
    port: 10800
    portRange: 100
    shutdownQuietPeriod: 0
    shutdownTimeout: 15000

  clientConnector:
    ssl:
      enabled: true
      clientAuth: require
      keyStore:
        path: /path/to/keystore.jks
        password: changeit
      trustStore:
        path: /path/to/truststore.jks
        password: changeit

  rest:
    port: 10300
    dualProtocol: false

  cluster:
    name: ignite-cluster
    membershipProvider: static
    members:
      - node1.example.com:10800
      - node2.example.com:10800
      - node3.example.com:10800
```

## HOCON

HOCON (Human-Optimized Config Object Notation) is the primary configuration format for Apache Ignite 3 and GridGain 9. HOCON is a superset of JSON with additional features like comments, substitutions, and unquoted strings.

```javascript
// Node configuration (ignite-config.conf)
// HOCON syntax - rendered as JavaScript for syntax highlighting

ignite {
  # Network configuration
  network {
    port: 10800
    portRange: 100
    nodeFinder {
      netClusterNodes: ["localhost:3344", "localhost:3345", "localhost:3346"]
    }
  }

  # Client connector configuration
  clientConnector {
    port: 10800
    idleTimeout: 30000
    ssl {
      enabled: true
      clientAuth: "require"
      keyStore {
        type: "PKCS12"
        path: "/opt/ignite/config/keystore.p12"
        password: "changeit"
      }
      trustStore {
        type: "PKCS12"
        path: "/opt/ignite/config/truststore.p12"
        password: "changeit"
      }
    }
  }

  # REST configuration
  rest {
    port: 10300
    dualProtocol: false
    ssl {
      enabled: true
      port: 10400
      keyStore {
        path: "/opt/ignite/config/rest-keystore.p12"
        password: "changeit"
      }
    }
  }

  # Compute configuration
  compute {
    threadPoolSize: 8
    threadPoolStopTimeoutMillis: 10000
  }

  # Storage profiles
  aimem.regions: [{
    name: "inmemory_region"
    maxSize: "256M"
  }]

  rocksDb.regions: [{
    name: "persistent_region"
    size: "256M"
    writeBufferSize: "64M"
    path: "/opt/ignite/db"
  }]

  # Cluster configuration
  cluster {
    name: "ignite-cluster"
    managementPort: 10300
  }

  # Node attributes (using substitution)
  nodeAttributes {
    region: ${?REGION}
    environment: ${?ENV}
    nodeRole: "compute"
  }
}

// Example of HOCON features
rocksDb {
  # Reference to base configuration
  defaultRegion: ${aimem.regions.0}

  # Array concatenation
  regions: ${aimem.regions} [{
    name: "additional_region"
    size: "512M"
  }]

  # Object merging
  cache: {
    writeBufferSize: "32M"
    cache: ${rocksDb.cache} {
      blockSize: "4K"
    }
  }
}
```

## Docker Compose

Docker Compose is used for local development and testing of Apache Ignite 3 clusters.

```yaml
version: '3.8'

services:
  ignite-node-1:
    image: apacheignite/ignite3:latest
    container_name: ignite3-node-1
    environment:
      - IGNITE_NODE_NAME=node1
      - IGNITE_CLUSTER_NAME=ignite-cluster
    ports:
      - "10800:10800"
      - "10300:10300"
    volumes:
      - ./config/ignite-config.conf:/opt/ignite/config/ignite-config.conf
      - ignite-data-1:/opt/ignite/data
      - ignite-work-1:/opt/ignite/work
    networks:
      - ignite-network
    command: >
      /opt/ignite/bin/ignite3 node start node1
      --config=/opt/ignite/config/ignite-config.conf

  ignite-node-2:
    image: apacheignite/ignite3:latest
    container_name: ignite3-node-2
    environment:
      - IGNITE_NODE_NAME=node2
      - IGNITE_CLUSTER_NAME=ignite-cluster
    ports:
      - "10801:10800"
      - "10301:10300"
    volumes:
      - ./config/ignite-config.conf:/opt/ignite/config/ignite-config.conf
      - ignite-data-2:/opt/ignite/data
      - ignite-work-2:/opt/ignite/work
    networks:
      - ignite-network
    depends_on:
      - ignite-node-1
    command: >
      /opt/ignite/bin/ignite3 node start node2
      --config=/opt/ignite/config/ignite-config.conf

  ignite-node-3:
    image: apacheignite/ignite3:latest
    container_name: ignite3-node-3
    environment:
      - IGNITE_NODE_NAME=node3
      - IGNITE_CLUSTER_NAME=ignite-cluster
    ports:
      - "10802:10800"
      - "10302:10300"
    volumes:
      - ./config/ignite-config.conf:/opt/ignite/config/ignite-config.conf
      - ignite-data-3:/opt/ignite/data
      - ignite-work-3:/opt/ignite/work
    networks:
      - ignite-network
    depends_on:
      - ignite-node-1
    command: >
      /opt/ignite/bin/ignite3 node start node3
      --config=/opt/ignite/config/ignite-config.conf

volumes:
  ignite-data-1:
  ignite-data-2:
  ignite-data-3:
  ignite-work-1:
  ignite-work-2:
  ignite-work-3:

networks:
  ignite-network:
    driver: bridge
```

## Kubernetes (Helm Charts)

Kubernetes manifests and Helm charts for deploying Apache Ignite 3 clusters.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ignite3-cluster
  namespace: ignite
  labels:
    app: ignite3
    component: cluster
spec:
  serviceName: ignite3-service
  replicas: 3
  selector:
    matchLabels:
      app: ignite3
  template:
    metadata:
      labels:
        app: ignite3
        component: node
    spec:
      serviceAccountName: ignite3
      containers:
      - name: ignite3
        image: apacheignite/ignite3:3.1.0
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 10800
          name: client
          protocol: TCP
        - containerPort: 10300
          name: rest
          protocol: TCP
        - containerPort: 3344
          name: discovery
          protocol: TCP
        env:
        - name: IGNITE_NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: IGNITE_CLUSTER_NAME
          value: "ignite-cluster"
        - name: JAVA_OPTS
          value: "-Xms2g -Xmx2g"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        volumeMounts:
        - name: config
          mountPath: /opt/ignite/config
        - name: data
          mountPath: /opt/ignite/data
        - name: work
          mountPath: /opt/ignite/work
        livenessProbe:
          httpGet:
            path: /management/v1/node/state
            port: 10300
          initialDelaySeconds: 60
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /management/v1/node/state
            port: 10300
          initialDelaySeconds: 30
          periodSeconds: 5
      volumes:
      - name: config
        configMap:
          name: ignite3-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: "fast-ssd"
      resources:
        requests:
          storage: 10Gi
  - metadata:
      name: work
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: "fast-ssd"
      resources:
        requests:
          storage: 5Gi
---
apiVersion: v1
kind: Service
metadata:
  name: ignite3-service
  namespace: ignite
  labels:
    app: ignite3
spec:
  clusterIP: None
  ports:
  - port: 10800
    name: client
  - port: 10300
    name: rest
  - port: 3344
    name: discovery
  selector:
    app: ignite3
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: ignite3-config
  namespace: ignite
data:
  ignite-config.conf: |
    ignite {
      network {
        port: 10800
        portRange: 100
        nodeFinder {
          netClusterNodes: [
            "ignite3-cluster-0.ignite3-service:3344",
            "ignite3-cluster-1.ignite3-service:3344",
            "ignite3-cluster-2.ignite3-service:3344"
          ]
        }
      }
      clientConnector.port: 10800
      rest.port: 10300
    }
```

## XML (Spring Boot / Spring Data)

XML configuration for Spring Boot and Spring Data integration with Apache Ignite 3.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:context="http://www.springframework.org/schema/context"
       xsi:schemaLocation="
           http://www.springframework.org/schema/beans
           http://www.springframework.org/schema/beans/spring-beans.xsd
           http://www.springframework.org/schema/context
           http://www.springframework.org/schema/context/spring-context.xsd">

    <!-- Enable annotation-driven configuration -->
    <context:annotation-config/>
    <context:component-scan base-package="org.apache.ignite.examples"/>

    <!-- Ignite Client Configuration -->
    <bean id="igniteClientConfiguration"
          class="org.apache.ignite.client.IgniteClientConfiguration">
        <property name="addresses">
            <list>
                <value>127.0.0.1:10800</value>
                <value>127.0.0.1:10801</value>
                <value>127.0.0.1:10802</value>
            </list>
        </property>
        <property name="connectTimeout" value="5000"/>
        <property name="reconnectThrottlingPeriod" value="30000"/>
        <property name="reconnectThrottlingRetries" value="3"/>
        <property name="heartbeatInterval" value="30000"/>
        <property name="sslConfiguration">
            <bean class="org.apache.ignite.client.SslConfiguration">
                <property name="enabled" value="true"/>
                <property name="clientAuth" value="require"/>
                <property name="keyStorePath" value="classpath:keystore.jks"/>
                <property name="keyStorePassword" value="changeit"/>
                <property name="trustStorePath" value="classpath:truststore.jks"/>
                <property name="trustStorePassword" value="changeit"/>
            </bean>
        </property>
    </bean>

    <!-- Ignite Client Bean -->
    <bean id="igniteClient"
          class="org.apache.ignite.client.IgniteClient"
          factory-method="start"
          destroy-method="close">
        <constructor-arg ref="igniteClientConfiguration"/>
    </bean>

    <!-- Data Source Configuration -->
    <bean id="dataSource" class="org.apache.ignite.jdbc.IgniteJdbcDataSource">
        <property name="url" value="jdbc:ignite:thin://127.0.0.1:10800"/>
        <property name="schema" value="PUBLIC"/>
    </bean>

    <!-- Transaction Manager -->
    <bean id="transactionManager"
          class="org.springframework.jdbc.datasource.DataSourceTransactionManager">
        <property name="dataSource" ref="dataSource"/>
    </bean>

    <!-- JDBC Template -->
    <bean id="jdbcTemplate" class="org.springframework.jdbc.core.JdbcTemplate">
        <property name="dataSource" ref="dataSource"/>
    </bean>

    <!-- Example Repository Bean -->
    <bean id="personRepository"
          class="org.apache.ignite.examples.repository.PersonRepositoryImpl">
        <property name="igniteClient" ref="igniteClient"/>
        <property name="jdbcTemplate" ref="jdbcTemplate"/>
    </bean>

    <!-- Example Service Bean -->
    <bean id="personService"
          class="org.apache.ignite.examples.service.PersonServiceImpl">
        <property name="personRepository" ref="personRepository"/>
    </bean>

</beans>
```

Example Spring Boot `application.xml` for Ignite properties:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
    <!-- Ignite Client Configuration -->
    <entry key="ignite.client.addresses">127.0.0.1:10800,127.0.0.1:10801</entry>
    <entry key="ignite.client.connectTimeout">5000</entry>
    <entry key="ignite.client.reconnectThrottlingPeriod">30000</entry>
    <entry key="ignite.client.heartbeatInterval">30000</entry>

    <!-- SSL Configuration -->
    <entry key="ignite.client.ssl.enabled">true</entry>
    <entry key="ignite.client.ssl.clientAuth">require</entry>
    <entry key="ignite.client.ssl.keyStorePath">classpath:keystore.jks</entry>
    <entry key="ignite.client.ssl.keyStorePassword">changeit</entry>
    <entry key="ignite.client.ssl.trustStorePath">classpath:truststore.jks</entry>
    <entry key="ignite.client.ssl.trustStorePassword">changeit</entry>

    <!-- JDBC Configuration -->
    <entry key="ignite.jdbc.url">jdbc:ignite:thin://127.0.0.1:10800</entry>
    <entry key="ignite.jdbc.schema">PUBLIC</entry>
    <entry key="ignite.jdbc.driverClassName">org.apache.ignite.jdbc.IgniteJdbcThinDriver</entry>

    <!-- Connection Pool Settings -->
    <entry key="ignite.jdbc.pool.initialSize">10</entry>
    <entry key="ignite.jdbc.pool.maxActive">50</entry>
    <entry key="ignite.jdbc.pool.maxIdle">20</entry>
    <entry key="ignite.jdbc.pool.minIdle">5</entry>
</properties>
```

## Log Output

Apache Ignite 3 uses Java Util Logging (JUL) with a custom formatter. Log output is used in troubleshooting sections to show startup sequences, errors, and diagnostic information.

:::note
Prism.js does not have built-in syntax highlighting for JUL log format. For best readability in documentation, use `plaintext` or `text` language identifier. IDEs like VS Code provide richer log syntax highlighting with semantic colorization of log levels, timestamps, and stack traces.
:::

```text
2025-10-14 10:30:45:123 +0000 [INFO][main][IgniteServerImpl] Apache Ignite 3 starting...
2025-10-14 10:30:45:156 +0000 [INFO][main][IgniteServerImpl] Node name: ignite-node-1
2025-10-14 10:30:45:189 +0000 [INFO][main][ConfigurationTreeGenerator] Configuration initialized successfully
2025-10-14 10:30:45:234 +0000 [INFO][main][RocksDbStorageEngine] Initializing RocksDB storage engine
2025-10-14 10:30:45:267 +0000 [INFO][main][RocksDbStorageEngine] RocksDB storage engine initialized at /opt/ignite/db
2025-10-14 10:30:45:345 +0000 [INFO][main][NettyBootstrapFactory] Starting network services on port 10800
2025-10-14 10:30:45:456 +0000 [INFO][main][ScaleCubeClusterServiceFactory] Initializing cluster discovery
2025-10-14 10:30:45:512 +0000 [INFO][main][ClusterService] Cluster node started: ignite-node-1
2025-10-14 10:30:45:567 +0000 [INFO][main][ClusterService] Discovered cluster nodes: [ignite-node-1, ignite-node-2, ignite-node-3]
2025-10-14 10:30:45:623 +0000 [INFO][main][ClientHandlerModule] Client connector started on port 10800
2025-10-14 10:30:45:678 +0000 [INFO][main][RestComponent] REST endpoint started on port 10300
2025-10-14 10:30:45:734 +0000 [INFO][main][IgniteServerImpl] Apache Ignite 3 started successfully
2025-10-14 10:30:45:789 +0000 [INFO][main][IgniteServerImpl] Node is ready to accept connections

2025-10-14 10:35:12:456 +0000 [INFO][client-handler-worker-1][ClientHandlerModule] Client connected from /192.168.1.100:54321
2025-10-14 10:35:12:512 +0000 [DEBUG][sql-execution-pool-2][SqlQueryProcessor] Executing query: SELECT * FROM person WHERE age > 25
2025-10-14 10:35:12:567 +0000 [DEBUG][sql-execution-pool-2][SqlQueryProcessor] Query execution time: 45ms, rows returned: 123

2025-10-14 10:40:23:789 +0000 [WARN][metastorage-watch-processor-3][MetaStorageManagerImpl] Detected slow operation: metastorage watch processing took 5234ms
2025-10-14 10:40:23:845 +0000 [WARN][partition-operations-4][PartitionReplicaListener] Partition rebalancing is taking longer than expected: partition=5, elapsed=12345ms

2025-10-14 10:45:34:123 +0000 [ERROR][tx-manager-5][TxManagerImpl] Transaction rolled back due to timeout
2025-10-14 10:45:34:178 +0000 [ERROR][tx-manager-5][TxManagerImpl] Transaction ID: 0x00000000-0000-0000-0000-000000000123
org.apache.ignite.tx.TransactionException: Transaction timed out after 30000ms
	at org.apache.ignite.internal.tx.impl.TxManagerImpl.rollback(TxManagerImpl.java:456)
	at org.apache.ignite.internal.tx.impl.TxManagerImpl.lambda$beginAsync$2(TxManagerImpl.java:234)
	at java.base/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1768)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	at java.base/java.lang.Thread.run(Thread.java:840)

2025-10-14 10:50:45:234 +0000 [ERROR][network-io-6][ConnectionManager] Failed to establish connection to node: ignite-node-2
java.net.ConnectException: Connection refused: /192.168.1.102:10800
	at java.base/sun.nio.ch.Net.pollConnect(Native Method)
	at java.base/sun.nio.ch.Net.pollConnectNow(Net.java:672)
	at java.base/sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:946)
	at io.netty.channel.socket.nio.NioSocketChannel.doFinishConnect(NioSocketChannel.java:337)
	at io.netty.channel.nio.AbstractNioChannel$AbstractNioUnsafe.finishConnect(AbstractNioChannel.java:334)
	at io.netty.channel.nio.NioEventLoop.processSelectedKey(NioEventLoop.java:776)
	at io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized(NioEventLoop.java:724)
	at io.netty.channel.nio.NioEventLoop.processSelectedKeys(NioEventLoop.java:650)
	at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:562)
	at io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:997)
	at io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
	at java.base/java.lang.Thread.run(Thread.java:840)
Caused by: java.net.ConnectException: Connection refused
	... 12 more

2025-10-14 10:55:56:345 +0000 [INFO][shutdown-thread][IgniteServerImpl] Stopping Apache Ignite 3 node...
2025-10-14 10:55:56:456 +0000 [INFO][shutdown-thread][ClientHandlerModule] Client connector stopped
2025-10-14 10:55:56:567 +0000 [INFO][shutdown-thread][RestComponent] REST endpoint stopped
2025-10-14 10:55:56:678 +0000 [INFO][shutdown-thread][ClusterService] Node left the cluster: ignite-node-1
2025-10-14 10:55:56:789 +0000 [INFO][shutdown-thread][RocksDbStorageEngine] Storage engine stopped
2025-10-14 10:55:56:890 +0000 [INFO][shutdown-thread][IgniteServerImpl] Apache Ignite 3 stopped
```

Log Format: `YYYY-MM-DD HH:mm:ss:SSS Z [LEVEL][thread-name][logger-name] message`

Log Levels:
- **TRACE**: Very detailed debug information
- **DEBUG**: Debug information useful for troubleshooting
- **INFO**: Informational messages about normal operation
- **WARNING**: Warnings about potential issues
- **ERROR**: Error messages with stack traces

## Multi-Language Code Tabs Example

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="clients">
  <TabItem value="java" label="Java">

```java
try (IgniteClient client = IgniteClient.builder()
        .addresses("127.0.0.1:10800")
        .build()) {
    // Client is ready
    System.out.println("Connected to Ignite cluster");
}
```

  </TabItem>
  <TabItem value="csharp" label="C#">

```csharp
var cfg = new IgniteClientConfiguration("127.0.0.1:10800");
using var client = await IgniteClient.StartAsync(cfg);
// Client is ready
Console.WriteLine("Connected to Ignite cluster");
```

  </TabItem>
  <TabItem value="cpp" label="C++">

```cpp
ignite_client_configuration cfg{"127.0.0.1:10800"};
auto client = ignite_client::start(cfg, std::chrono::seconds(5));
// Client is ready
std::cout << "Connected to Ignite cluster" << std::endl;
```

  </TabItem>
  <TabItem value="python" label="Python">

```python
client = Client()
client.connect('127.0.0.1', 10800)
# Client is ready
print('Connected to Ignite cluster')
```

  </TabItem>
</Tabs>
