/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.runner.app.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.stream.IntStream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests thin client connection failover with and without partition awareness.
 */
public class ItThinConnectionFailoverTest extends ClusterPerTestIntegrationTest {
    private IgniteClient client;

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{2};
    }

    @BeforeEach
    void setUp() {
        client = getClient();

        client.sql().executeScript(
                "CREATE ZONE zone1 (REPLICAS 3) STORAGE PROFILES ['default'];"
                        + "CREATE TABLE t(id INT PRIMARY KEY, val INT) ZONE zone1");

        Awaitility.await().until(() -> client.connections().size(), is(initialNodes()));
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void testStopNodePartitionAwarenessKeyValue() {
        KeyValueView<Integer, Integer> kvView = client.tables().table("t")
                .keyValueView(Integer.class, Integer.class);

        for (int i = 0; i < 10; i++) {
            kvView.put(null, i, i);
        }

        cluster.stopNode(0);
        assertThat(client.connections().size(), is(2));

        for (int i = 10; i < 20; i++) {
            kvView.put(null, i, i);
        }
    }

    @Test
    void testStopNodePartitionAwarenessQuery() {
        IgniteSql sql = client.sql();

        for (int i = 0; i < 10; i++) {
            sql.execute("INSERT INTO t VALUES (?, ?)", i, i).close();
        }

        cluster.stopNode(0);
        assertThat(client.connections().size(), is(2));

        for (int i = 10; i < 20; i++) {
            sql.execute("INSERT INTO t VALUES (?, ?)", i, i).close();
        }
    }

    @Test
    void testStopNodeNonPartitionAwareQuery() {
        IgniteSql sql = client.sql();

        for (int i = 0; i < 10; i++) {
            sql.execute("SELECT " + i).close();
        }

        cluster.stopNode(0);
        assertThat(client.connections().size(), is(2));

        for (int i = 10; i < 20; i++) {
            sql.execute("SELECT " + i).close();
        }
    }

    private IgniteClient getClient() {
        String[] addresses = IntStream.range(0, initialNodes())
                .mapToObj(i -> "127.0.0.1:" + (10800 + i))
                .toArray(String[]::new);

        return IgniteClient.builder().addresses(addresses).build();
    }
}
