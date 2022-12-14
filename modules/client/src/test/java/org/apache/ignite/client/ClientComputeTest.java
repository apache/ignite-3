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

package org.apache.ignite.client;

import static org.apache.ignite.lang.ErrorGroups.Table.TABLE_NOT_FOUND_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Compute tests.
 */
public class ClientComputeTest {
    private static final String TABLE_NAME = "tbl1";

    private FakeIgnite ignite;
    private TestServer server1;
    private TestServer server2;
    private TestServer server3;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server1, server2, server3);
    }

    @Test
    public void testClientSendsComputeJobToTargetNodeWhenDirectConnectionExists() throws Exception {
        initServers(reqId -> false);

        // Provide same node multiple times to check this case as well.
        try (var client = getClient(server1, server2, server3, server1, server2)) {
            assertTrue(IgniteTestUtils.waitForCondition(() -> client.connections().size() == 3, 3000));

            String res1 = client.compute().<String>execute(getClusterNodes("s1"), "job").join();
            String res2 = client.compute().<String>execute(getClusterNodes("s2"), "job").join();
            String res3 = client.compute().<String>execute(getClusterNodes("s3"), "job").join();

            assertEquals("s1", res1);
            assertEquals("s2", res2);
            assertEquals("s3", res3);
        }
    }

    @Test
    public void testClientSendsComputeJobToDefaultNodeWhenDirectConnectionToTargetDoesNotExist() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server3)) {
            String res1 = client.compute().<String>execute(getClusterNodes("s1"), "job").join();
            String res2 = client.compute().<String>execute(getClusterNodes("s2"), "job").join();
            String res3 = client.compute().<String>execute(getClusterNodes("s3"), "job").join();

            assertEquals("s3", res1);
            assertEquals("s3", res2);
            assertEquals("s3", res3);
        }
    }

    @Test
    public void testClientRetriesComputeJobOnPrimaryAndDefaultNodes() throws Exception {
        initServers(reqId -> reqId % 3 == 0);

        try (var client = getClient(server3)) {
            for (int i = 0; i < 100; i++) {
                var nodeId = i % 3 + 1;
                var nodeName = "s" + nodeId;

                String res = client.compute().<String>execute(getClusterNodes(nodeName), "job").join();

                assertEquals("s3", res);
            }
        }
    }

    @Test
    public void testExecuteColocated() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server2)) {
            Tuple key = Tuple.create().set("key", "k");

            String res1 = client.compute().<String>executeColocated(TABLE_NAME, key, "job").join();
            assertEquals("s2", res1);

            String res2 = client.compute().<Long, String>executeColocated(TABLE_NAME, 1L, Mapper.of(Long.class), "job").join();
            assertEquals("s2", res2);
        }
    }

    @Test
    public void testExecuteColocatedThrowsTableNotFoundExceptionWhenTableDoesNotExist() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server1)) {
            Tuple key = Tuple.create().set("key", "k");

            var ex = assertThrows(CompletionException.class,
                    () -> client.compute().<String>executeColocated("bad-tbl", key, "job").join());

            var tblNotFoundEx = (TableNotFoundException) ex.getCause();
            assertThat(tblNotFoundEx.getMessage(), containsString("The table does not exist [name=\"PUBLIC\".\"bad-tbl\"]"));
            assertEquals(TABLE_NOT_FOUND_ERR, tblNotFoundEx.code());
        }
    }

    @Test
    void testExecuteColocatedUpdatesTableCacheOnTableDrop() throws Exception {
        String tableName = "drop-me";

        initServers(reqId -> false);
        ((FakeIgniteTables) ignite.tables()).createTable(tableName);

        try (var client = getClient(server3)) {
            Tuple key = Tuple.create().set("key", "k");

            String res1 = client.compute().<String>executeColocated(tableName, key, "job").join();
            assertEquals("s3", res1);

            // Drop table and create a new one with a different ID.
            ((FakeIgniteTables) ignite.tables()).dropTable(tableName);
            ((FakeIgniteTables) ignite.tables()).createTable(tableName);

            String res2 = client.compute().<Long, String>executeColocated(tableName, 1L, Mapper.of(Long.class), "job").join();
            assertEquals("s3", res2);
        }
    }

    private IgniteClient getClient(TestServer... servers) {
        String[] addresses = Arrays.stream(servers).map(s -> "127.0.0.1:" + s.port()).toArray(String[]::new);

        return IgniteClient.builder()
                .addresses(addresses)
                .reconnectThrottlingPeriod(0)
                .retryPolicy(new RetryLimitPolicy().retryLimit(3))
                .build();
    }

    private void initServers(Function<Integer, Boolean> shouldDropConnection) {
        ignite = new FakeIgnite();
        ((FakeIgniteTables) ignite.tables()).createTable(TABLE_NAME);

        var clusterId = UUID.randomUUID();

        server1 = new TestServer(10900, 10, 0, ignite, shouldDropConnection, 0, "s1", clusterId);
        server2 = new TestServer(10910, 10, 0, ignite, shouldDropConnection, 0, "s2", clusterId);
        server3 = new TestServer(10920, 10, 0, ignite, shouldDropConnection, 0, "s3", clusterId);
    }

    private Set<ClusterNode> getClusterNodes(String... names) {
        return Arrays.stream(names)
                .map(s -> new ClusterNode("id", s, new NetworkAddress("127.0.0.1", 8080)))
                .collect(Collectors.toSet());
    }
}
