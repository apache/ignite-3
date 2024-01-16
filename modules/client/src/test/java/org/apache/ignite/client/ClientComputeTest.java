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

import static org.apache.ignite.client.AbstractClientTest.getClient;
import static org.apache.ignite.client.AbstractClientTest.getClusterNodes;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.lang.ErrorGroups.Table.TABLE_NOT_FOUND_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.apache.ignite.client.fakes.FakeCompute;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Compute tests.
 */
@SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
public class ClientComputeTest extends BaseIgniteAbstractTest {
    private static final String TABLE_NAME = "tbl1";

    private FakeIgnite ignite;
    private TestServer server1;
    private TestServer server2;
    private TestServer server3;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server1, server2, server3);
        FakeCompute.future = null;
    }

    @Test
    public void testClientSendsComputeJobToTargetNodeWhenDirectConnectionExists() throws Exception {
        initServers(reqId -> false);

        // Provide same node multiple times to check this case as well.
        try (var client = getClient(server1, server2, server3, server1, server2)) {
            assertTrue(IgniteTestUtils.waitForCondition(() -> client.connections().size() == 3, 3000));

            CompletableFuture<String> fut1 = client.compute()
                    .<String>executeAsync(getClusterNodes("s1"), List.of(), "job", new Object[]{}).resultAsync();

            CompletableFuture<String> fut2 = client.compute()
                    .<String>executeAsync(getClusterNodes("s2"), List.of(), "job", new Object[]{}).resultAsync();

            CompletableFuture<String> fut3 = client.compute()
                    .<String>executeAsync(getClusterNodes("s3"), List.of(), "job", new Object[]{}).resultAsync();

            assertThat(fut1, willBe("s1"));
            assertThat(fut2, willBe("s2"));
            assertThat(fut3, willBe("s3"));
        }
    }

    @Test
    public void testClientSendsComputeJobToDefaultNodeWhenDirectConnectionToTargetDoesNotExist() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server3)) {
            CompletableFuture<String> fut1 = client.compute()
                    .<String>executeAsync(getClusterNodes("s1"), List.of(), "job", new Object[]{}).resultAsync();

            CompletableFuture<String> fut2 = client.compute()
                    .<String>executeAsync(getClusterNodes("s2"), List.of(), "job", new Object[]{}).resultAsync();

            CompletableFuture<String> fut3 = client.compute()
                    .<String>executeAsync(getClusterNodes("s3"), List.of(), "job", new Object[]{}).resultAsync();

            assertThat(fut1, willBe("s3"));
            assertThat(fut2, willBe("s3"));
            assertThat(fut3, willBe("s3"));
        }
    }

    @Test
    public void testClientRetriesComputeJobOnPrimaryAndDefaultNodes() throws Exception {
        initServers(reqId -> reqId % 3 == 0);

        try (var client = getClient(server3)) {
            for (int i = 0; i < 100; i++) {
                var nodeId = i % 3 + 1;
                var nodeName = "s" + nodeId;

                CompletableFuture<String> fut = client.compute()
                        .<String>executeAsync(getClusterNodes(nodeName), List.of(), "job", new Object[]{}).resultAsync();

                assertThat(fut, willBe("s3"));
            }
        }
    }

    @Test
    public void testExecuteColocated() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server2)) {
            Tuple key = Tuple.create().set("key", "k");

            String res1 = client.compute().executeColocated(TABLE_NAME, key, List.of(), "job");
            String res2 = client.compute().executeColocated(TABLE_NAME, 1L, Mapper.of(Long.class), List.of(), "job");

            assertEquals("s2", res1);
            assertEquals("s2", res2);
        }
    }

    @Test
    public void testExecuteColocatedAsync() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server2)) {
            Tuple key = Tuple.create().set("key", "k");

            CompletableFuture<String> fut1 = client.compute()
                    .<String>executeColocatedAsync(TABLE_NAME, key, List.of(), "job").resultAsync();

            CompletableFuture<String> fut2 = client.compute()
                    .<Long, String>executeColocatedAsync(TABLE_NAME, 1L, Mapper.of(Long.class), List.of(), "job").resultAsync();

            assertThat(fut1, willBe("s2"));
            assertThat(fut2, willBe("s2"));
        }
    }

    @Test
    public void testExecuteColocatedThrowsTableNotFoundExceptionWhenTableDoesNotExist() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server1)) {
            Tuple key = Tuple.create().set("key", "k");

            var ex = assertThrows(CompletionException.class,
                    () -> client.compute().executeColocatedAsync("bad-tbl", key, List.of(), "job").resultAsync().join());

            var tblNotFoundEx = (TableNotFoundException) ex.getCause();
            assertThat(tblNotFoundEx.getMessage(), containsString("The table does not exist [name=\"PUBLIC\".\"bad-tbl\"]"));
            assertEquals(TABLE_NOT_FOUND_ERR, tblNotFoundEx.code());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExecuteColocatedUpdatesTableCacheOnTableDrop(boolean forceLoadAssignment) throws Exception {
        String tableName = "drop-me";

        initServers(reqId -> false);
        ((FakeIgniteTables) ignite.tables()).createTable(tableName);

        try (var client = getClient(server3)) {
            Tuple key = Tuple.create().set("key", "k");

            String res1 = client.compute()
                    .<String>executeColocatedAsync(tableName, key, List.of(), "job").resultAsync().join();

            // Drop table and create a new one with a different ID.
            ((FakeIgniteTables) ignite.tables()).dropTable(tableName);
            ((FakeIgniteTables) ignite.tables()).createTable(tableName);

            if (forceLoadAssignment) {
                Map<String, ClientTable> tables = IgniteTestUtils.getFieldValue(client.compute(), "tableCache");
                ClientTable table = tables.get(tableName);
                assertNotNull(table);
                IgniteTestUtils.setFieldValue(table, "partitionAssignment", null);
            }

            String res2 = client.compute()
                    .<Long, String>executeColocatedAsync(tableName, 1L, Mapper.of(Long.class), List.of(), "job").resultAsync().join();

            assertEquals("s3", res1);
            assertEquals("s3", res2);
        }
    }

    @Test
    void testUnitsPropagation() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server1)) {
            assertThat(getUnits(client, List.of()), willBe(""));
            assertThat(getUnits(client, List.of(new DeploymentUnit("u1", "1.2.3"))), willBe("u1:1.2.3"));
            assertThat(getUnits(client, List.of(new DeploymentUnit("u", "LaTeSt"))), willBe("u:latest"));
            assertThat(
                    getUnits(client, List.of(new DeploymentUnit("u1", "1.2.3"), new DeploymentUnit("unit2", Version.LATEST))),
                    willBe("u1:1.2.3,unit2:latest")
            );
        }
    }

    private static CompletableFuture<String> getUnits(IgniteClient client, List<DeploymentUnit> units) {
        return client.compute()
                .<String>executeAsync(getClusterNodes("s1"), units, FakeCompute.GET_UNITS, new Object[]{}).resultAsync();
    }

    @Test
    void testExceptionInJob() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server1)) {
            FakeCompute.future = CompletableFuture.failedFuture(new RuntimeException("job failed"));

            CompletableFuture<Object> fut = client.compute().executeAsync(getClusterNodes("s1"), List.of(), "job").resultAsync();

            assertThat(
                    fut,
                    willThrowFast(IgniteException.class)
            );
        }
    }

    private void initServers(Function<Integer, Boolean> shouldDropConnection) {
        ignite = new FakeIgnite();
        ((FakeIgniteTables) ignite.tables()).createTable(TABLE_NAME);

        var clusterId = UUID.randomUUID();

        server1 = new TestServer(0, ignite, shouldDropConnection, null, "s1", clusterId, null, null);
        server2 = new TestServer(0, ignite, shouldDropConnection, null, "s2", clusterId, null, null);
        server3 = new TestServer(0, ignite, shouldDropConnection, null, "s3", clusterId, null, null);
    }
}
