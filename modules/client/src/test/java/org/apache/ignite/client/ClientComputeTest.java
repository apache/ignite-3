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
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.internal.testframework.matchers.TaskStateMatcher.taskStateWithStatus;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.lang.ErrorGroups.Table.TABLE_NOT_FOUND_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.TaskStatus;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
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

    private FakeIgnite ignite1;
    private FakeIgnite ignite2;
    private FakeIgnite ignite3;

    private TestServer server1;
    private TestServer server2;
    private TestServer server3;

    @AfterEach
    void tearDown() throws Exception {
        closeAll(server1, server2, server3);
        FakeCompute.future = null;
    }

    @Test
    public void testClientSendsComputeJobToTargetNodeWhenDirectConnectionExists() throws Exception {
        initServers(reqId -> false);

        // Provide same node multiple times to check this case as well.
        try (var client = getClient(server1, server2, server3, server1, server2)) {
            assertTrue(IgniteTestUtils.waitForCondition(() -> client.connections().size() == 3, 3000));

            JobDescriptor job = JobDescriptor.builder("job").build();

            JobExecution<String> execution1 = client.compute().submit(getClusterNodes("s1"), job, null);
            JobExecution<String> execution2 = client.compute().submit(getClusterNodes("s2"), job, null);
            JobExecution<String> execution3 = client.compute().submit(getClusterNodes("s3"), job, null);

            assertThat(execution1.resultAsync(), willBe("s1"));
            assertThat(execution2.resultAsync(), willBe("s2"));
            assertThat(execution3.resultAsync(), willBe("s3"));

            assertThat(execution1.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
            assertThat(execution2.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
            assertThat(execution3.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        }
    }

    @Test
    public void testClientSendsComputeJobToDefaultNodeWhenDirectConnectionToTargetDoesNotExist() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server3)) {
            JobDescriptor job = JobDescriptor.builder("job").build();

            JobExecution<String> execution1 = client.compute().submit(getClusterNodes("s1"), job, null);
            JobExecution<String> execution2 = client.compute().submit(getClusterNodes("s2"), job, null);
            JobExecution<String> execution3 = client.compute().submit(getClusterNodes("s3"), job, null);

            assertThat(execution1.resultAsync(), willBe("s3"));
            assertThat(execution2.resultAsync(), willBe("s3"));
            assertThat(execution3.resultAsync(), willBe("s3"));

            assertThat(execution1.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
            assertThat(execution2.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
            assertThat(execution3.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        }
    }

    @Test
    public void testClientRetriesComputeJobOnPrimaryAndDefaultNodes() throws Exception {
        initServers(reqId -> reqId % 3 == 0);

        try (var client = getClient(server3)) {
            for (int i = 0; i < 100; i++) {
                var nodeId = i % 3 + 1;
                var nodeName = "s" + nodeId;

                JobDescriptor job = JobDescriptor.builder("job").build();
                CompletableFuture<String> fut = client.compute().executeAsync(getClusterNodes(nodeName), job, null);

                assertThat(fut, willBe("s3"));
            }
        }
    }

    @Test
    public void testExecuteColocated() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server2)) {
            JobDescriptor<Object, String> job = JobDescriptor.<Object, String>builder("job").build();

            String res1 = client.compute().execute(JobTarget.colocated(TABLE_NAME, Tuple.create().set("key", "k")), job, null);
            String res2 = client.compute().execute(JobTarget.colocated(TABLE_NAME, 1L, Mapper.of(Long.class)), job, null);

            assertEquals("s2", res1);
            assertEquals("s2", res2);
        }
    }

    @Test
    public void testExecuteColocatedAsync() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server2)) {
            JobDescriptor job = JobDescriptor.builder("job").build();

            JobExecution<String> execution1 = client.compute().submit(JobTarget.colocated(
                    TABLE_NAME,
                    Tuple.create().set("key", "k")),
                    job,
                    null
            );
            JobExecution<String> execution2 = client.compute().submit(JobTarget.colocated(
                    TABLE_NAME,
                    1L,
                    Mapper.of(Long.class)),
                    job,
                    null
            );

            assertThat(execution1.resultAsync(), willBe("s2"));
            assertThat(execution2.resultAsync(), willBe("s2"));

            assertThat(execution1.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
            assertThat(execution2.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        }
    }

    @Test
    public void testExecuteColocatedThrowsTableNotFoundExceptionWhenTableDoesNotExist() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server1)) {
            Tuple key = Tuple.create().set("key", "k");

            var ex = assertThrows(CompletionException.class,
                    () -> client.compute().executeAsync(
                            JobTarget.colocated("bad-tbl", key), JobDescriptor.builder("job").build(), null).join());

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
        createTable(tableName);

        try (var client = getClient(server3)) {
            Tuple key = Tuple.create().set("key", "k");

            String res1 = client.compute().execute(
                    JobTarget.colocated(tableName, key), JobDescriptor.<Object, String>builder("job").build(), null
            );

            // Drop table and create a new one with a different ID.
            dropTable(tableName);
            createTable(tableName);

            if (forceLoadAssignment) {
                Map<String, ClientTable> tables = IgniteTestUtils.getFieldValue(client.compute(), "tableCache");
                ClientTable table = tables.get(tableName);
                assertNotNull(table);
                IgniteTestUtils.setFieldValue(table, "partitionAssignment", null);
            }

            String res2 = client.compute().execute(
                    JobTarget.colocated(
                    tableName,
                    1L,
                    Mapper.of(Long.class)),
                    JobDescriptor.<Object, String>builder("job").build(),
                    null
            );

            assertEquals("s3", res1);
            assertEquals("s3", res2);
        }
    }

    @Test
    void testMapReduceExecute() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server1)) {
            Object args = "arg1";
            String res1 = client.compute().executeMapReduce(TaskDescriptor.<Object, String>builder("job").build(), args);
            assertEquals("s1", res1);
        }
    }

    @Test
    void testMapReduceSubmit() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server1)) {
            IgniteCompute igniteCompute = client.compute();
            TaskExecution<Object> task = igniteCompute.submitMapReduce(
                    TaskDescriptor.builder("job").build(), null);

            assertThat(task.resultAsync(), willBe("s1"));

            assertThat(task.stateAsync(), willBe(taskStateWithStatus(TaskStatus.COMPLETED)));
            assertThat(task.statesAsync(), willBe(everyItem(jobStateWithStatus(COMPLETED))));

            assertThat("compute task and sub tasks ids must be different",
                    task.idsAsync(), willBe(not(hasItem(task.idAsync().get()))));
        }
    }

    @Test
    void testMapReduceException() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server1)) {
            FakeCompute.future = CompletableFuture.failedFuture(new RuntimeException("job failed"));

            IgniteCompute igniteCompute = client.compute();
            TaskExecution<Object> execution = igniteCompute.submitMapReduce(
                    TaskDescriptor.builder("job").build(), null);

            assertThat(execution.resultAsync(), willThrowFast(IgniteException.class));
            assertThat(execution.stateAsync(), willBe(taskStateWithStatus(TaskStatus.FAILED)));
            assertThat(execution.statesAsync(), willBe(everyItem(jobStateWithStatus(FAILED))));
        }
    }

    @Test
    void testUnitsPropagation() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server1)) {
            assertThat(getUnits(client, List.of()), is(""));
            assertThat(getUnits(client, List.of(new DeploymentUnit("u1", "1.2.3"))), is("u1:1.2.3"));
            assertThat(getUnits(client, List.of(new DeploymentUnit("u", "LaTeSt"))), is("u:latest"));
            assertThat(
                    getUnits(client, List.of(new DeploymentUnit("u1", "1.2.3"), new DeploymentUnit("unit2", Version.LATEST))),
                    is("u1:1.2.3,unit2:latest")
            );
        }
    }

    private static String getUnits(IgniteClient client, List<DeploymentUnit> units) {
        return client.compute().execute(getClusterNodes("s1"),
                JobDescriptor.<Object, String>builder(FakeCompute.GET_UNITS).units(units).build(), null);
    }

    @Test
    void testExceptionInJob() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient(server1)) {
            FakeCompute.future = CompletableFuture.failedFuture(new RuntimeException("job failed"));

            IgniteCompute igniteCompute = client.compute();
            var jobTarget = getClusterNodes("s1");
            JobExecution<String> execution = igniteCompute.submit(jobTarget, JobDescriptor.<Object, String>builder("job").build(), null);

            assertThat(execution.resultAsync(), willThrowFast(IgniteException.class));
            assertThat(execution.stateAsync(), willBe(jobStateWithStatus(FAILED)));
        }
    }

    private void initServers(Function<Integer, Boolean> shouldDropConnection) {
        ignite1 = new FakeIgnite("s1");
        ignite2 = new FakeIgnite("s2");
        ignite3 = new FakeIgnite("s3");
        createTable(TABLE_NAME);

        var clusterId = UUID.randomUUID();

        server1 = new TestServer(0, ignite1, shouldDropConnection, null, "s1", clusterId, null, null);
        server2 = new TestServer(0, ignite2, shouldDropConnection, null, "s2", clusterId, null, null);
        server3 = new TestServer(0, ignite3, shouldDropConnection, null, "s3", clusterId, null, null);
    }

    private void createTable(String tableName) {
        for (FakeIgnite ignite : List.of(ignite1, ignite2, ignite3)) {
            ((FakeIgniteTables) ignite.tables()).createTable(tableName);
        }
    }

    private void dropTable(String tableName) {
        for (FakeIgnite ignite : List.of(ignite1, ignite2, ignite3)) {
            ((FakeIgniteTables) ignite.tables()).dropTable(tableName);
        }
    }
}
