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

package org.apache.ignite.internal.sql.engine.kill;

import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType.COMPUTE;
import static org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType.QUERY;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.expectQueryCancelled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.apache.ignite.internal.sql.engine.systemviews.ItComputeSystemViewTest.InfiniteJob;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.awaitility.Awaitility;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Integration tests for SQL '{@code KILL}' statement.
 */
public class ItSqlKillCommandTest extends BaseSqlIntegrationTest {
    @BeforeAll
    void createTestTable() {
        sql("CREATE TABLE test (id INT PRIMARY KEY)");
    }

    @AfterEach
    public void checkResourceLeak() {
        sql("DELETE FROM test");

        waitUntilRunningQueriesCount(is(0));
        assertThat(txManager().pending(), is(0));
    }

    @ParameterizedTest
    @EnumSource(CancellableOperationType.class)
    public void killQueryMetadata(CancellableOperationType type) {
        assertQuery(format("KILL {} '00000000-0000-0000-0000-000000000000'", type))
                .columnMetadata(
                        new MetadataMatcher().name("APPLIED").type(ColumnType.BOOLEAN).nullable(false)
                )
                .check();
    }

    @ParameterizedTest
    @EnumSource(CancellableOperationType.class)
    public void killWithInvalidIdentifier(CancellableOperationType type) {
        Consumer<String> exceptionChecker = query -> {
            SqlException err = assertThrowsSqlException(
                    SqlException.class,
                    Sql.RUNTIME_ERR,
                    format("Invalid operation ID format [operationId=123, type={}]", type),
                    () -> await(igniteSql().executeAsync(format("KILL  {} '123' NO WAIT", type)))
            );

            assertThat(err.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(err.getCause().getMessage(), equalTo("Invalid UUID string: 123"));
        };

        exceptionChecker.accept(format("KILL {} '123'", type));
        exceptionChecker.accept(format("KILL {} '123' NO WAIT", type));
    }

    @ParameterizedTest
    @EnumSource(CancellableOperationType.class)
    public void killNonExistentOperation(CancellableOperationType type) {
        assertThat(executeKill(CLUSTER.aliveNode(), type, UUID.randomUUID(), false), is(false));

        // NO WAIT should never return false.
        assertThat(executeKill(CLUSTER.aliveNode(), type, UUID.randomUUID(), true), is(true));
    }

    @Test
    public void killQueryFromLocal() {
        Ignite node = CLUSTER.aliveNode();

        AsyncSqlCursor<InternalSqlRow> cursor = await(executeQueryInternal(node, "SELECT 1"));

        List<QueryInfo> queries = runningQueries();
        assertThat(queries.size(), is(1));
        UUID targetQueryId = queries.get(0).id();

        assertThat(executeKillSqlQuery(node, targetQueryId), is(true));

        waitUntilRunningQueriesCount(is(0));
        expectQueryCancelled(new DrainCursor(cursor));

        assertThat(executeKillSqlQuery(node, targetQueryId), is(false));
        assertThat(executeKill(node, QUERY, targetQueryId, true), is(true));
    }

    @Test
    public void killComputeJobFromLocal() {
        Ignite node = CLUSTER.aliveNode();
        JobExecution<Void> execution = submit(node, JobDescriptor.builder(InfiniteJob.class).build());

        UUID jobId = await(execution.idAsync());
        assertThat(jobId, not(nullValue()));
        assertThat(executeKillJob(node, jobId), is(true));

        Awaitility.await().until(execution::stateAsync, willBe(jobStateWithStatus(CANCELED)));

        assertThat(executeKillJob(node, jobId), is(false));
        assertThat(executeKill(node, COMPUTE, jobId, true), is(true));
    }

    @Test
    public void killQueryFromRemote() {
        Ignite local = CLUSTER.node(0);
        Ignite remote = CLUSTER.node(2);

        // Simple query.
        {
            AsyncSqlCursor<InternalSqlRow> cursor = await(executeQueryInternal(local, "SELECT 1"));

            List<QueryInfo> queries = runningQueries();
            assertThat(queries.size(), is(1));
            UUID targetQueryId = queries.get(0).id();

            assertThat(executeKill(remote, QUERY, targetQueryId, false), is(true));

            waitUntilRunningQueriesCountInCluster(is(0));
            expectQueryCancelled(new DrainCursor(cursor));
        }

        // Long query.
        {
            String query = "INSERT INTO test SELECT x AS id FROM TABLE(system_range(0, 1000000000))";
            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> curFut = executeQueryInternal(local, query);

            Awaitility.await().untilAsserted(() -> assertThat(runningQueries(), hasSize(1)));
            UUID targetQueryId = runningQueries().get(0).id();

            assertThat(executeKill(remote, QUERY, targetQueryId, false), is(true));

            waitUntilRunningQueriesCountInCluster(is(0));
            expectQueryCancelled(() -> await(curFut));
        }
    }

    @Test
    public void killQueryFromRemoteNoWait() {
        Ignite local = CLUSTER.node(0);
        Ignite remote = CLUSTER.node(2);

        // Simple query.
        {
            AsyncSqlCursor<InternalSqlRow> cursor = await(executeQueryInternal(local, "SELECT 1"));

            List<QueryInfo> queries = runningQueries();
            assertThat(queries.size(), is(1));
            UUID targetQueryId = queries.get(0).id();

            assertThat(executeKill(remote, QUERY, targetQueryId, true), is(true));

            waitUntilRunningQueriesCountInCluster(is(0));
            expectQueryCancelled(new DrainCursor(cursor));
        }

        // Long query.
        {
            String query = "INSERT INTO test SELECT x AS id FROM TABLE(system_range(0, 1000000000))";
            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> curFut = executeQueryInternal(local, query);

            Awaitility.await().untilAsserted(() -> assertThat(runningQueries(), hasSize(1)));
            UUID targetQueryId = runningQueries().get(0).id();

            assertThat(executeKill(remote, QUERY, targetQueryId, true), is(true));

            waitUntilRunningQueriesCountInCluster(is(0));
            expectQueryCancelled(() -> await(curFut));
        }
    }

    @Test
    public void killComputeJobFromRemote() {
        Ignite local = CLUSTER.node(0);
        Ignite remote = CLUSTER.node(2);

        // Single execution.
        {
            JobExecution<Void> execution = submit(local, JobDescriptor.builder(InfiniteJob.class).build());

            UUID jobId = await(execution.idAsync());
            assertThat(jobId, not(nullValue()));
            assertThat(executeKillJob(remote, jobId), is(true));

            Awaitility.await().until(execution::stateAsync, willBe(jobStateWithStatus(CANCELED)));

            assertThat(executeKillJob(remote, jobId), is(false));
            assertThat(executeKillJob(local, jobId), is(false));
        }

        // Single execution with nowait.
        {
            JobExecution<Void> execution = submit(local, JobDescriptor.builder(InfiniteJob.class).build());

            UUID jobId = await(execution.idAsync());
            assertThat(jobId, not(nullValue()));
            assertThat(executeKill(remote, COMPUTE, jobId, true), is(true));

            Awaitility.await().until(execution::stateAsync, willBe(jobStateWithStatus(CANCELED)));

            assertThat(executeKill(remote, COMPUTE, jobId, true), is(true));
        }

        // Multiple executions.
        {
            JobDescriptor<Void, Void> job = JobDescriptor.builder(InfiniteJob.class).units(List.of()).build();
            Collection<JobExecution<Void>> executions = submit(local, Set.of(clusterNode(0), clusterNode(1)), job);

            executions.forEach(execution -> {
                ClusterNode node = execution.node();
                UUID jobId = await(execution.idAsync());
                assertThat(jobId, not(nullValue()));
                assertThat("Node=" + node.name(), executeKillJob(remote, jobId), is(true));

                Awaitility.await().until(execution::stateAsync, willBe(jobStateWithStatus(CANCELED)));

                assertThat("Node=" + node.name(), executeKillJob(remote, jobId), is(false));
                assertThat("Node=" + node.name(), executeKillJob(local, jobId), is(false));
            });
        }
    }

    private static boolean executeKillSqlQuery(Ignite node, UUID queryId) {
        return executeKill(node, QUERY, queryId, false);
    }

    private static boolean executeKillJob(Ignite node, UUID jobId) {
        return executeKill(node, COMPUTE, jobId, false);
    }

    private static boolean executeKill(Ignite node, CancellableOperationType type, UUID queryId, boolean noWait) {
        String query = format("KILL {} '{}'{}", type, queryId, noWait ? " NO WAIT" : "");

        try (ResultSet<SqlRow> res = node.sql().execute(query)) {
            assertThat(res.hasRowSet(), is(false));

            return res.wasApplied();
        }
    }

    private static List<QueryInfo> runningQueries() {
        return CLUSTER.runningNodes()
                .flatMap(node -> ((SqlQueryProcessor) unwrapIgniteImpl(node).queryEngine()).runningQueries().stream())
                .collect(Collectors.toList());
    }

    private static CompletableFuture<AsyncSqlCursor<InternalSqlRow>> executeQueryInternal(Ignite node, String query) {
        IgniteImpl ignite = unwrapIgniteImpl(node);

        return ignite.queryEngine().queryAsync(
                new SqlProperties(),
                ignite.observableTimeTracker(),
                null,
                null,
                query
        );
    }

    private static void waitUntilRunningQueriesCountInCluster(Matcher<Integer> matcher) {
        SqlTestUtils.waitUntilRunningQueriesCount(CLUSTER, matcher);
    }

    private static JobExecution<Void> submit(Ignite node, JobDescriptor<Void, Void> job) {
        CompletableFuture<JobExecution<Void>> executionFut = node.compute().submitAsync(JobTarget.node(clusterNode(node)), job, null);
        assertThat(executionFut, willCompleteSuccessfully());
        return executionFut.join();
    }

    private static Collection<JobExecution<Void>> submit(Ignite node, Set<ClusterNode> nodes, JobDescriptor<Void, Void> job) {
        CompletableFuture<BroadcastExecution<Void>> executionFut = node.compute().submitAsync(BroadcastJobTarget.nodes(nodes), job, null);
        assertThat(executionFut, willCompleteSuccessfully());
        return executionFut.join().executions();
    }
}
