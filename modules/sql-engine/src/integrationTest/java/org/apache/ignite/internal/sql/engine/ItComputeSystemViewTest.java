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

package org.apache.ignite.internal.sql.engine;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.sql.engine.util.Commons.closeQuiet;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.internal.testframework.matchers.TaskStateMatcher.taskStateWithStatus;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.TaskStatus;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * End-to-end tests to verify {@code COMPUTE_TASKS} system view.
 */
public class ItComputeSystemViewTest extends BaseSqlIntegrationTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void checkMeta(boolean isClient) {
        String query = "SELECT * FROM COMPUTE_TASKS";

        Ignite entryNode = isClient ? IgniteClient.builder().addresses("localhost").build() : CLUSTER.node(0);

        try {
            // Verify metadata.
            assertQuery(query)
                    .withDefaultSchema("SYSTEM")
                    .columnMetadata(
                            new MetadataMatcher().name("COORDINATOR_NODE_ID").type(ColumnType.STRING).nullable(false),
                            new MetadataMatcher().name("ID").type(ColumnType.STRING).precision(36).nullable(true),
                            new MetadataMatcher().name("STATUS").type(ColumnType.STRING).nullable(true),
                            new MetadataMatcher().name("CREATE_TIME").type(ColumnType.TIMESTAMP).nullable(true),
                            new MetadataMatcher().name("START_TIME").type(ColumnType.TIMESTAMP).nullable(true),
                            new MetadataMatcher().name("FINISH_TIME").type(ColumnType.TIMESTAMP).nullable(true)
                    )
                    .check();
        } finally {
            closeQuiet(entryNode);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void viewRunningJobs(boolean isClient) {
        Ignite entryNode = isClient ? IgniteClient.builder().addresses("localhost").build() : CLUSTER.node(0);
        Ignite targetNode = CLUSTER.node(0);

        try {
            ClockService clockService = unwrapIgniteImpl(targetNode).clockService();

            long tsBefore = clockService.now().getPhysical();

            JobDescriptor<Void, Void> job = JobDescriptor.builder(InfiniteJob.class).units(List.of()).build();
            JobExecution<Void> execution = entryNode.compute().submit(JobTarget.node(clusterNode(targetNode)), job, null);

            await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

            long tsAfter = clockService.now().getPhysical();

            String query = "SELECT * FROM SYSTEM.COMPUTE_TASKS WHERE STATUS = ?";

            List<List<Object>> res = sql(0, query, EXECUTING.name());

            assertThat(res, Matchers.hasSize(1));

            verifyComputeJobState(res.get(0), List.of(targetNode.name()), EXECUTING.name(), tsBefore, tsAfter);

            IgniteTestUtils.await(execution.cancelAsync());

            await().until(execution::stateAsync, willBe(jobStateWithStatus(CANCELED)));

            // Second Job call on different node.
            job = JobDescriptor.builder(InfiniteJob.class).units(List.of()).build();

            tsBefore = clockService.now().getPhysical();

            targetNode = CLUSTER.node(1);

            execution = entryNode.compute().submit(JobTarget.node(clusterNode(targetNode)), job, null);

            await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

            tsAfter = clockService.now().getPhysical();

            query = "SELECT * FROM SYSTEM.COMPUTE_TASKS WHERE COORDINATOR_NODE_ID = ? AND STATUS = ?";

            res = sql(0, query, targetNode.name(), EXECUTING.name());

            verifyComputeJobState(res.get(0), List.of(targetNode.name()), EXECUTING.name(), tsBefore, tsAfter);

            IgniteTestUtils.await(execution.cancelAsync());

            await().until(execution::stateAsync, willBe(jobStateWithStatus(CANCELED)));
        } finally {
            closeQuiet(entryNode);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void viewRunningBroadcasts(boolean isClient) {
        Ignite entryNode = isClient ? IgniteClient.builder().addresses("localhost").build() : CLUSTER.node(0);

        try {
            ClockService clockService = unwrapIgniteImpl(CLUSTER.node(0)).clockService();

            long tsBefore = clockService.now().getPhysical();

            JobDescriptor<Void, Void> job = JobDescriptor.builder(InfiniteJob.class).units(List.of()).build();
            Map<ClusterNode, JobExecution<Void>> execution = entryNode.compute().submitBroadcast(
                    Set.of(clusterNode(CLUSTER.node(0)), clusterNode(CLUSTER.node(1))), job, null);

            execution.forEach((k, exec) -> await().until(exec::stateAsync, willBe(jobStateWithStatus(EXECUTING))));

            long tsAfter = clockService.now().getPhysical();

            String query = "SELECT * FROM SYSTEM.COMPUTE_TASKS WHERE STATUS = ?";

            List<List<Object>> res = sql(0, query, EXECUTING.name());

            assertThat(res.size(), is(2));
            List<String> execNodes = List.of(CLUSTER.node(0).name(), CLUSTER.node(1).name());

            verifyComputeJobState(res.get(0), execNodes, EXECUTING.name(), tsBefore, tsAfter);
            verifyComputeJobState(res.get(1), execNodes, EXECUTING.name(), tsBefore, tsAfter);

            execution.forEach((k, exec) -> IgniteTestUtils.await(exec.cancelAsync()));
        } finally {
            closeQuiet(entryNode);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void viewRunningMapReduceTask(boolean isClient) {
        Ignite entryNode = isClient ? IgniteClient.builder().addresses("localhost").build() : CLUSTER.node(0);
        Ignite targetNode = CLUSTER.node(0);

        try {
            ClockService clockService = unwrapIgniteImpl(targetNode).clockService();

            long tsBefore = clockService.now().getPhysical();

            TaskExecution<Void> execution = entryNode.compute()
                    .submitMapReduce(TaskDescriptor.builder(MapReduceTaskCustom.class).build(), null);

            await().until(execution::stateAsync, willBe(taskStateWithStatus(TaskStatus.EXECUTING)));

            long tsAfter = clockService.now().getPhysical();

            String query = "SELECT * FROM SYSTEM.COMPUTE_TASKS WHERE STATUS = ?";

            List<List<Object>> res = sql(0, query, EXECUTING.name());

            assertThat(res.size(), is(2));
            List<String> execNodes = List.of(CLUSTER.node(0).name(), CLUSTER.node(1).name());

            verifyComputeJobState(res.get(0), execNodes, EXECUTING.name(), tsBefore, tsAfter);
            verifyComputeJobState(res.get(1), execNodes, EXECUTING.name(), tsBefore, tsAfter);

            IgniteTestUtils.await(execution.cancelAsync());
        } finally {
            closeQuiet(entryNode);
        }
    }

    private static class InfiniteMapReduceJob implements ComputeJob<Void, Void> {
        @Override
        public CompletableFuture<Void> executeAsync(JobExecutionContext context, Void input) {
            return new CompletableFuture<>();
        }
    }

    private static class MapReduceTaskCustom implements MapReduceTask<Void, Void, Void, Void> {
        @Override
        public CompletableFuture<List<MapReduceJob<Void, Void>>> splitAsync(TaskExecutionContext taskContext, @Nullable Void input) {
            return completedFuture(List.of(
                    MapReduceJob.<Void, Void>builder()
                            .jobDescriptor(JobDescriptor.builder(InfiniteMapReduceJob.class).build())
                            .nodes(taskContext.ignite().clusterNodes())
                            .build()
            ));
        }

        @Override
        public CompletableFuture<Void> reduceAsync(TaskExecutionContext taskContext, Map<UUID, Void> results) {
            return completedFuture(null);
        }
    }

    /** Infinite job. */
    public static class InfiniteJob implements ComputeJob<Void, Void> {
        @Override
        public @Nullable CompletableFuture<Void> executeAsync(JobExecutionContext context, @Nullable Void arg) {
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // No op, just return from loop
                    break;
                }
            }

            return null;
        }
    }

    private static void verifyComputeJobState(
            List<Object> row,
            List<String> nodeName,
            String phase,
            long tsBefore,
            long tsAfter
    ) {
        int idx = 0;

        // INITIATOR_NODE
        assertThat(nodeName, hasItem((String) row.get(idx++)));

        // ID
        assertThat((String) row.get(idx++), hasLength(36));

        // PHASE
        assertThat(row.get(idx++), equalTo(phase));

        // CREATE_TIME
        assertThat(((Instant) row.get(idx++)).toEpochMilli(), Matchers.allOf(greaterThanOrEqualTo(tsBefore), lessThanOrEqualTo(tsAfter)));

        // START_TIME
        assertThat(((Instant) row.get(idx++)).toEpochMilli(), Matchers.allOf(greaterThanOrEqualTo(tsBefore), lessThanOrEqualTo(tsAfter)));

        // FINISH_TIME
        // Asynchronously updated and eventually can be null
        if (row.get(idx) != null) {
            assertThat(((Instant) row.get(idx)).toEpochMilli(),
                    Matchers.allOf(greaterThanOrEqualTo(tsBefore), greaterThanOrEqualTo(tsAfter)));
        }
    }
}
