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

package org.apache.ignite.internal.compute.threading;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.PublicApiThreadingTests.anIgniteThread;
import static org.apache.ignite.internal.PublicApiThreadingTests.asyncContinuationPool;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.compute.IgniteComputeImpl;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Enum;

class ItComputeApiThreadingTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test";

    private static final int KEY = 1;

    private static final Tuple KEY_TUPLE = Tuple.create().set("id", KEY);

    @Override
    protected int initialNodes() {
        // We start 2 nodes to have ability to ask first node to execute something on second node. This ensures that executions complete
        // in network threads.
        return 2;
    }

    @BeforeAll
    void createTable() {
        sql("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR)");
    }

    private static Table testTable() {
        return CLUSTER.aliveNode().tables().table(TABLE_NAME);
    }

    @BeforeEach
    void upsertRecord() {
        KeyValueView<Integer, String> view = testTable().keyValueView(Integer.class, String.class);

        view.put(null, KEY, "one");
    }

    @CartesianTest
    void computeFuturesCompleteInContinuationsPool(@Enum ComputeAsyncOperation operation) {
        CompletableFuture<?> executionFut = operation.executeOn(computeForPublicUse());

        assertThat(completerFuture(executionFut), willBe(either(is(currentThread())).or(asyncContinuationPool())));
    }

    @CartesianTest
    void computeFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(@Enum ComputeAsyncOperation operation) {
        CompletableFuture<?> executionFut = operation.executeOn(computeForInternalUse());

        assertThat(completerFuture(executionFut), willBe(either(is(currentThread())).or(anIgniteThread())));
    }

    @CartesianTest
    void jobExecutionFuturesCompleteInContinuationsPool(
            @Enum ComputeSubmitOperation submitOperation,
            @Enum JobExecutionAsyncOperation executionOperation
    ) {
        CompletableFuture<? extends JobExecution<?>> executionFut = submitOperation.executeOn(computeForPublicUse());

        assertThat(executionFut, willCompleteSuccessfully());
        JobExecution<?> execution = executionFut.join();

        assertThat(completerFuture(executionOperation.executeOn(execution)), willBe(
                either(is(currentThread())).or(asyncContinuationPool())
        ));
    }

    @CartesianTest
    void jobExecutionFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(
            @Enum ComputeSubmitOperation submitOperation,
            @Enum JobExecutionAsyncOperation executionOperation
    ) {
        CompletableFuture<? extends JobExecution<?>> executionFut = submitOperation.executeOn(computeForInternalUse());

        assertThat(executionFut, willCompleteSuccessfully());
        JobExecution<?> execution = executionFut.join();

        assertThat(completerFuture(executionOperation.executeOn(execution)), willBe(
                either(is(currentThread())).or(anIgniteThread())
        ));
    }

    private static Set<ClusterNode> justNonEntryNode() {
        return Set.of(clusterNode(1));
    }

    @CartesianTest
    void taskExecutionFuturesCompleteInContinuationsPool(
            @Enum ComputeMapReduceOperation mapReduceOperation,
            @Enum TaskExecutionAsyncOperation executionOperation
    ) {
        TaskExecution<?> execution = mapReduceOperation.executeOn(computeForPublicUse());

        assertThat(completerFuture(executionOperation.executeOn(execution)), willBe(
                either(is(currentThread())).or(asyncContinuationPool())
        ));
    }

    @CartesianTest
    void taskExecutionFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(
            @Enum ComputeMapReduceOperation submitOperation,
            @Enum TaskExecutionAsyncOperation executionOperation
    ) {
        TaskExecution<?> execution = submitOperation.executeOn(computeForInternalUse());

        assertThat(completerFuture(executionOperation.executeOn(execution)), willBe(
                either(is(currentThread())).or(anIgniteThread())
        ));
    }

    private static IgniteCompute computeForPublicUse() {
        return CLUSTER.aliveNode().compute();
    }

    private static IgniteCompute computeForInternalUse() {
        return Wrappers.unwrap(CLUSTER.aliveNode().compute(), IgniteComputeImpl.class);
    }

    private static CompletableFuture<Thread> completerFuture(CompletableFuture<?> future) {
        return future.thenApply(unused -> currentThread());
    }

    private static class NoOpJob implements ComputeJob<Void, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Void input) {
            return completedFuture("ok");
        }
    }

    private static class NoOpMapReduceTask implements MapReduceTask<Void, Void, String, Void> {
        @Override
        public CompletableFuture<List<MapReduceJob<Void, String>>> splitAsync(TaskExecutionContext taskContext, @Nullable Void input) {
            return completedFuture(List.of(
                    MapReduceJob.<Void, String>builder()
                            .jobDescriptor(JobDescriptor.builder(NoOpJob.class).build())
                            .nodes(taskContext.ignite().cluster().nodes())
                            .build()
            ));
        }

        @Override
        public CompletableFuture<Void> reduceAsync(TaskExecutionContext taskContext, Map<UUID, String> results) {
            return nullCompletedFuture();
        }
    }

    private enum ComputeAsyncOperation {
        SUBMIT_ASYNC(compute -> compute.submitAsync(
                JobTarget.anyNode(justNonEntryNode()),
                JobDescriptor.builder(NoOpJob.class).build(),
                null)),
        SUBMIT_BROADCAST_ASYNC(compute -> compute.submitAsync(BroadcastJobTarget.nodes(justNonEntryNode()),
                JobDescriptor.builder(NoOpJob.class).build(),
                null)),
        EXECUTE_ASYNC(compute -> compute.executeAsync(
                JobTarget.anyNode(justNonEntryNode()),
                JobDescriptor.builder(NoOpJob.class).build(),
                null)),
        EXECUTE_COLOCATED_BY_TUPLE_ASYNC(compute -> compute.executeAsync(
                JobTarget.colocated(TABLE_NAME, KEY_TUPLE),
                JobDescriptor.builder(NoOpJob.class).build(), null)),
        EXECUTE_COLOCATED_BY_KEY_ASYNC(compute -> compute.executeAsync(
                JobTarget.colocated(TABLE_NAME, KEY, Mapper.of(Integer.class)),
                JobDescriptor.builder(NoOpJob.class).build(),
                null)),
        EXECUTE_BROADCAST_ASYNC(compute -> compute.executeAsync(
                BroadcastJobTarget.nodes(justNonEntryNode()),
                JobDescriptor.builder(NoOpJob.class).build(),
                null)),
        EXECUTE_MAP_REDUCE_ASYNC(compute -> compute.executeMapReduceAsync(
                TaskDescriptor.builder(NoOpMapReduceTask.class).build(),
                null)
        );

        private final Function<IgniteCompute, CompletableFuture<?>> action;

        ComputeAsyncOperation(Function<IgniteCompute, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(IgniteCompute compute) {
            return action.apply(compute);
        }
    }

    private enum ComputeSubmitOperation {
        SUBMIT_ASYNC(compute -> compute.submitAsync(
                JobTarget.anyNode(justNonEntryNode()),
                JobDescriptor.builder(NoOpJob.class).build(),
                null)),
        SUBMIT_COLOCATED_BY_TUPLE(compute -> compute.submitAsync(
                JobTarget.colocated(TABLE_NAME, KEY_TUPLE),
                JobDescriptor.builder(NoOpJob.class).build(),
                null)),
        SUBMIT_COLOCATED_BY_KEY(compute -> compute.submitAsync(
                JobTarget.colocated(TABLE_NAME, KEY, Mapper.of(Integer.class)),
                JobDescriptor.builder(NoOpJob.class).build(), null)
        ),
        SUBMIT_BROADCAST_ASYNC(compute -> compute
                .submitAsync(BroadcastJobTarget.nodes(justNonEntryNode()), JobDescriptor.builder(NoOpJob.class).build(), null)
                .thenApply(broadcastExecution -> broadcastExecution.executions().iterator().next())
        );

        private final Function<IgniteCompute, CompletableFuture<? extends JobExecution<?>>> action;

        ComputeSubmitOperation(Function<IgniteCompute, CompletableFuture<? extends JobExecution<?>>> action) {
            this.action = action;
        }

        CompletableFuture<? extends JobExecution<?>> executeOn(IgniteCompute compute) {
            return action.apply(compute);
        }
    }

    private enum JobExecutionAsyncOperation {
        RESULT_ASYNC(JobExecution::resultAsync),
        STATE_ASYNC(JobExecution::stateAsync),
        ID_ASYNC(JobExecution::idAsync),
        CHANGE_PRIORITY_ASYNC(execution -> execution.changePriorityAsync(1));

        private final Function<JobExecution<Object>, CompletableFuture<?>> action;

        JobExecutionAsyncOperation(Function<JobExecution<Object>, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(JobExecution<?> execution) {
            return action.apply((JobExecution<Object>) execution);
        }
    }

    private enum ComputeMapReduceOperation {
        SUBMIT_MAP_REDUCE(compute -> compute
                .submitMapReduce(TaskDescriptor.builder(NoOpMapReduceTask.class).build(), null)
        );

        private final Function<IgniteCompute, TaskExecution<?>> action;

        ComputeMapReduceOperation(Function<IgniteCompute, TaskExecution<?>> action) {
            this.action = action;
        }

        TaskExecution<?> executeOn(IgniteCompute compute) {
            return action.apply(compute);
        }
    }

    private enum TaskExecutionAsyncOperation {
        STATES_ASYNC(TaskExecution::statesAsync),
        IDS_ASYNC(TaskExecution::idsAsync),
        RESULT_ASYNC(TaskExecution::resultAsync),
        STATE_ASYNC(TaskExecution::stateAsync),
        ID_ASYNC(TaskExecution::idAsync),
        CHANGE_PRIORITY_ASYNC(execution -> execution.changePriorityAsync(1));

        private final Function<TaskExecution<Object>, CompletableFuture<?>> action;

        TaskExecutionAsyncOperation(Function<TaskExecution<Object>, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(TaskExecution<?> execution) {
            return action.apply((TaskExecution<Object>) execution);
        }
    }
}
