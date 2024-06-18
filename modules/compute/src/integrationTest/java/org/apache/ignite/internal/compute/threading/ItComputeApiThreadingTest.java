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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.compute.IgniteComputeImpl;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Enum;

@SuppressWarnings("resource")
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
        IgniteCompute compute = computeForPublicUse();

        CompletableFuture<Thread> completerFuture = operation.executeOn(compute)
                .thenApply(unused -> currentThread());

        assertThat(completerFuture, willBe(either(is(currentThread())).or(asyncContinuationPool())));
    }

    private static IgniteCompute computeForPublicUse() {
        return CLUSTER.aliveNode().compute();
    }

    @CartesianTest
    void computeFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(@Enum ComputeAsyncOperation operation) {
        IgniteCompute compute = computeForInternalUse();

        CompletableFuture<Thread> completerFuture = operation.executeOn(compute)
                .thenApply(unused -> currentThread());

        assertThat(completerFuture, willBe(either(is(currentThread())).or(anIgniteThread())));
    }

    private static IgniteCompute computeForInternalUse() {
        return Wrappers.unwrap(CLUSTER.aliveNode().compute(), IgniteComputeImpl.class);
    }

    @CartesianTest
    void jobExecutionFuturesCompleteInContinuationsPool(
            @Enum ComputeSubmitOperation submitOperation,
            @Enum JobExecutionAsyncOperation executionOperation
    ) {
        JobExecution<?> execution = submitOperation.executeOn(computeForPublicUse());

        CompletableFuture<Thread> completerFuture = executionOperation.executeOn(execution)
                        .thenApply(unused -> currentThread());

        assertThat(completerFuture, willBe(
                either(is(currentThread())).or(asyncContinuationPool())
        ));
    }

    @CartesianTest
    void jobExecutionFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(
            @Enum ComputeSubmitOperation submitOperation,
            @Enum JobExecutionAsyncOperation executionOperation
    ) {
        JobExecution<?> execution = submitOperation.executeOn(computeForInternalUse());

        CompletableFuture<Thread> completerFuture = executionOperation.executeOn(execution)
                .thenApply(unused -> currentThread());

        assertThat(completerFuture, willBe(
                either(is(currentThread())).or(anIgniteThread())
        ));
    }

    private static Set<ClusterNode> justNonEntryNode() {
        return Set.of(CLUSTER.node(1).node());
    }

    private static class NoOpJob implements ComputeJob<String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Object... args) {
            return completedFuture("ok");
        }
    }

    private enum ComputeAsyncOperation {
        EXECUTE_ASYNC(compute -> compute.executeAsync(JobTarget.anyNode(justNonEntryNode()), JobDescriptor.builder(NoOpJob.class).build())),
        EXECUTE_COLOCATED_BY_TUPLE_ASYNC(compute ->
                compute.executeColocatedAsync(TABLE_NAME, KEY_TUPLE, JobDescriptor.builder(NoOpJob.class).build())),
        EXECUTE_COLOCATED_BY_KEY_ASYNC(compute ->
                compute.executeColocatedAsync(TABLE_NAME, KEY, Mapper.of(Integer.class), JobDescriptor.builder(NoOpJob.class).build())),
        EXECUTE_BROADCAST_ASYNC(compute -> compute.executeBroadcastAsync(justNonEntryNode(), JobDescriptor.builder(NoOpJob.class).build()));

        private final Function<IgniteCompute, CompletableFuture<?>> action;

        ComputeAsyncOperation(Function<IgniteCompute, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(IgniteCompute compute) {
            return action.apply(compute);
        }
    }

    private enum ComputeSubmitOperation {
        SUBMIT(compute -> compute.submit(JobTarget.anyNode(justNonEntryNode()), JobDescriptor.builder(NoOpJob.class).build())),
        SUBMIT_COLOCATED_BY_TUPLE(compute -> compute.submitColocated(TABLE_NAME, KEY_TUPLE, JobDescriptor.builder(NoOpJob.class).build())),
        SUBMIT_COLOCATED_BY_KEY(compute -> compute.submitColocated(
                TABLE_NAME, KEY, Mapper.of(Integer.class), JobDescriptor.builder(NoOpJob.class).build())
        ),
        SUBMIT_BROADCAST(compute -> compute
                .submitBroadcast(justNonEntryNode(), JobDescriptor.builder(NoOpJob.class).build())
                .values().iterator().next()
        );

        private final Function<IgniteCompute, JobExecution<?>> action;

        ComputeSubmitOperation(Function<IgniteCompute, JobExecution<?>> action) {
            this.action = action;
        }

        JobExecution<?> executeOn(IgniteCompute compute) {
            return action.apply(compute);
        }
    }

    private enum JobExecutionAsyncOperation {
        RESULT_ASYNC(execution -> execution.resultAsync()),
        STATUS_ASYNC(execution -> execution.statusAsync()),
        ID_ASYNC(execution -> execution.idAsync()),
        CANCEL_ASYNC(execution -> execution.cancelAsync()),
        CHANGE_PRIORITY_ASYNC(execution -> execution.changePriorityAsync(1));

        private final Function<JobExecution<Object>, CompletableFuture<?>> action;

        JobExecutionAsyncOperation(Function<JobExecution<Object>, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(JobExecution<?> execution) {
            return action.apply((JobExecution<Object>) execution);
        }
    }
}
