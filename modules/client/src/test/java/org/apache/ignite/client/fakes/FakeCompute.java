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

package org.apache.ignite.client.fakes;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.AnyNodeJobTarget;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.ColocatedJobTarget;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.TaskState;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.ExecutionContext;
import org.apache.ignite.internal.compute.HybridTimestampProvider;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.JobExecutionContextImpl;
import org.apache.ignite.internal.compute.JobStateImpl;
import org.apache.ignite.internal.compute.MarshallerProvider;
import org.apache.ignite.internal.compute.SharedComputeUtils;
import org.apache.ignite.internal.compute.TaskStateImpl;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata;
import org.apache.ignite.internal.compute.events.ComputeEventMetadataBuilder;
import org.apache.ignite.internal.compute.loader.JobClassLoader;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.PublicClusterNodeImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Fake {@link IgniteCompute}.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class FakeCompute implements IgniteComputeInternal {
    public static final String GET_UNITS = "get-units";

    public static volatile @Nullable CompletableFuture future;

    public static volatile @Nullable RuntimeException err;

    public static volatile HybridTimestamp observableTimestamp = HybridTimestamp.MIN_VALUE;

    private final Map<UUID, JobState> jobStates = new ConcurrentHashMap<>();

    public static volatile CountDownLatch latch = new CountDownLatch(0);

    private final String nodeName;

    private final Ignite ignite;

    public FakeCompute(String nodeName, Ignite ignite) {
        this.nodeName = nodeName;
        this.ignite = ignite;
    }

    @Override
    public CompletableFuture<JobExecution<ComputeJobDataHolder>> executeAsyncWithFailover(
            Set<InternalClusterNode> nodes,
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    ) {
        String jobClassName = executionContext.jobClassName();
        if (Objects.equals(jobClassName, GET_UNITS)) {
            String unitString = executionContext.units().stream().map(DeploymentUnit::render).collect(Collectors.joining(","));
            return completedExecution(unitString);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        var err0 = err;
        if (err0 != null) {
            throw err0;
        }

        if (jobClassName.startsWith("org.apache.ignite")) {
            JobClassLoader jobClassLoader = new JobClassLoader(List.of(), this.getClass().getClassLoader());
            Class<ComputeJob<Object, Object>> jobClass = ComputeUtils.jobClass(jobClassLoader, jobClassName);
            ComputeJob<Object, Object> job = ComputeUtils.instantiateJob(jobClass);
            CompletableFuture<Object> jobFut = job.executeAsync(
                    new JobExecutionContextImpl(ignite, new AtomicBoolean(), jobClassLoader, null),
                    SharedComputeUtils.unmarshalArgOrResult(executionContext.arg(), null, null));

            return jobExecution(jobFut != null ? jobFut : nullCompletedFuture());
        }

        var future0 = future;

        CompletableFuture<ComputeJobDataHolder> resFut =
                future0 != null
                        ? future0
                        : completedFuture(SharedComputeUtils.marshalArgOrResult(nodeName, null, observableTimestamp.longValue()));

        return jobExecution(resFut);
    }

    @Override
    public CompletableFuture<JobExecution<ComputeJobDataHolder>> submitColocatedInternal(
            TableViewInternal table,
            Tuple key,
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    ) {
        return jobExecution(future != null
                ? future
                : completedFuture(SharedComputeUtils.marshalArgOrResult(nodeName, null, observableTimestamp.longValue())));
    }

    @Override
    public CompletableFuture<JobExecution<ComputeJobDataHolder>> submitPartitionedInternal(
            TableViewInternal table,
            int partitionId,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable ComputeJobDataHolder arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return nullCompletedFuture();
    }

    @Override
    public <T, R> TaskExecution<R> submitMapReduceInternal(
            TaskDescriptor<T, R> taskDescriptor,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return taskExecution(future != null ? future : completedFuture((R) nodeName));
    }

    @Override
    public <T, R> CompletableFuture<JobExecution<R>> submitAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        if (target instanceof AnyNodeJobTarget) {
            Set<InternalClusterNode> nodes = ((AnyNodeJobTarget) target).nodes().stream()
                    .map(ClusterNodeImpl::fromPublicClusterNode)
                    .collect(toUnmodifiableSet());

            return executeAsyncWithFailover(
                    nodes,
                    new ExecutionContext(
                            descriptor,
                            ComputeEventMetadata.builder(),
                            SharedComputeUtils.marshalArgOrResult(arg, null, observableTimestamp.longValue())
                    ),
                    cancellationToken
            ).thenApply(internalExecution -> unmarshalingExecution(descriptor, internalExecution));
        } else if (target instanceof ColocatedJobTarget) {
            return jobExecution(future != null ? future : completedFuture((R) nodeName));
        } else {
            throw new IllegalArgumentException("Unsupported job target: " + target);
        }
    }

    @Override
    public <T, R> CompletableFuture<BroadcastExecution<R>> submitAsync(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return nullCompletedFuture();
    }

    // Don't inline this - it triggers javac bug on JDK 17
    private static <T, R> JobExecution<R> unmarshalingExecution(
            JobDescriptor<T, R> descriptor,
            JobExecution<ComputeJobDataHolder> internalExecution
    ) {
        return new JobExecution<>() {
            @Override
            public CompletableFuture<R> resultAsync() {
                return internalExecution.resultAsync()
                        .thenApply(r -> SharedComputeUtils.unmarshalArgOrResult(
                                r, descriptor.resultMarshaller(), descriptor.resultClass()));
            }

            @Override
            public CompletableFuture<@Nullable JobState> stateAsync() {
                return internalExecution.stateAsync();
            }

            @Override
            public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
                return internalExecution.changePriorityAsync(newPriority);
            }

            @Override
            public ClusterNode node() {
                return internalExecution.node();
            }
        };
    }

    @Override
    public <T, R> R execute(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T args,
            @Nullable CancellationToken cancellationToken
    ) {
        return sync(executeAsync(target, descriptor, args, cancellationToken));
    }

    @Override
    public <T, R> Collection<R> execute(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return sync(executeAsync(target, descriptor, arg, cancellationToken));
    }

    @Override
    public <T, R> TaskExecution<R> submitMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return submitMapReduceInternal(taskDescriptor, null, arg, cancellationToken);
    }

    @Override
    public <T, R> R executeMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return sync(executeMapReduceAsync(taskDescriptor, arg, cancellationToken));
    }

    private <R> CompletableFuture<JobExecution<ComputeJobDataHolder>> completedExecution(R result) {
        return jobExecution(completedFuture(SharedComputeUtils.marshalArgOrResult(result, null, observableTimestamp.longValue())));
    }

    private <R> CompletableFuture<JobExecution<ComputeJobDataHolder>> jobExecution(CompletableFuture<R> result) {
        UUID jobId = UUID.randomUUID();

        JobState state = JobStateImpl.builder()
                .id(jobId)
                .status(EXECUTING)
                .createTime(Instant.now())
                .startTime(Instant.now())
                .build();
        jobStates.put(jobId, state);

        result.whenComplete((r, throwable) -> {
            JobStatus status = throwable != null ? FAILED : COMPLETED;
            JobState newState = JobStateImpl.toBuilder(state).status(status).finishTime(Instant.now()).build();
            jobStates.put(jobId, newState);
        });

        return completedFuture(new FakeJobExecution<>(result.thenApply(r -> r instanceof ComputeJobDataHolder
                ? (ComputeJobDataHolder) r
                : SharedComputeUtils.marshalArgOrResult(r, null, observableTimestamp.longValue())), jobId));
    }

    private class FakeJobExecution<R> implements JobExecution<R>, MarshallerProvider<R> {
        private final CompletableFuture<R> result;
        private final UUID jobId;

        private FakeJobExecution(CompletableFuture<R> result, UUID jobId) {
            this.result = result;
            this.jobId = jobId;
        }

        @Override
        public CompletableFuture<R> resultAsync() {
            return result;
        }

        @Override
        public CompletableFuture<@Nullable JobState> stateAsync() {
            return completedFuture(jobStates.get(jobId));
        }

        @Override
        public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
            return trueCompletedFuture();
        }

        @Override
        public ClusterNode node() {
            return new PublicClusterNodeImpl(UUID.randomUUID(), nodeName, new NetworkAddress("local-host", 65535));
        }

        @Override
        public Marshaller<R, byte[]> resultMarshaller() {
            return null;
        }

        @Override
        public boolean marshalResult() {
            return false;
        }
    }

    private <R> TaskExecution<R> taskExecution(CompletableFuture<R> result) {
        BiFunction<UUID, JobStatus, JobState> toState = (id, status) ->
                JobStateImpl.builder()
                        .id(id)
                        .status(status)
                        .createTime(Instant.now())
                        .startTime(Instant.now())
                        .build();

        UUID jobId = UUID.randomUUID();
        UUID subJobId1 = UUID.randomUUID();
        UUID subJobId2 = UUID.randomUUID();

        jobStates.put(jobId, toState.apply(jobId, EXECUTING));
        jobStates.put(subJobId1, toState.apply(subJobId1, EXECUTING));
        jobStates.put(subJobId2, toState.apply(subJobId2, EXECUTING));

        result.whenComplete((r, throwable) -> {
            JobStatus status = throwable != null ? FAILED : COMPLETED;

            jobStates.put(jobId, toState.apply(jobId, status));
            jobStates.put(subJobId1, toState.apply(subJobId1, status));
            jobStates.put(subJobId2, toState.apply(subJobId2, status));
        });

        return new FakeTaskExecution<>(result, jobId, subJobId1, subJobId2, null);

    }

    class FakeTaskExecution<R> implements TaskExecution<R>, MarshallerProvider<R>, HybridTimestampProvider {
        private final CompletableFuture<R> result;
        private final UUID jobId;
        private final UUID subJobId1;
        private final UUID subJobId2;
        private final Marshaller<R, byte[]> marshaller;

        FakeTaskExecution(CompletableFuture<R> result, UUID jobId, UUID subJobId1, UUID subJobId2, Marshaller<R, byte[]> marshaller) {
            this.result = result;
            this.jobId = jobId;
            this.subJobId1 = subJobId1;
            this.subJobId2 = subJobId2;
            this.marshaller = marshaller;
        }

        @Override
        public CompletableFuture<R> resultAsync() {
            return result;
        }

        @Override
        public CompletableFuture<@Nullable TaskState> stateAsync() {
            return completedFuture(TaskStateImpl.toBuilder(jobStates.get(jobId)).build());
        }

        @Override
        public CompletableFuture<List<@Nullable JobState>> statesAsync() {
            return completedFuture(List.of(jobStates.get(subJobId1), jobStates.get(subJobId2)));
        }

        @Override
        public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
            return trueCompletedFuture();
        }

        @Override
        public @Nullable Marshaller<R, byte[]> resultMarshaller() {
            return marshaller;
        }

        @Override
        public boolean marshalResult() {
            return false;
        }

        @Override
        public long hybridTimestamp() {
            return 0;
        }
    }

    @Override
    public CompletableFuture<Collection<JobState>> statesAsync() {
        return completedFuture(jobStates.values());
    }

    @Override
    public CompletableFuture<@Nullable JobState> stateAsync(UUID jobId) {
        return completedFuture(jobStates.get(jobId));
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync(UUID jobId) {
        return trueCompletedFuture();
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(UUID jobId, int newPriority) {
        return trueCompletedFuture();
    }

    private static <R> R sync(CompletableFuture<R> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }
}
