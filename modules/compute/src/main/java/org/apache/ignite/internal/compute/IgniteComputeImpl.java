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

package org.apache.ignite.internal.compute;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;
import static org.apache.ignite.marshalling.Marshaller.tryMarshalOrCast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.compute.AnyNodeJobTarget;
import org.apache.ignite.compute.ColocatedJobTarget;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.NodeNotFoundException;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.client.proto.StreamerReceiverSerializer;
import org.apache.ignite.internal.compute.streamer.StreamerReceiverJob;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.CancelHandleHelper;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.ReceiverDescriptor;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link IgniteCompute}.
 */
public class IgniteComputeImpl implements IgniteComputeInternal, StreamerReceiverRunner {
    private final TopologyService topologyService;

    private final IgniteTablesInternal tables;

    private final ComputeComponent computeComponent;

    private final PlacementDriver placementDriver;

    private final HybridClock clock;

    /**
     * Create new instance.
     */
    public IgniteComputeImpl(PlacementDriver placementDriver, TopologyService topologyService,
            IgniteTablesInternal tables, ComputeComponent computeComponent,
            HybridClock clock) {
        this.placementDriver = placementDriver;
        this.topologyService = topologyService;
        this.tables = tables;
        this.computeComponent = computeComponent;
        this.clock = clock;

        tables.setStreamerReceiverRunner(this);
    }

    <T, R> JobExecution<R> submit(JobTarget target, JobDescriptor<T, R> descriptor, @Nullable CancellationToken cancellationToken,
            @Nullable T arg) {
        Objects.requireNonNull(target);
        Objects.requireNonNull(descriptor);

        Marshaller<T, byte[]> argumentMarshaller = descriptor.argumentMarshaller();
        Marshaller<R, byte[]> resultMarshaller = descriptor.resultMarshaller();

        if (target instanceof AnyNodeJobTarget) {
            Set<ClusterNode> nodes = ((AnyNodeJobTarget) target).nodes();

            return new ResultUnmarshallingJobExecution<>(
                    executeAsyncWithFailover(
                            nodes, descriptor.units(), descriptor.jobClassName(), descriptor.options(), cancellationToken,
                            tryMarshalOrCast(argumentMarshaller, arg)
                    ),
                    resultMarshaller
            );
        }

        if (target instanceof ColocatedJobTarget) {
            ColocatedJobTarget colocatedTarget = (ColocatedJobTarget) target;
            var mapper = (Mapper<? super Object>) colocatedTarget.keyMapper();
            String tableName = colocatedTarget.tableName();
            Object key = colocatedTarget.key();

            CompletableFuture<JobExecution<R>> jobFut;
            if (mapper != null) {
                jobFut = requiredTable(tableName)
                        .thenCompose(table -> primaryReplicaForPartitionByMappedKey(table, key, mapper)
                                .thenApply(primaryNode -> executeOnOneNodeWithFailover(
                                        primaryNode,
                                        new NextColocatedWorkerSelector<>(placementDriver, topologyService, clock, table, key, mapper),
                                        descriptor.units(),
                                        descriptor.jobClassName(),
                                        descriptor.options(), cancellationToken,
                                        tryMarshalOrCast(argumentMarshaller, arg)
                                )));

            } else {
                jobFut = requiredTable(tableName)
                        .thenCompose(table -> submitColocatedInternal(
                                table,
                                (Tuple) key,
                                descriptor.units(),
                                descriptor.jobClassName(),
                                descriptor.options(),
                                cancellationToken,
                                tryMarshalOrCast(argumentMarshaller, arg)))
                        .thenApply(job -> (JobExecution<R>) job);
            }

            return new ResultUnmarshallingJobExecution<>(new JobExecutionFutureWrapper<>(jobFut), resultMarshaller);
        }

        throw new IllegalArgumentException("Unsupported job target: " + target);
    }

    @Override
    public <T, R> JobExecution<R> submit(JobTarget target, JobDescriptor<T, R> descriptor, @Nullable T arg) {
        return submit(target, descriptor, null, arg);
    }

    @Override
    public <T, R> CompletableFuture<R> executeAsync(JobTarget target, JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken, @Nullable T arg) {
        return submit(target, descriptor, cancellationToken, arg).resultAsync();
    }

    @Override
    public <T, R> R execute(JobTarget target, JobDescriptor<T, R> descriptor, @Nullable CancellationToken cancellationToken,
            @Nullable T args) {
        return sync(executeAsync(target, descriptor, cancellationToken, args));
    }

    @Override
    public <R> JobExecution<R> executeAsyncWithFailover(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            @Nullable CancellationToken cancellationToken,
            @Nullable Object arg
    ) {
        Set<ClusterNode> candidates = new HashSet<>();
        for (ClusterNode node : nodes) {
            if (topologyService.getByConsistentId(node.name()) != null) {
                candidates.add(node);
            }
        }
        if (candidates.isEmpty()) {
            Set<String> nodeNames = nodes.stream().map(ClusterNode::name).collect(Collectors.toSet());
            return new FailedExecution<>(new NodeNotFoundException(nodeNames));
        }

        ClusterNode targetNode = randomNode(candidates);
        candidates.remove(targetNode);

        NextWorkerSelector selector = new DeqNextWorkerSelector(new ConcurrentLinkedDeque<>(candidates));

        return new JobExecutionWrapper<>(
                executeOnOneNodeWithFailover(
                        targetNode,
                        selector,
                        units,
                        jobClassName,
                        options,
                        cancellationToken,
                        arg
                ));
    }

    private static ClusterNode randomNode(Set<ClusterNode> nodes) {
        int nodesToSkip = ThreadLocalRandom.current().nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }

    private <T, R> JobExecution<R> executeOnOneNodeWithFailover(
            ClusterNode targetNode,
            NextWorkerSelector nextWorkerSelector,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions jobExecutionOptions,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        ExecutionOptions options = ExecutionOptions.from(jobExecutionOptions);
        return executeOnOneNodeWithFailover(targetNode, nextWorkerSelector, units, jobClassName, options, cancellationToken, arg);
    }

    private <T, R> JobExecution<R> executeOnOneNodeWithFailover(
            ClusterNode targetNode,
            NextWorkerSelector nextWorkerSelector,
            List<DeploymentUnit> units,
            String jobClassName,
            ExecutionOptions options,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(options, units, jobClassName, cancellationToken, arg);
        } else {
            return computeComponent.executeRemotelyWithFailover(
                    targetNode, nextWorkerSelector, units, jobClassName, options, cancellationToken, arg);
        }
    }

    private static class DeqNextWorkerSelector implements NextWorkerSelector {
        private final ConcurrentLinkedDeque<ClusterNode> deque;

        private DeqNextWorkerSelector(ConcurrentLinkedDeque<ClusterNode> deque) {
            this.deque = deque;
        }

        @Override
        public CompletableFuture<ClusterNode> next() {
            try {
                return completedFuture(deque.pop());
            } catch (NoSuchElementException ex) {
                return nullCompletedFuture();
            }
        }
    }

    private boolean isLocal(ClusterNode targetNode) {
        return targetNode.name().equals(topologyService.localMember().name());
    }

    @Override
    public <R> CompletableFuture<JobExecution<R>> submitColocatedInternal(
            TableViewInternal table,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            @Nullable CancellationToken cancellationToken,
            @Nullable Object arg) {
        return primaryReplicaForPartitionByTupleKey(table, key)
                .thenApply(primaryNode -> executeOnOneNodeWithFailover(
                        primaryNode,
                        new NextColocatedWorkerSelector<>(placementDriver, topologyService, clock, table, key),
                        units, jobClassName, options, cancellationToken, arg
                ));
    }

    private CompletableFuture<TableViewInternal> requiredTable(String tableName) {
        String parsedName = IgniteNameUtils.parseSimpleName(tableName);

        return tables.tableViewAsync(parsedName)
                .thenApply(table -> {
                    if (table == null) {
                        throw new TableNotFoundException(SqlCommon.DEFAULT_SCHEMA_NAME, parsedName);
                    }
                    return table;
                });
    }

    private CompletableFuture<ClusterNode> primaryReplicaForPartitionByTupleKey(TableViewInternal table, Tuple key) {
        return primaryReplicaForPartition(table, table.partition(key));
    }

    private <K> CompletableFuture<ClusterNode> primaryReplicaForPartitionByMappedKey(TableViewInternal table, K key,
            Mapper<K> keyMapper) {
        return primaryReplicaForPartition(table, table.partition(key, keyMapper));
    }

    private CompletableFuture<ClusterNode> primaryReplicaForPartition(TableViewInternal table, int partitionIndex) {
        TablePartitionId tablePartitionId = new TablePartitionId(table.tableId(), partitionIndex);

        return placementDriver.awaitPrimaryReplica(tablePartitionId, clock.now(), 30, TimeUnit.SECONDS)
                .thenApply(replicaMeta -> {
                    if (replicaMeta != null && replicaMeta.getLeaseholderId() != null) {
                        return topologyService.getById(replicaMeta.getLeaseholderId());
                    }

                    throw new ComputeException(
                            Compute.PRIMARY_REPLICA_RESOLVE_ERR,
                            "Can not find primary replica for [table=" + table.name() + ", partition=" + partitionIndex + "]."
                    );
                });
    }

    @Override
    public <T, R> Map<ClusterNode, JobExecution<R>> submitBroadcast(
            Set<ClusterNode> nodes,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(descriptor);

        return nodes.stream()
                .collect(toUnmodifiableMap(identity(),
                        node -> executeWithoutFailover(node, descriptor, ExecutionOptions.from(descriptor.options()), null, arg)
                ));
    }

    @Override
    public <T, R> CompletableFuture<Map<ClusterNode, JobExecution<R>>> submitBroadcastPartitioned(
            String tableName,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return submitBroadcastPartitioned(tableName, descriptor, null, arg);
    }

    private <T, R> CompletableFuture<Map<ClusterNode, JobExecution<R>>> submitBroadcastPartitioned(
            String tableName,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(descriptor);

        return requiredTable(tableName)
                .thenCompose(table -> table.partitionManager().primaryReplicasAsync())
                .thenApply(replicas -> {
                    Map<ClusterNode, List<Integer>> partitioning = new HashMap<>();
                    for (Entry<Partition, ClusterNode> entry : replicas.entrySet()) {
                        partitioning.computeIfAbsent(entry.getValue(), k -> new ArrayList<>())
                                .add(((HashPartition) entry.getKey()).partitionId());
                    }

                    return partitioning.entrySet().stream().collect(toUnmodifiableMap(
                            Entry::getKey,
                            entry -> executePartitioned(entry.getKey(), entry.getValue(), descriptor, cancellationToken, arg))
                    );
                });
    }

    private <T, R> JobExecution<R> executePartitioned(
            ClusterNode node,
            List<Integer> partitions,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        ExecutionOptions options = ExecutionOptions.builder()
                .priority(descriptor.options().priority())
                .maxRetries(descriptor.options().maxRetries())
                .partitions(partitions)
                .build();
        return executeWithoutFailover(node, descriptor, options, cancellationToken, arg);
    }

    private <T, R> JobExecution<R> executeWithoutFailover(
            ClusterNode node,
            JobDescriptor<T, R> descriptor,
            ExecutionOptions options,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        Marshaller<T, byte[]> argumentMarshaller = descriptor.argumentMarshaller();
        Marshaller<R, byte[]> resultMarshaller = descriptor.resultMarshaller();

        // No failover nodes for broadcast. We use failover here in order to complete futures with exceptions
        // if worker node has left the cluster.
        if (topologyService.getByConsistentId(node.name()) == null) {
            return new FailedExecution<>(new NodeNotFoundException(Set.of(node.name())));
        }
        return new ResultUnmarshallingJobExecution<>(
                new JobExecutionWrapper<>(
                        executeOnOneNodeWithFailover(
                                node, CompletableFutures::nullCompletedFuture,
                                descriptor.units(), descriptor.jobClassName(),
                                options, cancellationToken, tryMarshalOrCast(argumentMarshaller, arg))),
                resultMarshaller
        );
    }

    @Override
    public <T, R> CompletableFuture<Map<ClusterNode, R>> executeBroadcastPartitionedAsync(
            String tableName,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        return submitBroadcastPartitioned(tableName, descriptor, cancellationToken, arg)
                .thenCompose(executions -> {
                    Map<ClusterNode, CompletableFuture<R>> futures = executions.entrySet().stream()
                            .collect(toMap(Entry::getKey, entry -> entry.getValue().resultAsync()));

                    return allOf(futures.values().toArray(CompletableFuture[]::new))
                            .thenApply(ignored -> {
                                        Map<ClusterNode, R> map = new HashMap<>();

                                        for (Entry<ClusterNode, CompletableFuture<R>> entry : futures.entrySet()) {
                                            map.put(entry.getKey(), entry.getValue().join());
                                        }

                                        return map;
                                    }
                            );
                });
    }

    @Override
    public <T, R> Map<ClusterNode, R> executeBroadcastPartitioned(
            String tableName,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        return sync(executeBroadcastPartitionedAsync(tableName, descriptor, cancellationToken, arg));
    }

    @Override
    public <T, R> TaskExecution<R> submitMapReduce(TaskDescriptor<T, R> taskDescriptor, @Nullable T arg) {
        return submitMapReduce(taskDescriptor, null, arg);
    }

    private <T, R> TaskExecution<R> submitMapReduce(TaskDescriptor<T, R> taskDescriptor, @Nullable CancellationToken cancellationToken,
            @Nullable T arg) {
        Objects.requireNonNull(taskDescriptor);

        TaskExecutionWrapper<R> execution = new TaskExecutionWrapper<>(
                computeComponent.executeTask(this::submitJob, taskDescriptor.units(), taskDescriptor.taskClassName(), arg));

        if (cancellationToken != null) {
            CancelHandleHelper.addCancelAction(cancellationToken, execution::cancelAsync, execution.resultAsync());
        }

        return execution;
    }

    @Override
    public <T, R> CompletableFuture<R> executeMapReduceAsync(TaskDescriptor<T, R> taskDescriptor,
            @Nullable CancellationToken cancellationToken, @Nullable T arg) {
        return submitMapReduce(taskDescriptor, cancellationToken, arg).resultAsync();
    }

    @Override
    public <T, R> R executeMapReduce(TaskDescriptor<T, R> taskDescriptor, @Nullable CancellationToken cancellationToken, @Nullable T arg) {
        return sync(executeMapReduceAsync(taskDescriptor, cancellationToken, arg));
    }

    private <M, T> JobExecution<T> submitJob(MapReduceJob<M, T> runner) {
        return submit(JobTarget.anyNode(runner.nodes()), runner.jobDescriptor(), runner.arg());
    }

    @Override
    public CompletableFuture<Collection<JobState>> statesAsync() {
        return computeComponent.statesAsync();
    }

    @Override
    public CompletableFuture<@Nullable JobState> stateAsync(UUID jobId) {
        return computeComponent.stateAsync(jobId);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync(UUID jobId) {
        return computeComponent.cancelAsync(jobId);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(UUID jobId, int newPriority) {
        return computeComponent.changePriorityAsync(jobId, newPriority);
    }

    @Override
    public <A, I, R> CompletableFuture<Collection<R>> runReceiverAsync(
            ReceiverDescriptor<A> receiver,
            @Nullable A receiverArg,
            Collection<I> items,
            ClusterNode node,
            List<DeploymentUnit> deploymentUnits) {
        var payload = StreamerReceiverSerializer.serializeReceiverInfoWithElementCount(receiver, receiverArg, items);

        return runReceiverAsync(payload, node, deploymentUnits)
                .thenApply(StreamerReceiverSerializer::deserializeReceiverJobResults);
    }

    @Override
    public CompletableFuture<byte[]> runReceiverAsync(byte[] payload, ClusterNode node, List<DeploymentUnit> deploymentUnits) {
        // Use Compute to execute receiver on the target node with failover, class loading, scheduling.
        JobExecution<byte[]> jobExecution = executeAsyncWithFailover(
                Set.of(node),
                deploymentUnits,
                StreamerReceiverJob.class.getName(),
                JobExecutionOptions.DEFAULT,
                null,
                payload);

        return jobExecution.resultAsync()
                .handle((res, err) -> {
                    if (err != null) {
                        if (err.getCause() instanceof ComputeException) {
                            ComputeException computeErr = (ComputeException) err.getCause();
                            throw new IgniteException(
                                    COMPUTE_JOB_FAILED_ERR,
                                    "Streamer receiver failed: " + computeErr.getMessage(), computeErr);
                        }

                        ExceptionUtils.sneakyThrow(err);
                    }

                    return res;
                });
    }

    @TestOnly
    ComputeComponent computeComponent() {
        return computeComponent;
    }

    private static <R> R sync(CompletableFuture<R> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(mapToPublicException(unwrapCause(e)));
        }
    }
}
