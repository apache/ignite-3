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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.compute.ComputeUtils.convertToComputeFuture;
import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import java.util.stream.Stream;
import org.apache.ignite.compute.AllNodesBroadcastJobTarget;
import org.apache.ignite.compute.AnyNodeJobTarget;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.ColocatedJobTarget;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobExecutorType;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.NodeNotFoundException;
import org.apache.ignite.compute.TableJobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.client.proto.StreamerReceiverSerializer;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.compute.events.ComputeEventMetadataBuilder;
import org.apache.ignite.internal.compute.streamer.StreamerReceiverJob;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ZonePartitionId;
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
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.ReceiverExecutionOptions;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link IgniteCompute}.
 */
public class IgniteComputeImpl implements IgniteComputeInternal, StreamerReceiverRunner {
    private final String nodeName;

    private final PlacementDriver placementDriver;

    private final TopologyService topologyService;

    private final IgniteTablesInternal tables;

    private final ComputeComponent computeComponent;

    private final HybridClock clock;

    private final HybridTimestampTracker observableTimestampTracker;

    /**
     * Create new instance.
     */
    public IgniteComputeImpl(
            String nodeName,
            PlacementDriver placementDriver,
            TopologyService topologyService,
            IgniteTablesInternal tables,
            ComputeComponent computeComponent,
            HybridClock clock,
            HybridTimestampTracker observableTimestampTracker
    ) {
        this.nodeName = nodeName;
        this.placementDriver = placementDriver;
        this.topologyService = topologyService;
        this.tables = tables;
        this.computeComponent = computeComponent;
        this.clock = clock;
        this.observableTimestampTracker = observableTimestampTracker;

        tables.setStreamerReceiverRunner(this);
    }

    @Override
    public <T, R> CompletableFuture<JobExecution<R>> submitAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return submitAsync(target, descriptor, ComputeEventMetadata.builder(Type.SINGLE), arg, cancellationToken);
    }

    private <T, R> CompletableFuture<JobExecution<R>> submitAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        Objects.requireNonNull(target);
        Objects.requireNonNull(descriptor);

        ComputeJobDataHolder argHolder = SharedComputeUtils.marshalArgOrResult(arg, descriptor.argumentMarshaller());
        ExecutionContext executionContext = new ExecutionContext(descriptor, metadataBuilder, argHolder);

        if (target instanceof AnyNodeJobTarget) {
            Set<InternalClusterNode> nodes = internalNodesFromPublicNodes(((AnyNodeJobTarget) target).nodes());

            return unmarshalResult(
                    executeAsyncWithFailover(nodes, executionContext, cancellationToken),
                    descriptor,
                    observableTimestampTracker
            );
        }

        if (target instanceof ColocatedJobTarget) {
            ColocatedJobTarget colocatedTarget = (ColocatedJobTarget) target;
            var mapper = (Mapper<? super Object>) colocatedTarget.keyMapper();
            QualifiedName tableName = colocatedTarget.tableName();
            Object key = colocatedTarget.key();

            executionContext.metadataBuilder().tableName(tableName.toCanonicalForm());

            CompletableFuture<JobExecution<ComputeJobDataHolder>> jobFut;
            if (mapper != null) {
                jobFut = requiredTable(tableName)
                        .thenCompose(table -> primaryReplicaForPartitionByMappedKey(table, key, mapper)
                                .thenCompose(primaryNode -> executeOnOneNodeWithFailover(
                                        primaryNode,
                                        new NextColocatedWorkerSelector<>(
                                                placementDriver,
                                                topologyService,
                                                clock,
                                                table,
                                                key,
                                                mapper
                                        ),
                                        executionContext,
                                        cancellationToken
                                )));

            } else {
                jobFut = requiredTable(tableName)
                        .thenCompose(table -> submitColocatedInternal(
                                table,
                                (Tuple) key,
                                executionContext,
                                cancellationToken
                        ));
            }

            return unmarshalResult(jobFut, descriptor, observableTimestampTracker);
        }

        throw new IllegalArgumentException("Unsupported job target: " + target);
    }

    @Override
    public <T, R> CompletableFuture<BroadcastExecution<R>> submitAsync(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        Objects.requireNonNull(target);
        Objects.requireNonNull(descriptor);

        ComputeJobDataHolder argHolder = SharedComputeUtils.marshalArgOrResult(arg, descriptor.argumentMarshaller());

        // Assign common task ID to all jobs
        UUID taskId = UUID.randomUUID();

        if (target instanceof AllNodesBroadcastJobTarget) {
            AllNodesBroadcastJobTarget allNodesBroadcastTarget = (AllNodesBroadcastJobTarget) target;
            Set<InternalClusterNode> nodes = internalNodesFromPublicNodes(allNodesBroadcastTarget.nodes());

            return toBroadcastExecution(nodes.stream().map(node -> submitForBroadcast(
                    node,
                    descriptor,
                    taskId,
                    argHolder,
                    cancellationToken
            )));
        } else if (target instanceof TableJobTarget) {
            TableJobTarget tableJobTarget = (TableJobTarget) target;
            return requiredTable(tableJobTarget.tableName())
                    .thenCompose(table -> table.partitionDistribution().primaryReplicasAsync()
                            .thenCompose(replicas -> toBroadcastExecution(replicas.entrySet().stream()
                                    .map(entry -> new IgniteBiTuple<>(entry.getKey(), findNodeByConsistentId(entry.getValue())))
                                    .filter(entry -> entry.getValue() != null)
                                    .map(entry -> submitForBroadcast(
                                            entry.getValue(),
                                            entry.getKey(),
                                            table.zoneId(),
                                            descriptor,
                                            tableJobTarget.tableName().toCanonicalForm(),
                                            taskId,
                                            argHolder,
                                            cancellationToken
                                    )))
                            ));
        }

        throw new IllegalArgumentException("Unsupported job target: " + target);
    }

    private static Set<InternalClusterNode> internalNodesFromPublicNodes(Set<ClusterNode> nodes) {
        return nodes.stream()
                .map(ClusterNodeImpl::fromPublicClusterNode)
                .collect(Collectors.toSet());
    }

    private @Nullable InternalClusterNode findNodeByConsistentId(ClusterNode clusterNode) {
        return topologyService.getByConsistentId(clusterNode.name());
    }

    private static <R> CompletableFuture<BroadcastExecution<R>> toBroadcastExecution(
            Stream<CompletableFuture<JobExecution<R>>> executionsStream
    ) {
        //noinspection unchecked
        CompletableFuture<JobExecution<R>>[] futures = executionsStream.toArray(CompletableFuture[]::new);
        // Wait for all the futures but don't fail resulting future, keep individual futures in executions.
        return allOf(futures).handle((unused, throwable) -> new BroadcastJobExecutionImpl<>(
                Arrays.stream(futures)
                        .map(IgniteComputeImpl::mapExecution)
                        .collect(Collectors.toList())
        ));
    }

    private static <T, R> JobExecution<R> mapExecution(CompletableFuture<JobExecution<R>> future) {
        try {
            return future.join();
        } catch (Exception e) {
            return new FailedExecution<>(unwrapCause(e));
        }
    }

    private <T, R> CompletableFuture<JobExecution<R>> submitForBroadcast(
            InternalClusterNode node,
            JobDescriptor<T, R> descriptor,
            UUID taskId,
            @Nullable ComputeJobDataHolder argHolder,
            @Nullable CancellationToken cancellationToken
    ) {
        ExecutionOptions options = ExecutionOptions.from(descriptor.options());

        // No failover nodes for broadcast. We use failover here in order to complete futures with exceptions if worker node has left the
        // cluster.
        NextWorkerSelector nextWorkerSelector = CompletableFutures::nullCompletedFuture;

        return submitForBroadcast(node, descriptor, options, nextWorkerSelector, taskId, null, argHolder, cancellationToken);
    }

    private <T, R> CompletableFuture<JobExecution<R>> submitForBroadcast(
            InternalClusterNode node,
            Partition partition,
            int zoneId,
            JobDescriptor<T, R> descriptor,
            String tableName,
            UUID taskId,
            @Nullable ComputeJobDataHolder argHolder,
            @Nullable CancellationToken cancellationToken
    ) {
        ExecutionOptions options = ExecutionOptions.builder()
                .priority(descriptor.options().priority())
                .maxRetries(descriptor.options().maxRetries())
                .executorType(descriptor.options().executorType())
                .partition(partition)
                .build();

        PartitionNextWorkerSelector nextWorkerSelector = new PartitionNextWorkerSelector(
                placementDriver, topologyService, clock,
                zoneId, partition
        );
        return submitForBroadcast(node, descriptor, options, nextWorkerSelector, taskId, tableName, argHolder, cancellationToken);
    }

    private <T, R> CompletableFuture<JobExecution<R>> submitForBroadcast(
            InternalClusterNode node,
            JobDescriptor<T, R> descriptor,
            ExecutionOptions options,
            NextWorkerSelector nextWorkerSelector,
            UUID taskId,
            @Nullable String tableName,
            @Nullable ComputeJobDataHolder argHolder,
            @Nullable CancellationToken cancellationToken
    ) {
        if (topologyService.getByConsistentId(node.name()) == null) {
            return failedFuture(new NodeNotFoundException(Set.of(node.name())));
        }

        ComputeEventMetadataBuilder metadataBuilder = ComputeEventMetadata.builder(Type.BROADCAST)
                .taskId(taskId)
                .tableName(tableName);

        return unmarshalResult(
                executeOnOneNodeWithFailover(
                        node,
                        nextWorkerSelector,
                        new ExecutionContext(options, descriptor.units(), descriptor.jobClassName(), metadataBuilder, argHolder),
                        cancellationToken
                ),
                descriptor,
                observableTimestampTracker
        );
    }

    private static <T, R> CompletableFuture<JobExecution<R>> unmarshalResult(
            CompletableFuture<JobExecution<ComputeJobDataHolder>> executionFuture,
            JobDescriptor<T, R> descriptor,
            HybridTimestampTracker observableTimestampTracker
    ) {
        return executionFuture.thenApply(execution -> new ResultUnmarshallingJobExecution<>(
                execution,
                descriptor.resultMarshaller(),
                descriptor.resultClass(),
                observableTimestampTracker
        ));
    }

    @Override
    public <T, R> R execute(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return sync(executeAsync(target, descriptor, arg, cancellationToken));
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
    public CompletableFuture<JobExecution<ComputeJobDataHolder>> executeAsyncWithFailover(
            Set<InternalClusterNode> nodes,
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    ) {
        Set<InternalClusterNode> candidates1 = new HashSet<>();
        for (InternalClusterNode node : nodes) {
            if (topologyService.getByConsistentId(node.name()) != null) {
                candidates1.add(node);
            }
        }
        Set<InternalClusterNode> candidates = candidates1;
        if (candidates.isEmpty()) {
            Set<String> nodeNames = nodes.stream().map(InternalClusterNode::name).collect(Collectors.toSet());
            return failedFuture(new NodeNotFoundException(nodeNames));
        }

        InternalClusterNode targetNode = randomNode(candidates);
        candidates.remove(targetNode);

        NextWorkerSelector selector = new DeqNextWorkerSelector(new ConcurrentLinkedDeque<>(candidates));

        return executeOnOneNodeWithFailover(targetNode, selector, executionContext, cancellationToken);
    }

    private static InternalClusterNode randomNode(Set<InternalClusterNode> nodes) {
        int nodesToSkip = ThreadLocalRandom.current().nextInt(nodes.size());

        Iterator<InternalClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }

    private CompletableFuture<JobExecution<ComputeJobDataHolder>> executeOnOneNodeWithFailover(
            InternalClusterNode targetNode,
            NextWorkerSelector nextWorkerSelector,
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    ) {
        return convertToComputeFuture(
                executeOnOneNodeWithFailoverInternal(targetNode, nextWorkerSelector, executionContext, cancellationToken)
        ).thenApply(JobExecutionWrapper::new);
    }

    private CompletableFuture<? extends JobExecution<ComputeJobDataHolder>> executeOnOneNodeWithFailoverInternal(
            InternalClusterNode targetNode,
            NextWorkerSelector nextWorkerSelector,
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    ) {
        executionContext.metadataBuilder().initiatorNode(nodeName);

        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(executionContext, cancellationToken);
        } else {
            return computeComponent.executeRemotelyWithFailover(targetNode, nextWorkerSelector, executionContext, cancellationToken);
        }
    }

    private static class DeqNextWorkerSelector implements NextWorkerSelector {
        private final ConcurrentLinkedDeque<InternalClusterNode> deque;

        private DeqNextWorkerSelector(ConcurrentLinkedDeque<InternalClusterNode> deque) {
            this.deque = deque;
        }

        @Override
        public CompletableFuture<InternalClusterNode> next() {
            try {
                return completedFuture(deque.pop());
            } catch (NoSuchElementException ex) {
                return nullCompletedFuture();
            }
        }
    }

    private boolean isLocal(InternalClusterNode targetNode) {
        return targetNode.name().equals(topologyService.localMember().name());
    }

    @Override
    public CompletableFuture<JobExecution<ComputeJobDataHolder>> submitColocatedInternal(
            TableViewInternal table,
            Tuple key,
            ExecutionContext executionContext,
            @Nullable CancellationToken cancellationToken
    ) {
        return primaryReplicaForPartitionByTupleKey(table, key)
                .thenCompose(primaryNode -> executeOnOneNodeWithFailover(
                        primaryNode,
                        new NextColocatedWorkerSelector<>(placementDriver, topologyService, clock, table, key),
                        executionContext,
                        cancellationToken
                ));
    }

    @Override
    public CompletableFuture<JobExecution<ComputeJobDataHolder>> submitPartitionedInternal(
            TableViewInternal table,
            int partitionId,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions jobExecutionOptions,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable ComputeJobDataHolder arg,
            @Nullable CancellationToken cancellationToken
    ) {
        HashPartition partition = new HashPartition(partitionId);
        ExecutionOptions options = ExecutionOptions.builder()
                .priority(jobExecutionOptions.priority())
                .maxRetries(jobExecutionOptions.maxRetries())
                .executorType(jobExecutionOptions.executorType())
                .partition(partition)
                .build();

        return primaryReplicaForPartition(table, partitionId)
                .thenCompose(primaryNode -> executeOnOneNodeWithFailover(
                        primaryNode,
                        new PartitionNextWorkerSelector(
                                placementDriver, topologyService, clock,
                                table.zoneId(), partition),
                        new ExecutionContext(options, units, jobClassName, metadataBuilder, arg),
                        cancellationToken
                ));
    }

    private CompletableFuture<TableViewInternal> requiredTable(QualifiedName tableName) {
        return tables.tableViewAsync(tableName)
                .thenApply(table -> {
                    if (table == null) {
                        throw new TableNotFoundException(tableName);
                    }
                    return table;
                });
    }

    private CompletableFuture<InternalClusterNode> primaryReplicaForPartitionByTupleKey(TableViewInternal table, Tuple key) {
        return primaryReplicaForPartition(table, table.partitionId(key));
    }

    private <K> CompletableFuture<InternalClusterNode> primaryReplicaForPartitionByMappedKey(
            TableViewInternal table,
            K key,
            Mapper<K> keyMapper
    ) {
        return primaryReplicaForPartition(table, table.partitionId(key, keyMapper));
    }

    private CompletableFuture<InternalClusterNode> primaryReplicaForPartition(TableViewInternal table, int partitionIndex) {
        ZonePartitionId replicationGroupId = new ZonePartitionId(table.zoneId(), partitionIndex);

        return placementDriver.awaitPrimaryReplica(replicationGroupId, clock.now(), 30, TimeUnit.SECONDS)
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
    public <T, R> TaskExecution<R> submitMapReduceInternal(
            TaskDescriptor<T, R> taskDescriptor,
            ComputeEventMetadataBuilder metadataBuilder,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        Objects.requireNonNull(taskDescriptor);

        CancellableTaskExecution<R> taskExecution = computeComponent.executeTask(
                this::submitJobs,
                taskDescriptor.units(),
                taskDescriptor.taskClassName(),
                metadataBuilder,
                arg
        );

        if (cancellationToken != null) {
            CancelHandleHelper.addCancelAction(cancellationToken, taskExecution::cancelAsync, taskExecution.resultAsync());
        }

        return new TaskExecutionWrapper<>(taskExecution);
    }

    @Override
    public <T, R> TaskExecution<R> submitMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return submitMapReduceInternal(taskDescriptor, ComputeEventMetadata.builder(Type.MAP_REDUCE), arg, cancellationToken);
    }

    @Override
    public <T, R> R executeMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return sync(executeMapReduceAsync(taskDescriptor, arg, cancellationToken));
    }

    private <M, T> CompletableFuture<List<JobExecution<T>>> submitJobs(
            List<MapReduceJob<M, T>> runners,
            ComputeEventMetadataBuilder metadataBuilder,
            CancellationToken cancellationToken
    ) {
        //noinspection unchecked
        return allOfToList(
                runners.stream()
                        .map(runner -> submitAsync(
                                JobTarget.anyNode(runner.nodes()),
                                runner.jobDescriptor(),
                                metadataBuilder.copyOf(), // Make a copy since the builder is mutable
                                runner.arg(),
                                cancellationToken
                        ))
                        .toArray(CompletableFuture[]::new)
        );
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
            DataStreamerReceiverDescriptor<I, A, R> receiver,
            @Nullable A receiverArg,
            Collection<I> items,
            InternalClusterNode node,
            List<DeploymentUnit> deploymentUnits) {
        var payload = StreamerReceiverSerializer.serializeReceiverInfoWithElementCount(
                receiver, receiverArg, receiver.payloadMarshaller(), receiver.argumentMarshaller(), items);

        return runReceiverAsync(payload, node, deploymentUnits, receiver.options())
                .thenApply(r -> {
                    byte[] resBytes = r.get1();

                    assert r.get2() != null : "Observable timestamp should not be null";
                    long observableTimestamp = r.get2();
                    observableTimestampTracker.update(observableTimestamp);

                    //noinspection DataFlowIssue
                    return StreamerReceiverSerializer.deserializeReceiverJobResults(resBytes, receiver.resultMarshaller());
                });
    }

    @Override
    public CompletableFuture<IgniteBiTuple<byte[], Long>> runReceiverAsync(
            byte[] payload,
            InternalClusterNode node,
            List<DeploymentUnit> deploymentUnits,
            ReceiverExecutionOptions options
    ) {
        ExecutionOptions jobOptions = ExecutionOptions.builder()
                .priority(options.priority())
                .maxRetries(options.maxRetries())
                .executorType(options.executorType())
                .build();

        ExecutionContext executionContext = new ExecutionContext(
                jobOptions,
                deploymentUnits,
                getReceiverJobClassName(options.executorType()),
                ComputeEventMetadata.builder(Type.DATA_RECEIVER),
                SharedComputeUtils.marshalArgOrResult(payload, null)
        );

        // Use Compute to execute receiver on the target node with failover, class loading, scheduling.
        return executeAsyncWithFailover(Set.of(node), executionContext, null)
                .thenCompose(JobExecution::resultAsync)
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

                    byte[] resBytes = SharedComputeUtils.unmarshalArgOrResult(res, null, null);

                    return new IgniteBiTuple<>(resBytes, res.observableTimestamp());
                });
    }

    @TestOnly
    public ComputeComponent computeComponent() {
        return computeComponent;
    }

    private static <R> R sync(CompletableFuture<R> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(mapToPublicException(unwrapCause(e)));
        }
    }

    private static String getReceiverJobClassName(JobExecutorType executorType) {
        switch (executorType) {
            case JAVA_EMBEDDED:
                return StreamerReceiverJob.class.getName();

            case DOTNET_SIDECAR:
                return "Apache.Ignite.Internal.Table.StreamerReceiverJob, Apache.Ignite";

            default:
                throw new IllegalArgumentException("Unsupported job executor type: " + executorType);
        }
    }
}
