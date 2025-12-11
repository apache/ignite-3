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

package org.apache.ignite.internal.client.compute;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.client.TcpIgniteClient.unpackClusterNode;
import static org.apache.ignite.lang.ErrorGroups.Client.TABLE_ID_NOT_FOUND_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.compute.AllNodesBroadcastJobTarget;
import org.apache.ignite.compute.AnyNodeJobTarget;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.ColocatedJobTarget;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TableJobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientComputeJobPacker;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.table.ClientRecordSerializer;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.client.table.ClientTables;
import org.apache.ignite.internal.client.table.ClientTupleSerializer;
import org.apache.ignite.internal.client.table.PartitionAwarenessProvider;
import org.apache.ignite.internal.compute.BroadcastJobExecutionImpl;
import org.apache.ignite.internal.compute.FailedExecution;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.ViewUtils;
import org.apache.ignite.lang.CancelHandleHelper;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.jetbrains.annotations.Nullable;

/**
 * Client compute implementation.
 */
public class ClientCompute implements IgniteCompute {
    /** Channel. */
    private final ReliableChannel ch;

    /** Tables. */
    private final ClientTables tables;

    /** Cached tables. */
    private final ConcurrentHashMap<QualifiedName, ClientTable> tableCache = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param tables Tales.
     */
    public ClientCompute(ReliableChannel ch, ClientTables tables) {
        this.ch = ch;
        this.tables = tables;
    }

    @Override
    public <T, R> CompletableFuture<JobExecution<R>> submitAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        Objects.requireNonNull(target);
        Objects.requireNonNull(descriptor);

        return submit0(target, descriptor, arg).thenApply(submitResult -> {
            ClientJobExecution<R> execution = new ClientJobExecution<>(
                    ch,
                    submitResult,
                    descriptor.resultMarshaller(),
                    descriptor.resultClass()
            );

            if (cancellationToken != null) {
                CancelHandleHelper.addCancelAction(cancellationToken, execution::cancelAsync, execution.resultAsync());
            }

            return execution;
        });
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

        // Assign common task ID to all jobs
        UUID taskId = UUID.randomUUID();

        if (target instanceof AllNodesBroadcastJobTarget) {
            AllNodesBroadcastJobTarget allNodesBroadcastTarget = (AllNodesBroadcastJobTarget) target;
            Set<ClusterNode> nodes = allNodesBroadcastTarget.nodes();

            //noinspection unchecked
            CompletableFuture<SubmitResult>[] futures = nodes.stream()
                    .map(node -> executeOnAnyNodeAsync(Set.of(node), descriptor, taskId, arg))
                    .toArray(CompletableFuture[]::new);

            return mapSubmitFutures(futures, descriptor, cancellationToken);
        } else if (target instanceof TableJobTarget) {
            TableJobTarget tableJobTarget = (TableJobTarget) target;
            QualifiedName tableName = tableJobTarget.tableName();
            return getTable(tableName)
                    .thenCompose(table -> table.partitionDistribution().primaryReplicasAsync())
                    .thenCompose(replicas -> {
                        //noinspection unchecked
                        CompletableFuture<SubmitResult>[] futures = replicas.keySet().stream()
                                .map(partition -> doExecutePartitionedAsync(tableName, partition, descriptor, taskId, arg))
                                .toArray(CompletableFuture[]::new);

                        return mapSubmitFutures(futures, descriptor, cancellationToken);
                    });
        }

        throw new IllegalArgumentException("Unsupported job target: " + target);
    }

    private <T, R> CompletableFuture<BroadcastExecution<R>> mapSubmitFutures(
            CompletableFuture<SubmitResult>[] futures,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken
    ) {
        // Wait for all the futures but don't fail resulting future, keep individual futures in executions.
        return allOf(futures).handle((unused, throwable) -> new BroadcastJobExecutionImpl<>(
                Arrays.stream(futures)
                        .map(fut -> mapSubmitResult(descriptor, cancellationToken, fut))
                        .collect(Collectors.toList())
        ));
    }

    private <T, R> JobExecution<R> mapSubmitResult(
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            CompletableFuture<SubmitResult> submitResultFut
    ) {
        SubmitResult submitResult;
        try {
            submitResult = submitResultFut.join();
        } catch (Exception e) {
            return new FailedExecution<>(ExceptionUtils.unwrapCause(e));
        }
        ClientJobExecution<R> execution = new ClientJobExecution<>(
                ch,
                submitResult,
                descriptor.resultMarshaller(),
                descriptor.resultClass()
        );
        if (cancellationToken != null) {
            CancelHandleHelper.addCancelAction(cancellationToken, execution::cancelAsync, execution.resultAsync());
        }
        return execution;
    }

    private <T, R> CompletableFuture<SubmitResult> submit0(JobTarget target, JobDescriptor<T, R> descriptor, T arg) {
        if (target instanceof AnyNodeJobTarget) {
            AnyNodeJobTarget anyNodeJobTarget = (AnyNodeJobTarget) target;

            return executeOnAnyNodeAsync(anyNodeJobTarget.nodes(), descriptor, null, arg);
        }

        if (target instanceof ColocatedJobTarget) {
            ColocatedJobTarget colocatedTarget = (ColocatedJobTarget) target;
            var mapper = (Mapper<? super Object>) colocatedTarget.keyMapper();

            QualifiedName qualifiedName = colocatedTarget.tableName();

            if (mapper != null) {
                return doExecuteColocatedAsync(qualifiedName, colocatedTarget.key(), mapper, descriptor, arg);
            } else {
                return doExecuteColocatedAsync(qualifiedName, (Tuple) colocatedTarget.key(), descriptor, arg);
            }
        }

        throw new IllegalArgumentException("Unsupported job target: " + target);
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

    private <T, R> CompletableFuture<SubmitResult> doExecuteColocatedAsync(
            QualifiedName tableName,
            Tuple key,
            JobDescriptor<T, R> descriptor,
            T arg
    ) {
        return getTable(tableName)
                .thenCompose(table -> executeColocatedTupleKey(table, key, descriptor, arg))
                .handle((res, err) -> handleMissingTable(
                        tableName,
                        res,
                        err,
                        () -> doExecuteColocatedAsync(tableName, key, descriptor, arg)
                ))
                .thenCompose(Function.identity());
    }

    private <K, T, R> CompletableFuture<SubmitResult> doExecuteColocatedAsync(
            QualifiedName tableName,
            K key,
            Mapper<K> keyMapper,
            JobDescriptor<T, R> descriptor,
            T arg
    ) {
        return getTable(tableName)
                .thenCompose(table -> executeColocatedObjectKey(table, key, keyMapper, descriptor, arg))
                .handle((res, err) -> handleMissingTable(
                        tableName,
                        res,
                        err,
                        () -> doExecuteColocatedAsync(tableName, key, keyMapper, descriptor, arg)
                ))
                .thenCompose(Function.identity());
    }

    @Override
    public <T, R> TaskExecution<R> submitMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        Objects.requireNonNull(taskDescriptor);

        ClientTaskExecution<R> clientExecution = new ClientTaskExecution<>(ch,
                doExecuteMapReduceAsync(taskDescriptor, arg),
                taskDescriptor.reduceJobResultMarshaller(),
                taskDescriptor.reduceJobResultClass()
        );

        if (cancellationToken != null) {
            CancelHandleHelper.addCancelAction(cancellationToken, clientExecution::cancelAsync, clientExecution.resultAsync());
        }

        return clientExecution;
    }

    @Override
    public <T, R> R executeMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return sync(executeMapReduceAsync(taskDescriptor, arg, cancellationToken));
    }

    private <T, R> CompletableFuture<SubmitTaskResult> doExecuteMapReduceAsync(TaskDescriptor<T, R> taskDescriptor, @Nullable T arg) {
        return ch.serviceAsync(
                ClientOp.COMPUTE_EXECUTE_MAPREDUCE,
                w -> packTask(w.out(), taskDescriptor, arg),
                ClientCompute::unpackSubmitTaskResult,
                (String) null,
                null,
                true
        );
    }

    private <T, R> CompletableFuture<SubmitResult> executeOnAnyNodeAsync(
            Set<ClusterNode> nodes,
            JobDescriptor<T, R> descriptor,
            @Nullable UUID taskId,
            @Nullable T arg
    ) {
        ClusterNode node = randomNode(nodes);

        return ch.serviceAsync(
                ClientOp.COMPUTE_EXECUTE,
                w -> {
                    packNodeNames(w.out(), nodes);
                    packJob(w, descriptor, arg);
                    packTaskId(w, taskId);
                },
                ClientCompute::unpackSubmitResult,
                node.name(),
                null,
                true
        );
    }

    private static ClusterNode randomNode(Set<ClusterNode> nodes) {
        if (nodes.size() == 1) {
            return nodes.iterator().next();
        }

        int nodesToSkip = ThreadLocalRandom.current().nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }

    private static <K, T, R> CompletableFuture<SubmitResult> executeColocatedObjectKey(
            ClientTable t,
            K key,
            Mapper<K> keyMapper,
            JobDescriptor<T, R> descriptor,
            T arg
    ) {
        return executeColocatedInternal(
                t,
                (outputChannel, schema) ->
                        ClientRecordSerializer.writeRecRaw(key, keyMapper, schema, outputChannel.out(), TuplePart.KEY, true),
                ClientTupleSerializer.getPartitionAwarenessProvider(keyMapper, key),
                descriptor,
                arg
        );
    }

    private static <T, R> CompletableFuture<SubmitResult> executeColocatedTupleKey(
            ClientTable t,
            Tuple key,
            JobDescriptor<T, R> descriptor,
            T arg
    ) {
        ClientTupleSerializer ser = new ClientTupleSerializer(t.tableId(), t::qualifiedName);
        return executeColocatedInternal(
                t,
                (outputChannel, schema) -> ser.writeTupleRaw(key, schema, outputChannel, true),
                ClientTupleSerializer.getPartitionAwarenessProvider(key),
                descriptor,
                arg
        );
    }

    private static <T, R> CompletableFuture<SubmitResult> executeColocatedInternal(
            ClientTable t,
            BiConsumer<PayloadOutputChannel, ClientSchema> keyWriter,
            PartitionAwarenessProvider partitionAwarenessProvider,
            JobDescriptor<T, R> descriptor,
            T arg
    ) {
        return t.doSchemaOutOpAsync(
                ClientOp.COMPUTE_EXECUTE_COLOCATED,
                (schema, outputChannel, unused) -> {
                    ClientMessagePacker w = outputChannel.out();

                    w.packInt(t.tableId());
                    w.packInt(schema.version());

                    keyWriter.accept(outputChannel, schema);

                    packJob(outputChannel, descriptor, arg);
                    packTaskId(outputChannel, null); // Placeholder for a possible future usage
                },
                ClientCompute::unpackSubmitResult,
                partitionAwarenessProvider,
                true,
                null);
    }

    private <T, R> CompletableFuture<SubmitResult> doExecutePartitionedAsync(
            QualifiedName tableName,
            Partition partition,
            JobDescriptor<T, R> descriptor,
            UUID taskId,
            @Nullable T arg
    ) {
        return getTable(tableName).thenCompose(table -> executePartitioned(table, partition, descriptor, taskId, arg)
                .handle((res, err) -> handleMissingTable(
                        tableName,
                        res,
                        err,
                        () -> doExecutePartitionedAsync(tableName, partition, descriptor, taskId, arg))
                )
                .thenCompose(Function.identity()));
    }

    private static <T, R> CompletableFuture<SubmitResult> executePartitioned(
            ClientTable t,
            Partition partition,
            JobDescriptor<T, R> descriptor,
            UUID taskId,
            @Nullable T arg
    ) {
        long partitionId = partition.id();
        return t.doSchemaOutOpAsync(
                ClientOp.COMPUTE_EXECUTE_PARTITIONED,
                (schema, outputChannel, unused) -> {
                    ClientMessagePacker w = outputChannel.out();

                    w.packInt(t.tableId());

                    w.packLong(partitionId);

                    packJob(outputChannel, descriptor, arg);
                    packTaskId(outputChannel, taskId);
                },
                ClientCompute::unpackSubmitResult,
                PartitionAwarenessProvider.of(Math.toIntExact(partitionId)),
                true,
                null
        );
    }

    private CompletableFuture<ClientTable> getTable(QualifiedName tableName) {
        // Cache tables by name to avoid extra network call on every executeColocated.
        var cached = tableCache.get(tableName);

        if (cached != null) {
            return completedFuture(cached);
        }

        return tables.tableAsync(tableName).thenApply(t -> {
            if (t == null) {
                throw new TableNotFoundException(tableName);
            }

            ClientTable clientTable = (ClientTable) t;
            tableCache.put(t.qualifiedName(), clientTable);

            return clientTable;
        });
    }

    private CompletableFuture<SubmitResult> handleMissingTable(
            QualifiedName tableName,
            SubmitResult res,
            Throwable err,
            Supplier<CompletableFuture<SubmitResult>> retry
    ) {
        if (err instanceof CompletionException) {
            err = err.getCause();
        }

        if (err instanceof IgniteException) {
            IgniteException clientEx = (IgniteException) err;

            if (clientEx.code() == TABLE_ID_NOT_FOUND_ERR) {
                // Table was dropped - remove from cache.
                tableCache.remove(tableName);

                return retry.get();
            }
        }

        if (err != null) {
            throw new CompletionException(err);
        }

        return completedFuture(res);
    }

    private static void packNodeNames(ClientMessagePacker w, Set<ClusterNode> nodes) {
        w.packInt(nodes.size());
        for (ClusterNode node : nodes) {
            w.packString(node.name());
        }
    }

    private static <T, R> void packJob(PayloadOutputChannel out, JobDescriptor<T, R> descriptor, T arg) {
        boolean platformComputeSupported = out.clientChannel().protocolContext()
                .isFeatureSupported(ProtocolBitmaskFeature.PLATFORM_COMPUTE_JOB);

        ClientComputeJobPacker.packJob(descriptor, arg, platformComputeSupported, out.out());
    }

    private static void packTaskId(PayloadOutputChannel out, @Nullable UUID taskId) {
        if (out.clientChannel().protocolContext().isFeatureSupported(ProtocolBitmaskFeature.COMPUTE_TASK_ID)) {
            out.out().packUuidNullable(taskId);
        }
    }

    private static <T, R> void packTask(ClientMessagePacker w, TaskDescriptor<T, R> taskDescriptor, @Nullable T arg) {
        w.packDeploymentUnits(taskDescriptor.units());
        w.packString(taskDescriptor.taskClassName());
        ClientComputeJobPacker.packJobArgument(arg, taskDescriptor.splitJobArgumentMarshaller(), w);
    }

    /**
     * Unpacks job id from channel and gets notification future. This is needed because we need to unpack message response in the payload
     * reader because the unpacker will be closed after the response is processed.
     *
     * @param ch Payload channel.
     * @return Result of the job submission.
     */
    private static SubmitResult unpackSubmitResult(PayloadInputChannel ch) {
        //noinspection DataFlowIssue (reviewed)
        return new SubmitResult(
                ch.in().unpackUuid(),
                unpackClusterNode(ch),
                ch.notificationFuture()
        );
    }

    /**
     * Unpacks coordination job id and jobs ids which are executing under this task from channel and gets notification future. This is
     * needed because we need to unpack message response in the payload reader because the unpacker will be closed after the response is
     * processed.
     *
     * @param ch Payload channel.
     * @return Result of the task submission.
     */
    private static SubmitTaskResult unpackSubmitTaskResult(PayloadInputChannel ch) {
        var jobId = ch.in().unpackUuid();

        var size = ch.in().unpackInt();
        List<UUID> jobIds = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            jobIds.add(ch.in().unpackUuid());
        }

        //noinspection DataFlowIssue (reviewed)
        return new SubmitTaskResult(
                jobId,
                jobIds,
                ch.clientChannel().protocolContext().clusterNode(), // Task is always executed on a client handler node
                ch.notificationFuture()
        );
    }

    private static <R> R sync(CompletableFuture<R> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ViewUtils.ensurePublicException(e));
        }
    }
}
