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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.lang.ErrorGroups.Client.TABLE_ID_NOT_FOUND_ERR;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.TaskExecution;
import org.apache.ignite.internal.client.ClientUtils;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.table.ClientRecordSerializer;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.client.table.ClientTables;
import org.apache.ignite.internal.client.table.ClientTupleSerializer;
import org.apache.ignite.internal.client.table.PartitionAwarenessProvider;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Client compute implementation.
 */
public class ClientCompute implements IgniteCompute {
    /** Channel. */
    private final ReliableChannel ch;

    /** Tables. */
    private final ClientTables tables;

    /** Cached tables. */
    private final ConcurrentHashMap<String, ClientTable> tableCache = new ConcurrentHashMap<>();

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

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> submit(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args) {
        Objects.requireNonNull(options);
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        return new ClientJobExecution<>(ch, executeOnNodesAsync(nodes, units, jobClassName, options, args));
    }

    /** {@inheritDoc} */
    @Override
    public <R> R execute(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return sync(executeAsync(nodes, units, jobClassName, options, args));
    }

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> submitColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);
        Objects.requireNonNull(options);

        return new ClientJobExecution<>(ch, doExecuteColocatedAsync(tableName, key, units, jobClassName, options, args));
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> JobExecution<R> submitColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);
        Objects.requireNonNull(options);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        return new ClientJobExecution<>(ch, doExecuteColocatedAsync(tableName, key, keyMapper, units, jobClassName, options, args));
    }

    private CompletableFuture<SubmitResult> doExecuteColocatedAsync(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return getTable(tableName)
                .thenCompose(table -> executeColocatedTupleKey(table, key, units, jobClassName, options, args))
                .handle((res, err) -> handleMissingTable(
                        tableName,
                        res,
                        err,
                        () -> doExecuteColocatedAsync(tableName, key, units, jobClassName, options, args)
                ))
                .thenCompose(Function.identity());
    }

    private <K> CompletableFuture<SubmitResult> doExecuteColocatedAsync(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return getTable(tableName)
                .thenCompose(table -> executeColocatedObjectKey(table, key, keyMapper, units, jobClassName, options, args))
                .handle((res, err) -> handleMissingTable(
                        tableName,
                        res,
                        err,
                        () -> doExecuteColocatedAsync(tableName, key, keyMapper, units, jobClassName, options, args)
                ))
                .thenCompose(Function.identity());
    }

    /** {@inheritDoc} */
    @Override
    public <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return sync(executeColocatedAsync(tableName, key, units, jobClassName, options, args));
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> R executeColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        return sync(executeColocatedAsync(tableName, key, keyMapper, units, jobClassName, options, args));
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, JobExecution<R>> submitBroadcast(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);
        Objects.requireNonNull(options);

        Map<ClusterNode, JobExecution<R>> map = new HashMap<>(nodes.size());

        for (ClusterNode node : nodes) {
            JobExecution<R> execution = new ClientJobExecution<>(ch, executeOnNodesAsync(
                    Set.of(node), units, jobClassName, options, args
            ));
            if (map.put(node, execution) != null) {
                throw new IllegalStateException("Node can't be specified more than once: " + node);
            }
        }

        return map;
    }

    @Override
    public <R> TaskExecution<R> submitMapReduce(List<DeploymentUnit> units, String taskClassName, Object... args) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-22124
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public <R> R executeMapReduce(List<DeploymentUnit> units, String taskClassName, Object... args) {
        return sync(executeMapReduceAsync(units, taskClassName, args));
    }

    private CompletableFuture<SubmitResult> executeOnNodesAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object[] args
    ) {
        ClusterNode node = randomNode(nodes);

        return ch.serviceAsync(
                ClientOp.COMPUTE_EXECUTE,
                w -> {
                    packNodeNames(w.out(), nodes);
                    packJob(w.out(), units, jobClassName, options, args);
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

    private static <K> CompletableFuture<SubmitResult> executeColocatedObjectKey(
            ClientTable t,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object[] args) {
        return executeColocatedInternal(
                t,
                (outputChannel, schema) -> ClientRecordSerializer.writeRecRaw(key, keyMapper, schema, outputChannel.out(), TuplePart.KEY),
                ClientTupleSerializer.getPartitionAwarenessProvider(null, keyMapper, key),
                units,
                jobClassName,
                options,
                args);
    }

    private static CompletableFuture<SubmitResult> executeColocatedTupleKey(
            ClientTable t,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object[] args) {
        return executeColocatedInternal(
                t,
                (outputChannel, schema) -> ClientTupleSerializer.writeTupleRaw(key, schema, outputChannel, true),
                ClientTupleSerializer.getPartitionAwarenessProvider(null, key),
                units,
                jobClassName,
                options,
                args);
    }

    private static CompletableFuture<SubmitResult> executeColocatedInternal(
            ClientTable t,
            BiConsumer<PayloadOutputChannel, ClientSchema> keyWriter,
            PartitionAwarenessProvider partitionAwarenessProvider,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object[] args) {
        return t.doSchemaOutOpAsync(
                ClientOp.COMPUTE_EXECUTE_COLOCATED,
                (schema, outputChannel) -> {
                    ClientMessagePacker w = outputChannel.out();

                    w.packInt(t.tableId());
                    w.packInt(schema.version());

                    keyWriter.accept(outputChannel, schema);

                    packJob(w, units, jobClassName, options, args);
                },
                ClientCompute::unpackSubmitResult,
                partitionAwarenessProvider,
                true,
                null);
    }

    private CompletableFuture<ClientTable> getTable(String tableName) {
        // Cache tables by name to avoid extra network call on every executeColocated.
        var cached = tableCache.get(tableName);

        if (cached != null) {
            return completedFuture(cached);
        }

        return tables.tableAsync(tableName).thenApply(t -> {
            if (t == null) {
                throw new TableNotFoundException(SqlCommon.DEFAULT_SCHEMA_NAME, tableName);
            }

            ClientTable clientTable = (ClientTable) t;
            tableCache.put(t.name(), clientTable);

            return clientTable;
        });
    }

    private CompletableFuture<SubmitResult> handleMissingTable(
            String tableName,
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

    private static void packJob(ClientMessagePacker w,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object[] args) {
        w.packInt(units.size());
        for (DeploymentUnit unit : units) {
            w.packString(unit.name());
            w.packString(unit.version().render());
        }

        w.packString(jobClassName);
        w.packInt(options.priority());
        w.packInt(options.maxRetries());
        w.packObjectArrayAsBinaryTuple(args);
    }

    /**
     * Unpacks job id from channel and gets notification future. This is needed because we need to unpack message response in the payload
     * reader because the unpacker will be closed after the response is processed.
     *
     * @param ch Payload channel.
     * @return Result of the job submission.
     */
    private static SubmitResult unpackSubmitResult(PayloadInputChannel ch) {
        return new SubmitResult(ch.in().unpackUuid(), ch.notificationFuture());
    }

    private static <R> R sync(CompletableFuture<R> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ClientUtils.ensurePublicException(e));
        }
    }
}
