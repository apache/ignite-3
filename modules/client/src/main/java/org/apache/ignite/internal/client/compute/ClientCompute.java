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

import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
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
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.table.ClientRecordSerializer;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.client.table.ClientTables;
import org.apache.ignite.internal.client.table.ClientTupleSerializer;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Client compute implementation.
 */
public class ClientCompute implements IgniteCompute {
    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    /** Indicates a missing table. */
    private static final Object MISSING_TABLE_TOKEN = new Object();

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
    public <R> CompletableFuture<R> executeAsync(Set<ClusterNode> nodes, List<DeploymentUnit> units, String jobClassName, Object... args) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        ClusterNode node = randomNode(nodes);

        return executeOnOneNode(node, units, jobClassName, args);
    }

    /** {@inheritDoc} */
    @Override
    public <R> R execute(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<R>executeAsync(nodes, units, jobClassName, args).join();
        } catch (CompletionException e) {
            throw sneakyThrow(IgniteExceptionMapperUtil.mapToPublicException(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeColocatedAsync(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        return getTable(tableName)
                .thenCompose(table -> (CompletableFuture<R>) executeColocatedTupleKey(table, key, units, jobClassName, args))
                .handle((res, err) -> handleMissingTable(tableName, res, err))
                .thenCompose(r ->
                        // If a table was dropped, try again: maybe a new table was created with the same name and new id.
                        r == MISSING_TABLE_TOKEN
                                ? executeColocatedAsync(tableName, key, units, jobClassName, args)
                                : CompletableFuture.completedFuture(r));
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> CompletableFuture<R> executeColocatedAsync(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        return getTable(tableName)
                .thenCompose(table -> (CompletableFuture<R>) executeColocatedObjectKey(table, key, keyMapper, units, jobClassName, args))
                .handle((res, err) -> handleMissingTable(tableName, res, err))
                .thenCompose(r ->
                        // If a table was dropped, try again: maybe a new table was created with the same name and new id.
                        r == MISSING_TABLE_TOKEN
                                ? executeColocatedAsync(tableName, key, keyMapper, units, jobClassName, args)
                                : CompletableFuture.completedFuture(r));
    }

    /** {@inheritDoc} */
    @Override
    public <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<R>executeColocatedAsync(tableName, key, units, jobClassName, args).join();
        } catch (CompletionException e) {
            throw sneakyThrow(IgniteExceptionMapperUtil.mapToPublicException(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> R executeColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<K, R>executeColocatedAsync(tableName, key, keyMapper, units, jobClassName, args).join();
        } catch (CompletionException e) {
            throw sneakyThrow(IgniteExceptionMapperUtil.mapToPublicException(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcastAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        Map<ClusterNode, CompletableFuture<R>> map = new HashMap<>(nodes.size());

        for (ClusterNode node : nodes) {
            if (map.put(node, executeOnOneNode(node, units, jobClassName, args)) != null) {
                throw new IllegalStateException("Node can't be specified more than once: " + node);
            }
        }

        return map;
    }

    private <R> CompletableFuture<R> executeOnOneNode(ClusterNode node, List<DeploymentUnit> units, String jobClassName, Object[] args) {
        return ch.serviceAsync(ClientOp.COMPUTE_EXECUTE, w -> {
            if (w.clientChannel().protocolContext().clusterNode().name().equals(node.name())) {
                w.out().packNil();
            } else {
                w.out().packString(node.name());
            }

            packJob(w.out(), units, jobClassName, args);
        }, r -> (R) r.in().unpackObjectFromBinaryTuple(), node.name(), null);
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

    private static <K, R> CompletableFuture<R> executeColocatedObjectKey(
            ClientTable t,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object[] args) {
        return t.doSchemaOutOpAsync(
                ClientOp.COMPUTE_EXECUTE_COLOCATED,
                (schema, outputChannel) -> {
                    ClientMessagePacker w = outputChannel.out();

                    w.packInt(t.tableId());
                    w.packInt(schema.version());

                    ClientRecordSerializer.writeRecRaw(key, keyMapper, schema, w, TuplePart.KEY);

                    packJob(w, units, jobClassName, args);
                },
                r -> (R) r.unpackObjectFromBinaryTuple(),
                ClientTupleSerializer.getPartitionAwarenessProvider(null, keyMapper, key));
    }

    private static <R> CompletableFuture<R> executeColocatedTupleKey(
            ClientTable t,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object[] args) {
        return t.doSchemaOutOpAsync(
                ClientOp.COMPUTE_EXECUTE_COLOCATED,
                (schema, outputChannel) -> {
                    ClientMessagePacker w = outputChannel.out();

                    w.packInt(t.tableId());
                    w.packInt(schema.version());

                    ClientTupleSerializer.writeTupleRaw(key, schema, outputChannel, true);

                    packJob(w, units, jobClassName, args);
                },
                r -> (R) r.unpackObjectFromBinaryTuple(),
                ClientTupleSerializer.getPartitionAwarenessProvider(null, key));
    }

    private CompletableFuture<ClientTable> getTable(String tableName) {
        // Cache tables by name to avoid extra network call on every executeColocated.
        var cached = tableCache.get(tableName);

        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        return tables.tableAsync(tableName).thenApply(t -> {
            if (t == null) {
                throw new TableNotFoundException(DEFAULT_SCHEMA_NAME, tableName);
            }

            ClientTable clientTable = (ClientTable) t;
            tableCache.put(t.name(), clientTable);

            return clientTable;
        });
    }

    private <R> R handleMissingTable(String tableName, R res, Throwable err) {
        if (err instanceof CompletionException) {
            err = err.getCause();
        }

        if (err instanceof IgniteException) {
            IgniteException clientEx = (IgniteException) err;

            if (clientEx.code() == TABLE_ID_NOT_FOUND_ERR) {
                // Table was dropped - remove from cache.
                tableCache.remove(tableName);

                return (R) MISSING_TABLE_TOKEN;
            }
        }

        if (err != null) {
            throw new CompletionException(err);
        }

        return res;
    }

    private static void packJob(ClientMessagePacker w, List<DeploymentUnit> units, String jobClassName, Object[] args) {
        w.packInt(units.size());
        for (DeploymentUnit unit : units) {
            w.packString(unit.name());
            w.packString(unit.version().render());
        }

        w.packString(jobClassName);
        w.packObjectArrayAsBinaryTuple(args);
    }
}
