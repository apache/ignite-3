/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientErrorCode;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.table.ClientRecordSerializer;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.client.table.ClientTables;
import org.apache.ignite.internal.client.table.ClientTupleSerializer;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Client compute implementation.
 */
public class ClientCompute implements IgniteCompute {
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
    public <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass, Object... args) {
        return execute(nodes, jobClass.getName(), args);
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, String jobClassName, Object... args) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(jobClassName);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        ClusterNode node = randomNode(nodes);

        return executeOnOneNode(node, jobClassName, args);
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeColocated(String tableName, Tuple key, Class<? extends ComputeJob<R>> jobClass, Object... args) {
        return executeColocated(tableName, key, jobClass.getName(), args);
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> CompletableFuture<R> executeColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            Class<? extends ComputeJob<R>> jobClass,
            Object... args
    ) {
        return executeColocated(tableName, key, keyMapper, jobClass.getName(), args);
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeColocated(String tableName, Tuple key, String jobClassName, Object... args) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(jobClassName);

        // TODO: IGNITE-16925 - implement partition awareness.
        return getTable(tableName)
                .thenCompose(table -> this.<R>executeColocatedTupleKey(table, key, jobClassName, args))
                .handle((res, err) -> handleMissingTable(tableName, res, err))
                .thenCompose(r ->
                        // If a table was dropped, try again: maybe a new table was created with the same name and new id.
                        r == MISSING_TABLE_TOKEN
                                ? executeColocated(tableName, key, jobClassName, args)
                                : CompletableFuture.completedFuture(r));
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> CompletableFuture<R> executeColocated(String tableName, K key, Mapper<K> keyMapper, String jobClassName, Object... args) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);
        Objects.requireNonNull(jobClassName);

        // TODO: IGNITE-16925 - implement partition awareness.
        return getTable(tableName)
                .thenCompose(table -> this.<K, R>executeColocatedObjectKey(table, key, keyMapper, jobClassName, args))
                .handle((res, err) -> handleMissingTable(tableName, res, err))
                .thenCompose(r ->
                        // If a table was dropped, try again: maybe a new table was created with the same name and new id.
                        r == MISSING_TABLE_TOKEN
                                ? executeColocated(tableName, key, keyMapper, jobClassName, args)
                                : CompletableFuture.completedFuture(r));
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass,
            Object... args) {
        return broadcast(nodes, jobClass.getName(), args);
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, String jobClassName, Object... args) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(jobClassName);

        Map<ClusterNode, CompletableFuture<R>> map = new HashMap<>(nodes.size());

        for (ClusterNode node : nodes) {
            if (map.put(node, executeOnOneNode(node, jobClassName, args)) != null) {
                throw new IllegalStateException("Node can't be specified more than once: " + node);
            }
        }

        return map;
    }

    private <R> CompletableFuture<R> executeOnOneNode(ClusterNode node, String jobClassName, Object[] args) {
        return ch.serviceAsync(ClientOp.COMPUTE_EXECUTE, w -> {
            if (w.clientChannel().protocolContext().clusterNode().name().equals(node.name())) {
                w.out().packNil();
            } else {
                w.out().packString(node.name());
            }

            w.out().packString(jobClassName);
            w.out().packObjectArray(args);
        }, r -> (R) r.in().unpackObjectWithType(), node.name());
    }

    private ClusterNode randomNode(Set<ClusterNode> nodes) {
        int nodesToSkip = ThreadLocalRandom.current().nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }

    private <K, R> CompletableFuture<R> executeColocatedObjectKey(
            ClientTable t,
            K key,
            Mapper<K> keyMapper,
            String jobClassName,
            Object[] args) {
        return t.doSchemaOutOpAsync(
                ClientOp.COMPUTE_EXECUTE_COLOCATED,
                (schema, outputChannel) -> {
                    ClientMessagePacker w = outputChannel.out();

                    w.packUuid(t.tableId());
                    w.packInt(schema.version());

                    ClientRecordSerializer.writeRecRaw(key, keyMapper, schema, w, TuplePart.KEY);

                    w.packString(jobClassName);
                    w.packObjectArray(args);
                },
                r -> (R) r.unpackObjectWithType());
    }

    private <R> CompletableFuture<R> executeColocatedTupleKey(
            ClientTable t,
            Tuple key,
            String jobClassName,
            Object[] args) {
        return t.doSchemaOutOpAsync(
                ClientOp.COMPUTE_EXECUTE_COLOCATED,
                (schema, outputChannel) -> {
                    ClientMessagePacker w = outputChannel.out();

                    w.packUuid(t.tableId());
                    w.packInt(schema.version());

                    ClientTupleSerializer.writeTupleRaw(key, schema, outputChannel, true);

                    w.packString(jobClassName);
                    w.packObjectArray(args);
                },
                r -> (R) r.unpackObjectWithType());
    }

    private CompletableFuture<ClientTable> getTable(String tableName) {
        // Cache tables by name to avoid extra network call on every executeColocated.
        var cached = tableCache.get(tableName);

        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        return tables.tableAsync(tableName).thenApply(t -> {
            if (t == null) {
                throw new IgniteClientException("Table '" + tableName + "' does not exist.");
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

        if (err instanceof IgniteClientException) {
            IgniteClientException clientEx = (IgniteClientException) err;

            if (clientEx.errorCode() == ClientErrorCode.TABLE_ID_DOES_NOT_EXIST) {
                // Table was dropped - remove from cache.
                tableCache.remove(tableName);

                return (R) MISSING_TABLE_TOKEN;
            }

            throw new IgniteClientException(clientEx.getMessage(), clientEx.errorCode(), clientEx);
        }

        if (err != null) {
            throw new IgniteClientException(err.getMessage(), err);
        }

        return res;
    }
}
