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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.table.ClientRecordBinaryView;
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
    /** Channel. */
    private final ReliableChannel ch;

    /** Tables. */
    private final ClientTables tables;

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
    public <R> CompletableFuture<R> executeColocated(String table, Tuple key, Class<? extends ComputeJob<R>> jobClass, Object... args) {
        // TODO: IGNITE-16786 - implement this
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> CompletableFuture<R> executeColocated(
            String table,
            K key,
            Mapper<K> keyMapper,
            Class<? extends ComputeJob<R>> jobClass,
            Object... args
    ) {
        // TODO: IGNITE-16786 - implement this
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeColocated(String table, Tuple key, String jobClassName, Object... args) {
        // TODO: IGNITE-16925 - implement partition awareness.
        // TODO: Cache tables by name. If the table gets dropped, reset table cache and try again.
        return tables.tableAsync(table).thenCompose(t -> {
            if (t == null) {
                throw new IgniteClientException("Table '" + table + "' does not exist.");
            }

            ClientTable tableInternal = (ClientTable)t;

            return tableInternal.doSchemaOutOpAsync(ClientOp.COMPUTE_EXECUTE_COLOCATED, (schema, outputChannel) -> {
                ClientMessagePacker w = outputChannel.out();

                w.packUuid(tableInternal.tableId());
                w.packInt(schema.version());

                ClientTupleSerializer serializer = ((ClientRecordBinaryView) t.recordView()).serializer();
                serializer.writeTuple(null, key, schema, outputChannel, true, true);

                w.packString(jobClassName);
                w.packObjectArray(args);
            }, r -> (R) r.unpackObjectWithType());
        });
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> CompletableFuture<R> executeColocated(String table, K key, Mapper<K> keyMapper, String jobClassName, Object... args) {
        // TODO: IGNITE-16925 - implement partition awareness.
        // TODO: Deduplicate with above.
        return tables.tableAsync(table).thenCompose(t -> {
            if (t == null) {
                throw new IgniteClientException("Table '" + table + "' does not exist.");
            }

            ClientTable tableInternal = (ClientTable)t;

            return tableInternal.doSchemaOutOpAsync(ClientOp.COMPUTE_EXECUTE_COLOCATED, (schema, outputChannel) -> {
                ClientMessagePacker w = outputChannel.out();

                w.packUuid(tableInternal.tableId());
                w.packInt(schema.version());

                var serializer = new ClientRecordSerializer<>(tableInternal.tableId(), keyMapper);
                serializer.writeTuple(null, key, schema, outputChannel, true, true);

                w.packString(jobClassName);
                w.packObjectArray(args);
            }, r -> (R) r.unpackObjectWithType());
        });
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
}
