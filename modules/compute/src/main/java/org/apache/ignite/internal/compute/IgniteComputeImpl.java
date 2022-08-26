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

package org.apache.ignite.internal.compute;

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Implementation of {@link IgniteCompute}.
 */
public class IgniteComputeImpl implements IgniteCompute {
    private final TopologyService topologyService;
    private final IgniteTablesInternal tables;
    private final ComputeComponent computeComponent;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    /**
     * Create new instance.
     */
    public IgniteComputeImpl(TopologyService topologyService, IgniteTablesInternal tables, ComputeComponent computeComponent) {
        this.topologyService = topologyService;
        this.tables = tables;
        this.computeComponent = computeComponent;
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, Class<? extends ComputeJob<R>> jobClass, Object... args) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(jobClass);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        return executeOnOneNode(randomNode(nodes), jobClass, args);
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> execute(Set<ClusterNode> nodes, String jobClassName, Object... args) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(jobClassName);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        return executeOnOneNode(randomNode(nodes), jobClassName, args);
    }

    private ClusterNode randomNode(Set<ClusterNode> nodes) {
        int nodesToSkip = random.nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }

    private <R> CompletableFuture<R> executeOnOneNode(ClusterNode targetNode, Class<? extends ComputeJob<R>> jobClass, Object[] args) {
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(jobClass, args);
        } else {
            return computeComponent.executeRemotely(targetNode, jobClass, args);
        }
    }

    private <R> CompletableFuture<R> executeOnOneNode(ClusterNode targetNode, String jobClassName, Object[] args) {
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(jobClassName, args);
        } else {
            return computeComponent.executeRemotely(targetNode, jobClassName, args);
        }
    }

    private boolean isLocal(ClusterNode targetNode) {
        return targetNode.equals(topologyService.localMember());
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeColocated(String tableName, Tuple key, Class<? extends ComputeJob<R>> jobClass, Object... args) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(jobClass);

        return requiredTable(tableName)
                .thenApply(table -> leaderOfTablePartitionByTupleKey(table, key))
                .thenCompose(primaryNode -> executeOnOneNode(primaryNode, jobClass, args));
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
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);
        Objects.requireNonNull(jobClass);

        return requiredTable(tableName)
                .thenApply(table -> leaderOfTablePartitionByMappedKey(table, key, keyMapper))
                .thenCompose(primaryNode -> executeOnOneNode(primaryNode, jobClass, args));
    }

    /** {@inheritDoc} */
    @Override
    public <R> CompletableFuture<R> executeColocated(String tableName, Tuple key, String jobClassName, Object... args) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(jobClassName);

        return requiredTable(tableName)
                .thenApply(table -> leaderOfTablePartitionByTupleKey(table, key))
                .thenCompose(primaryNode -> executeOnOneNode(primaryNode, jobClassName, args));
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> CompletableFuture<R> executeColocated(String tableName, K key, Mapper<K> keyMapper, String jobClassName, Object... args) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);
        Objects.requireNonNull(jobClassName);

        return requiredTable(tableName)
                .thenApply(table -> leaderOfTablePartitionByMappedKey(table, key, keyMapper))
                .thenCompose(primaryNode -> executeOnOneNode(primaryNode, jobClassName, args));
    }

    private CompletableFuture<TableImpl> requiredTable(String tableName) {
        return tables.tableImplAsync(tableName)
                .thenApply(table -> {
                    if (table == null) {
                        throw new TableNotFoundException(tableName);
                    }
                    return table;
                });
    }

    private ClusterNode leaderOfTablePartitionByTupleKey(TableImpl table, Tuple key) {
        return requiredLeaderByPartition(table, table.partition(key));
    }

    private <K> ClusterNode leaderOfTablePartitionByMappedKey(TableImpl table, K key, Mapper<K> keyMapper) {
        return requiredLeaderByPartition(table, table.partition(key, keyMapper));
    }

    private ClusterNode requiredLeaderByPartition(TableImpl table, int partitionIndex) {
        ClusterNode leaderNode = table.leaderAssignment(partitionIndex);
        if (leaderNode == null) {
            throw new IgniteInternalException("Leader not found for partition " + partitionIndex);
        }

        return leaderNode;
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcast(
            Set<ClusterNode> nodes,
            Class<? extends ComputeJob<R>> jobClass,
            Object... args
    ) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(jobClass);

        return nodes.stream()
                .collect(toUnmodifiableMap(node -> node, node -> executeOnOneNode(node, jobClass, args)));
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, CompletableFuture<R>> broadcast(Set<ClusterNode> nodes, String jobClassName, Object... args) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(jobClassName);

        return nodes.stream()
                .collect(toUnmodifiableMap(node -> node, node -> executeOnOneNode(node, jobClassName, args)));
    }
}
