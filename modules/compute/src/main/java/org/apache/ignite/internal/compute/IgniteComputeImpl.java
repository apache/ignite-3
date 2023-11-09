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

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Implementation of {@link IgniteCompute}.
 */
public class IgniteComputeImpl implements IgniteCompute {
    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

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
    public <R> CompletableFuture<R> executeAsync(Set<ClusterNode> nodes, List<DeploymentUnit> units, String jobClassName, Object... args) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        return executeOnOneNode(randomNode(nodes), units, jobClassName, args);
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
            throw ExceptionUtils.wrap(e);
        }
    }

    private ClusterNode randomNode(Set<ClusterNode> nodes) {
        int nodesToSkip = random.nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }

    private <R> CompletableFuture<R> executeOnOneNode(
            ClusterNode targetNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object[] args
    ) {
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(units, jobClassName, args);
        } else {
            return computeComponent.executeRemotely(targetNode, units, jobClassName, args);
        }
    }

    private boolean isLocal(ClusterNode targetNode) {
        return targetNode.equals(topologyService.localMember());
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

        return requiredTable(tableName)
                .thenCompose(table -> primaryReplicaByTupleKey(table, key))
                .thenCompose(primaryReplica -> executeOnOneNode(primaryReplica.node(), units, jobClassName, args));
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

        return requiredTable(tableName)
                .thenCompose(table -> primaryReplicaByMappedKey(table, key, keyMapper))
                .thenCompose(primaryReplica -> executeOnOneNode(primaryReplica.node(), units, jobClassName, args));
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
            throw ExceptionUtils.wrap(e);
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
            throw ExceptionUtils.wrap(e);
        }
    }

    private CompletableFuture<TableViewInternal> requiredTable(String tableName) {
        String parsedName = IgniteNameUtils.parseSimpleName(tableName);

        return tables.tableViewAsync(parsedName)
                .thenApply(table -> {
                    if (table == null) {
                        throw new TableNotFoundException(DEFAULT_SCHEMA_NAME, parsedName);
                    }
                    return table;
                });
    }

    private static CompletableFuture<PrimaryReplica> primaryReplicaByTupleKey(TableViewInternal table, Tuple key) {
        return primaryReplicaByPartition(table, table.partition(key));
    }

    private static  <K> CompletableFuture<PrimaryReplica> primaryReplicaByMappedKey(TableViewInternal table, K key, Mapper<K> keyMapper) {
        return primaryReplicaByPartition(table, table.partition(key, keyMapper));
    }

    private static CompletableFuture<PrimaryReplica> primaryReplicaByPartition(TableViewInternal table, int partitionIndex) {
        return table.internalTable().primaryReplica(partitionIndex);
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

        return nodes.stream()
                .collect(toUnmodifiableMap(node -> node, node -> executeOnOneNode(node, units, jobClassName, args)));
    }
}
