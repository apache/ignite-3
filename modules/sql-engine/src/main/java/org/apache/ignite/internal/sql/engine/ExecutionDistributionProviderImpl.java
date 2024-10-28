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

package org.apache.ignite.internal.sql.engine;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.AWAIT_PRIMARY_REPLICA_TIMEOUT;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.internal.util.IgniteUtils.newHashMap;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.systemview.api.SystemViewManager;

/** Execution nodes information provider. */
public class ExecutionDistributionProviderImpl implements ExecutionDistributionProvider {
    private static final IgniteLogger LOG = Loggers.forClass(ExecutionDistributionProviderImpl.class);
    private final PlacementDriver placementDriver;
    private final SystemViewManager systemViewManager;

    ExecutionDistributionProviderImpl(PlacementDriver placementDriver, SystemViewManager systemViewManager) {
        this.placementDriver = placementDriver;
        this.systemViewManager = systemViewManager;
    }

    @Override
    public CompletableFuture<DistributionHolder> distribution(
            HybridTimestamp operationTime,
            boolean mapOnBackups,
            Collection<IgniteTable> tables,
            Collection<String> views,
            String initiatorNode
    ) {
        if (tables.isEmpty() && views.isEmpty()) {
            DistributionHolder holder = new DistributionHolderImpl(List.of(initiatorNode), Map.of(), Map.of());

            return completedFuture(holder);
        } else {
            return distribution0(tables, views, operationTime, mapOnBackups, initiatorNode);
        }
    }

    private CompletableFuture<DistributionHolder> distribution0(
            Collection<IgniteTable> tables,
            Collection<String> views,
            HybridTimestamp operationTime,
            boolean mapOnBackups,
            String initiatorNode
    ) {
        Map<IgniteTable, CompletableFuture<List<TokenizedAssignments>>> mapResult = newHashMap(tables.size());
        Map<IgniteTable, List<TokenizedAssignments>> mapResultResolved = newHashMap(tables.size());

        for (IgniteTable tbl : tables) {
            CompletableFuture<List<TokenizedAssignments>> assignments = collectAssignments(tbl, operationTime, mapOnBackups);

            mapResult.put(tbl, assignments);
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(mapResult.values().toArray(new CompletableFuture[0]));

        CompletableFuture<Map<IgniteTable, List<TokenizedAssignments>>> fut = all.thenApply(v -> mapResult.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().join()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue))
        );

        CompletableFuture<List<String>> participantNodes = fut.thenApply(
                v -> v.values().stream().flatMap(List::stream).flatMap(i -> i.nodes().stream()).map(Assignment::consistentId)
                        .distinct()
                        .collect(Collectors.toList()));

        return participantNodes.thenApply(nodes -> {
            nodes.add(initiatorNode);

            // this is a safe join, because we have waited for all futures to be completed
            mapResult.forEach((k, v) -> mapResultResolved.put(k, v.join()));

            Map<String, List<String>> nodesPerView = views.stream().distinct()
                    .collect(Collectors.toMap(Function.identity(), systemViewManager::owningNodes));

            List<String> viewNodes = nodesPerView.values().stream().flatMap(List::stream).collect(Collectors.toList());

            List<String> nodes0 = Stream.concat(viewNodes.stream(), nodes.stream()).distinct().collect(Collectors.toList());

            return new DistributionHolderImpl(nodes0, mapResultResolved, nodesPerView);
        });
    }


    // need to be refactored after TODO: https://issues.apache.org/jira/browse/IGNITE-20925
    /** Get primary replicas. */
    private CompletableFuture<List<TokenizedAssignments>> collectAssignments(
            IgniteTable table, HybridTimestamp operationTime, boolean includeBackups
    ) {
        int partitions = table.partitions();

        if (includeBackups) {
            List<TablePartitionId> replicationGroupIds = new ArrayList<>(partitions);

            for (int p = 0; p < partitions; p++) {
                replicationGroupIds.add(new TablePartitionId(table.id(), p));
            }

            return allReplicas(replicationGroupIds, operationTime);
        }

        List<CompletableFuture<TokenizedAssignments>> result = new ArrayList<>(partitions);

        // no need to wait all partitions after pruning was implemented.
        for (int partId = 0; partId < partitions; ++partId) {
            ReplicationGroupId partGroupId = new TablePartitionId(table.id(), partId);

            CompletableFuture<TokenizedAssignments> partitionAssignment = primaryReplica(partGroupId, operationTime);

            result.add(partitionAssignment);
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(result.toArray(new CompletableFuture[0]));

        return all.thenApply(v -> result.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList())
        );
    }

    private CompletableFuture<TokenizedAssignments> primaryReplica(
            ReplicationGroupId replicationGroupId,
            HybridTimestamp operationTime
    ) {
        CompletableFuture<ReplicaMeta> f = placementDriver.awaitPrimaryReplica(
                replicationGroupId,
                operationTime,
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        return f.handle((primaryReplica, e) -> {
            if (e != null) {
                LOG.debug("Failed to retrieve primary replica for partition {}", e, replicationGroupId);

                throw withCause(IgniteInternalException::new, REPLICA_UNAVAILABLE_ERR, "Failed to get the primary replica"
                        + " [tablePartitionId=" + replicationGroupId + ']', e);
            } else {
                String holder = primaryReplica.getLeaseholder();

                assert holder != null : "Unable to map query, nothing holds the lease";

                return new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(holder)), primaryReplica.getStartTime().longValue());
            }
        });
    }

    private CompletableFuture<List<TokenizedAssignments>> allReplicas(
            List<TablePartitionId> replicationGroupIds,
            HybridTimestamp operationTime
    ) {
        CompletableFuture<List<TokenizedAssignments>> f = placementDriver.getAssignments(
                replicationGroupIds,
                operationTime
        );

        return f.thenCompose(assignments -> {
            // Collect missed assignments indexes if found.
            IntList missedAssignments = new IntArrayList(0);

            for (int i = 0; i < assignments.size(); i++) {
                if (assignments.get(i) == null) {
                    missedAssignments.add(i);
                }
            }

            if (missedAssignments.isEmpty()) {
                return completedFuture(assignments);
            }

            // assignments are not ready yet, let's fall back to primary replicas
            List<CompletableFuture<TokenizedAssignments>> primaryReplicaAssignment = new ArrayList<>(missedAssignments.size());

            for (int i = 0; i < missedAssignments.size(); i++) {
                primaryReplicaAssignment.add(primaryReplica(replicationGroupIds.get(missedAssignments.getInt(i)), operationTime));
            }

            CompletableFuture<Void> all = CompletableFuture.allOf(primaryReplicaAssignment.toArray(new CompletableFuture[0]));
            return all.thenApply(ignore -> {
                // Creates a mutable copy and replace missed assignments with primary replicas.
                List<TokenizedAssignments> finalAssignments = new ArrayList<>(assignments);

                for (int i = 0; i < missedAssignments.size(); i++) {
                    finalAssignments.set(missedAssignments.getInt(i), primaryReplicaAssignment.get(i).join());
                }

                return finalAssignments;
            });
        });
    }

    static class DistributionHolderImpl implements DistributionHolder {
        private final List<String> nodes;
        private final Map<IgniteTable, List<TokenizedAssignments>> assignmentsPerTable;
        private final Map<String, List<String>> nodesPerView;

        DistributionHolderImpl(
                List<String> nodes,
                Map<IgniteTable, List<TokenizedAssignments>> assignmentsPerTable,
                Map<String, List<String>> nodesPerView) {
            this.nodes = nodes;
            this.assignmentsPerTable = assignmentsPerTable;
            this.nodesPerView = nodesPerView;
        }

        @Override
        public List<String> nodes() {
            return nodes;
        }

        @Override
        public List<TokenizedAssignments> tableAssignments(IgniteTable table) {
            return assignmentsPerTable.get(table);
        }

        @Override
        public List<String> viewNodes(String viewName) {
            return nodesPerView.get(viewName);
        }
    }

    /** Nodes distribution information holder. */
    public interface DistributionHolder {
        /** Whole collection of participating nodes. */
        List<String> nodes();

        /** Nodes table assigned to. */
        List<TokenizedAssignments> tableAssignments(IgniteTable table);

        /** Nodes view assigned to. */
        List<String> viewNodes(String viewName);
    }
}
