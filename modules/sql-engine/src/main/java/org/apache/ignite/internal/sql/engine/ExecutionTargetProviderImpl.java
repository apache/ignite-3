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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.AWAIT_PRIMARY_REPLICA_TIMEOUT;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.affinity.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetProvider;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;

/**
 * Implementation of {@link ExecutionTargetProvider} which takes assignments from {@link PlacementDriver} and {@link SystemViewManager}.
 */
public class ExecutionTargetProviderImpl implements ExecutionTargetProvider {
    private static final IgniteLogger LOG = Loggers.forClass(ExecutionTargetProviderImpl.class);

    private final PlacementDriver placementDriver;
    private final SystemViewManager systemViewManager;

    ExecutionTargetProviderImpl(
            PlacementDriver placementDriver, SystemViewManager systemViewManager
    ) {
        this.placementDriver = placementDriver;
        this.systemViewManager = systemViewManager;
    }

    @Override
    public CompletableFuture<ExecutionTarget> forTable(
            HybridTimestamp operationTime,
            ExecutionTargetFactory factory,
            IgniteTable table,
            boolean includeBackups
    ) {
        return collectAssignments(table, operationTime, includeBackups)
                .thenApply(factory::partitioned);
    }

    @Override
    public CompletableFuture<ExecutionTarget> forSystemView(ExecutionTargetFactory factory, IgniteSystemView view) {
        List<String> nodes = systemViewManager.owningNodes(view.name());

        if (nullOrEmpty(nodes)) {
            return failedFuture(
                    new SqlException(Sql.MAPPING_ERR, format("The view with name '{}' could not be found on"
                            + " any active nodes in the cluster", view.name()))
            );
        }

        return completedFuture(
                view.distribution() == IgniteDistributions.single()
                        ? factory.oneOf(nodes)
                        : factory.allOf(nodes)
        );
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
}
