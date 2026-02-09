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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.AWAIT_PRIMARY_REPLICA_TIMEOUT;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_UNAVAILABLE_ERR;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.systemview.api.SystemViewManager;

/** Execution nodes information provider. */
public class ExecutionDistributionProviderImpl implements ExecutionDistributionProvider {
    // TODO https://issues.apache.org/jira/browse/IGNITE-26651
    // TODO https://issues.apache.org/jira/browse/IGNITE-26652
    /** Non-empty assignments await timeout. */
    public static final int AWAIT_NON_EMPTY_ASSIGNMENTS_TIMEOUT_MILLIS = 30_000;

    private static final IgniteLogger LOG = Loggers.forClass(ExecutionDistributionProviderImpl.class);
    private final PlacementDriver placementDriver;
    private final SystemViewManager systemViewManager;

    /**
     * Constructor.
     *
     * @param placementDriver Placement driver.
     * @param systemViewManager Manager for system views.
     */
    public ExecutionDistributionProviderImpl(PlacementDriver placementDriver, SystemViewManager systemViewManager) {
        this.placementDriver = placementDriver;
        this.systemViewManager = systemViewManager;
    }

    @Override
    public List<String> forSystemView(IgniteSystemView view) {
        return systemViewManager.owningNodes(view.name());
    }

    @Override
    public CompletableFuture<List<TokenizedAssignments>> forTable(
            HybridTimestamp operationTime,
            IgniteTable table,
            boolean includeBackups
    ) {
        return collectAssignments(table, operationTime, includeBackups);
    }

    // need to be refactored after TODO: https://issues.apache.org/jira/browse/IGNITE-20925
    /** Get primary replicas. */
    private CompletableFuture<List<TokenizedAssignments>> collectAssignments(
            IgniteTable table, HybridTimestamp operationTime, boolean includeBackups
    ) {
        int partitions = table.partitions();

        if (includeBackups) {
            List<ZonePartitionId> replicationGroupIds = new ArrayList<>(partitions);

            for (int partitionIndex = 0; partitionIndex < partitions; partitionIndex++) {
                replicationGroupIds.add(new ZonePartitionId(table.zoneId(), partitionIndex));
            }

            return allReplicas(replicationGroupIds, operationTime);
        }

        List<CompletableFuture<TokenizedAssignments>> result = new ArrayList<>(partitions);

        // no need to wait all partitions after pruning was implemented.
        for (int partitionIndex = 0; partitionIndex < partitions; partitionIndex++) {
            ZonePartitionId partGroupId = new ZonePartitionId(table.zoneId(), partitionIndex);

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
            ZonePartitionId replicationGroupId,
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
                        + " [replicationGroupId=" + replicationGroupId + ']', e);
            } else {
                String holder = primaryReplica.getLeaseholder();

                assert holder != null : "Unable to map query, nothing holds the lease";

                return new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(holder)), primaryReplica.getStartTime().longValue());
            }
        });
    }

    private CompletableFuture<List<TokenizedAssignments>> allReplicas(
            List<ZonePartitionId> replicationGroupIds,
            HybridTimestamp operationTime
    ) {
        return placementDriver.awaitNonEmptyAssignments(
                replicationGroupIds,
                operationTime,
                AWAIT_NON_EMPTY_ASSIGNMENTS_TIMEOUT_MILLIS
        );
    }
}
