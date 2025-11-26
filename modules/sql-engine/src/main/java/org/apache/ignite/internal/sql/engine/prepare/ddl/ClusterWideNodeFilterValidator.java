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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterNodeAttributes;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.sql.SqlException;

/**
 * Node filter validator that checks presence of nodes matching given node filter across cluster.
 */
public class ClusterWideNodeFilterValidator implements NodeFilterValidator {
    private final LogicalTopologyService logicalTopologyService;

    public ClusterWideNodeFilterValidator(LogicalTopologyService logicalTopologyService) {
        this.logicalTopologyService = logicalTopologyService;
    }

    @Override
    public CompletableFuture<Void> validate(@Nullable String nodeFilter) {
        if (nodeFilter == null) {
            return nullCompletedFuture();
        }

        Set<NodeWithAttributes> filtered = filteredTopologyNodes(logicalTopologyService.localLogicalTopology(), nodeFilter);

        if (!filtered.isEmpty()) {
            return nullCompletedFuture();
        } else {
            return logicalTopologyService.logicalTopologyOnLeader()
                    .handle((snapshot, e) -> {
                        if (e == null) {
                            if (filteredTopologyNodes(snapshot, nodeFilter).isEmpty()) {
                                throw new SqlException(
                                        STMT_VALIDATION_ERR,
                                        format("Node filter does not match any node in the cluster [filter='{}'].", nodeFilter)
                                );
                            } else {
                                return null;
                            }
                        } else {
                            throw new SqlException(
                                    INTERNAL_ERR,
                                    "Distributed validation of the node filter for distribution zone failed [filter='{}'].",
                                    e
                            );
                        }
                    });
        }
    }

    private static Set<NodeWithAttributes> filteredTopologyNodes(LogicalTopologySnapshot snapshot, String nodeFilter) {
        Set<NodeWithAttributes> nodes = snapshot.nodes().stream()
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                .collect(toSet());

        return nodes.stream()
                .filter(n -> filterNodeAttributes(n.userAttributes(), nodeFilter))
                .collect(toSet());
    }
}
