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
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.sql.SqlException;

/**
 * Storage profile names validator that checks presence of given to validate profile names across cluster.
 */
public class ClusterWideStorageProfileValidator implements StorageProfileValidator {
    private final LogicalTopologyService logicalTopologyService;

    public ClusterWideStorageProfileValidator(LogicalTopologyService logicalTopologyService) {
        this.logicalTopologyService = logicalTopologyService;
    }

    @Override
    public CompletableFuture<Void> validate(Collection<String> storageProfiles) {
        LogicalTopologySnapshot localLogicalTopologySnapshot = logicalTopologyService.localLogicalTopology();

        Set<String> missedStorageProfileNames = findStorageProfileNotPresentedInLogicalTopologySnapshot(
                storageProfiles,
                localLogicalTopologySnapshot
        );

        if (missedStorageProfileNames.isEmpty()) {
            return nullCompletedFuture();
        }

        return logicalTopologyService.logicalTopologyOnLeader()
                    .thenApply(topologySnapshot -> findStorageProfileNotPresentedInLogicalTopologySnapshot(
                            storageProfiles,
                            topologySnapshot
                    )).handle((missedProfileNames, e) -> {
                        if (e != null) {
                            String msg = format(
                                    "Storage profiles {} don't exist in local topology snapshot with profiles [{}], "
                                            + "and distributed refresh failed.",
                                    missedStorageProfileNames,
                                    localLogicalTopologySnapshot
                                            .nodes()
                                            .stream()
                                            .map(LogicalNode::storageProfiles)
                                            .collect(toSet())
                            );

                            throw new SqlException(INTERNAL_ERR, msg, e);
                        }

                        if (!missedProfileNames.isEmpty()) {
                            throw new SqlException(STMT_VALIDATION_ERR, format(
                                    "Some storage profiles don't exist [missedProfileNames={}].",
                                    missedProfileNames
                            ));
                        }

                        return null;
                    });
    }

    private static Set<String> findStorageProfileNotPresentedInLogicalTopologySnapshot(
            Collection<String> storageProfiles,
            LogicalTopologySnapshot snapshot
    ) {
        Set<String> topologyWideProfiles = extractStorageProfileNamesFromLogicalTopologySnapshot(snapshot);

        Set<String> missedProfiles = new HashSet<>();

        for (String profileName : storageProfiles) {
            if (!topologyWideProfiles.contains(profileName)) {
                missedProfiles.add(profileName);
            }
        }

        return missedProfiles;
    }

    private static Set<String> extractStorageProfileNamesFromLogicalTopologySnapshot(LogicalTopologySnapshot snapshot) {
        Set<LogicalNode> logicalNodes = snapshot.nodes();

        // Assume default persistent + rocks + aimem profiles on each node in average.
        Set<String> topologyWideProfiles = new HashSet<>(logicalNodes.size() * 3);

        for (LogicalNode logicalNode : logicalNodes) {
            topologyWideProfiles.addAll(logicalNode.storageProfiles());
        }

        return topologyWideProfiles;
    }
}
