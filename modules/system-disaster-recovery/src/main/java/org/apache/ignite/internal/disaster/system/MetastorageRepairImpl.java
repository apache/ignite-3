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

package org.apache.ignite.internal.disaster.system;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.disaster.system.message.BecomeMetastorageLeaderMessage;
import org.apache.ignite.internal.disaster.system.message.MetastorageIndexTermRequestMessage;
import org.apache.ignite.internal.disaster.system.message.MetastorageIndexTermResponseMessage;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.disaster.system.repair.MetastorageRepair;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.network.ClusterNode;

/**
 * Implementation of {@link MetastorageRepair}.
 */
public class MetastorageRepairImpl implements MetastorageRepair {
    private static final IgniteLogger LOG = Loggers.forClass(MetastorageRepairImpl.class);

    private final MessagingService messagingService;
    private final LogicalTopology logicalTopology;
    private final ClusterManagementGroupManager cmgManager;

    private final SystemDisasterRecoveryMessagesFactory messagesFactory = new SystemDisasterRecoveryMessagesFactory();

    /** Constructor. */
    public MetastorageRepairImpl(
            MessagingService messagingService,
            LogicalTopology logicalTopology,
            ClusterManagementGroupManager cmgManager
    ) {
        this.messagingService = messagingService;
        this.logicalTopology = logicalTopology;
        this.cmgManager = cmgManager;
    }

    @Override
    public CompletableFuture<Void> repair(Set<String> participatingNodeNames, int metastorageReplicationFactor) {
        LOG.info("Starting MG repair [participatingNodes={}, replicationFactor={}].", participatingNodeNames, metastorageReplicationFactor);

        return waitTillValidatedNodesContain(participatingNodeNames)
                .thenCompose(unused -> collectMetastorageIndexes(participatingNodeNames))
                .thenCompose(indexes -> {
                    LOG.info("Collected metastorage indexes [indexes={}].", indexes);

                    Set<String> newMgNodes = nodesWithBestIndexes(indexes, metastorageReplicationFactor);
                    LOG.info("Chose new MG nodes [mgNodes={}].", newMgNodes);

                    String bestNodeName = chooseNodeWithBestIndex(indexes, newMgNodes);
                    LOG.info("Chose best MG node [node={}].", bestNodeName);

                    return cmgManager.changeMetastorageNodes(newMgNodes)
                            .thenCompose(unused -> appointLeader(bestNodeName, indexes.get(bestNodeName).term(), newMgNodes))
                            .thenRun(() -> LOG.info("Appointed MG leader forcefully [leader={}].", bestNodeName));
                });
    }

    private CompletableFuture<Void> waitTillValidatedNodesContain(Set<String> nodeNames) {
        Set<String> cumulativeValidatedNodeNames = ConcurrentHashMap.newKeySet();
        CompletableFuture<Void> future = new CompletableFuture<>();

        LogicalTopologyEventListener listener = new LogicalTopologyEventListener() {
            @Override
            public void onNodeValidated(LogicalNode validatedNode) {
                cumulativeValidatedNodeNames.add(validatedNode.name());

                if (contains(cumulativeValidatedNodeNames, nodeNames)) {
                    future.complete(null);
                }
            }

            @Override
            public void onNodeInvalidated(LogicalNode invalidatedNode) {
                cumulativeValidatedNodeNames.remove(invalidatedNode.name());
            }
        };

        logicalTopology.addEventListener(listener);

        cmgManager.validatedNodes()
                .thenAccept(validatedNodes -> {
                    Set<String> validatedNodeNames = validatedNodes.stream()
                            .map(ClusterNode::name)
                            .collect(toSet());
                    if (contains(validatedNodeNames, nodeNames)) {
                        future.complete(null);
                    }

                    cumulativeValidatedNodeNames.addAll(validatedNodeNames);
                });

        return future
                .thenRun(() -> logicalTopology.removeEventListener(listener));
    }

    private static boolean contains(Set<String> container, Set<String> containee) {
        return difference(containee, container).isEmpty();
    }

    private CompletableFuture<Map<String, IndexWithTerm>> collectMetastorageIndexes(Set<String> participatingNodeNames) {
        MetastorageIndexTermRequestMessage request = messagesFactory.metastorageIndexTermRequestMessage().build();

        Map<String, CompletableFuture<MetastorageIndexTermResponseMessage>> responses = new HashMap<>();

        for (String nodeName : participatingNodeNames) {
            responses.put(
                    nodeName,
                    messagingService.invoke(nodeName, request, 10_000).thenApply(MetastorageIndexTermResponseMessage.class::cast)
            );
        }

        return allOf(responses.values()).thenApply(unused -> {
            return responses.entrySet().stream()
                    .collect(toMap(Entry::getKey, entry -> indexWithTerm(responses.get(entry.getKey()).join())));
        });
    }

    private static IndexWithTerm indexWithTerm(MetastorageIndexTermResponseMessage message) {
        return new IndexWithTerm(message.raftIndex(), message.raftTerm());
    }

    private static Set<String> nodesWithBestIndexes(Map<String, IndexWithTerm> indexes, int metastorageReplicationFactor) {
        return indexes.entrySet().stream()
                .sorted(Entry.<String, IndexWithTerm>comparingByValue().reversed())
                .limit(metastorageReplicationFactor)
                .map(Entry::getKey)
                .collect(toSet());
    }

    private static String chooseNodeWithBestIndex(Map<String, IndexWithTerm> indexes, Set<String> newMgNodes) {
        return newMgNodes.stream()
                .sorted(Comparator.<String, IndexWithTerm>comparing(indexes::get).reversed())
                .limit(1)
                .findFirst().orElseThrow();
    }

    private CompletableFuture<Void> appointLeader(String bestNodeName, long termBeforeChange, Set<String> newMgNodes) {
        LOG.info("Appointing MG leader forcefully [leader={}].", bestNodeName);

        BecomeMetastorageLeaderMessage request = messagesFactory.becomeMetastorageLeaderMessage()
                .termBeforeChange(termBeforeChange)
                .targetVotingSet(Set.copyOf(newMgNodes))
                .build();

        return messagingService.invoke(bestNodeName, request, 10_000)
                .thenApply(message -> null);
    }
}
