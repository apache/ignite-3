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

import static java.util.concurrent.TimeUnit.SECONDS;
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
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.disaster.system.message.BecomeMetastorageLeaderMessage;
import org.apache.ignite.internal.disaster.system.message.StartMetastorageRepairRequest;
import org.apache.ignite.internal.disaster.system.message.StartMetastorageRepairResponse;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.disaster.system.repair.MetastorageRepair;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.raft.IndexWithTerm;

/**
 * Implementation of {@link MetastorageRepair}.
 */
public class MetastorageRepairImpl implements MetastorageRepair {
    private static final IgniteLogger LOG = Loggers.forClass(MetastorageRepairImpl.class);

    /** Number of seconds to wait for nodes participating in a repair to appear. */
    private static final long WAIT_FOR_NODES_SECONDS = 60;

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
                .thenCompose(unused -> startMetastorageRepair(participatingNodeNames))
                .thenCompose(indexes -> {
                    LOG.info("Collected metastorage indexes [indexes={}].", indexes);

                    Set<String> newMgNodes = nodesWithBestIndexes(indexes, metastorageReplicationFactor);
                    LOG.info("Chose new MG nodes [mgNodes={}].", newMgNodes);

                    String bestNodeName = chooseNodeWithBestIndex(indexes, newMgNodes);
                    LOG.info("Chose best MG node [node={}].", bestNodeName);

                    long bestIndex = indexes.get(bestNodeName).index();
                    return cmgManager.changeMetastorageNodes(newMgNodes, bestIndex + 1)
                            .thenCompose(unused -> initiateForcefulConfigurationChange(
                                    bestNodeName,
                                    indexes.get(bestNodeName).term(),
                                    newMgNodes
                            ))
                            .thenRun(() -> LOG.info(
                                    "Initiated forceful MG configuration change [leader={}, targetVotingSet={}].",
                                    bestNodeName,
                                    newMgNodes
                            ));
                });
    }

    private CompletableFuture<Void> waitTillValidatedNodesContain(Set<String> nodeNames) {
        Set<String> cumulativeValidatedNodeNames = ConcurrentHashMap.newKeySet();
        CompletableFuture<Void> future = new CompletableFuture<>();

        LogicalTopologyEventListener listener = new LogicalTopologyEventListener() {
            @Override
            public void onNodeValidated(LogicalNode validatedNode) {
                LOG.info("Node (awaited by Metastorage repair) has been validated in CMG: {}", validatedNode.name());

                markNodeAsAdded(validatedNode);
            }

            @Override
            public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
                LOG.info("Node (awaited by Metastorage repair) has joined the cluster: {}", joinedNode.name());

                markNodeAsAdded(joinedNode);
            }

            private void markNodeAsAdded(LogicalNode validatedNode) {
                cumulativeValidatedNodeNames.add(validatedNode.name());

                if (isSuperset(cumulativeValidatedNodeNames, nodeNames)) {
                    future.complete(null);
                }
            }

            @Override
            public void onNodeInvalidated(LogicalNode invalidatedNode) {
                LOG.info("Node (awaited by Metastorage repair) has been invalidated in CMG: {}", invalidatedNode.name());

                cumulativeValidatedNodeNames.remove(invalidatedNode.name());
            }
        };

        logicalTopology.addEventListener(listener);

        cmgManager.validatedNodes()
                .thenAccept(validatedNodes -> {
                    Set<String> validatedNodeNames = validatedNodes.stream()
                            .map(InternalClusterNode::name)
                            .collect(toSet());

                    LOG.info("Nodes (awaited by Metastorage repair) that are currently validated/joined in CMG: {}", validatedNodeNames);

                    if (isSuperset(validatedNodeNames, nodeNames)) {
                        future.complete(null);
                    }

                    cumulativeValidatedNodeNames.addAll(validatedNodeNames);
                });

        return future
                .orTimeout(WAIT_FOR_NODES_SECONDS, SECONDS)
                .whenComplete((res, ex) -> {
                    logicalTopology.removeEventListener(listener);

                    if (ex instanceof TimeoutException) {
                        LOG.error("Did not see all participating nodes online in time, failing Metastorage repair, please try again", ex);
                    }
                });
    }

    private static boolean isSuperset(Set<String> container, Set<String> containee) {
        return difference(containee, container).isEmpty();
    }

    private CompletableFuture<Map<String, IndexWithTerm>> startMetastorageRepair(Set<String> participatingNodeNames) {
        LOG.info("Sending StartMetastorageRepair requests to {}", participatingNodeNames);

        StartMetastorageRepairRequest request = messagesFactory.startMetastorageRepairRequest().build();

        Map<String, CompletableFuture<StartMetastorageRepairResponse>> responses = new HashMap<>();

        for (String nodeName : participatingNodeNames) {
            responses.put(
                    nodeName,
                    messagingService.invoke(nodeName, request, 10_000).thenApply(StartMetastorageRepairResponse.class::cast)
            );
        }

        return allOf(responses.values()).thenApply(unused -> {
            return responses.entrySet().stream()
                    .collect(toMap(Entry::getKey, entry -> indexWithTerm(entry.getValue().join())));
        });
    }

    private static IndexWithTerm indexWithTerm(StartMetastorageRepairResponse message) {
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
                .max(Comparator.comparing(indexes::get))
                .orElseThrow();
    }

    private CompletableFuture<Void> initiateForcefulConfigurationChange(
            String bestNodeName,
            long termBeforeChange,
            Set<String> newMgNodes
    ) {
        LOG.info("Initiating forceful MG configuration change [leader={}, targetVotingSet={}].", bestNodeName, newMgNodes);

        BecomeMetastorageLeaderMessage request = messagesFactory.becomeMetastorageLeaderMessage()
                .termBeforeChange(termBeforeChange)
                .targetVotingSet(Set.copyOf(newMgNodes))
                .build();

        return messagingService.invoke(bestNodeName, request, 10_000)
                .thenApply(message -> null);
    }
}
