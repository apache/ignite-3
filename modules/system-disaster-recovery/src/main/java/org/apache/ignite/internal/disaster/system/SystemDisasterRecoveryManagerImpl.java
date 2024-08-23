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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNullElse;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorageManager;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessageGroup;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterNode;

/**
 * Implementation of {@link SystemDisasterRecoveryManager}.
 */
public class SystemDisasterRecoveryManagerImpl implements SystemDisasterRecoveryManager, IgniteComponent {
    private static final String NODE_INITIALIZED_VAULT_KEY = "systemRecovery.nodeInitialized";
    private static final String CLUSTER_NAME_VAULT_KEY = "systemRecovery.clusterName";
    private static final String RESET_CLUSTER_MESSAGE_VAULT_KEY = "systemRecovery.resetClusterMessage";

    private final String thisNodeName;
    private final TopologyService topologyService;
    private final MessagingService messagingService;
    private final VaultManager vaultManager;
    private final ServerRestarter restarter;

    private final ClusterStateStorageManager clusterStateStorageManager;

    private final SystemDisasterRecoveryMessagesFactory messagesFactory = new SystemDisasterRecoveryMessagesFactory();
    private final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();

    /** Constructor. */
    public SystemDisasterRecoveryManagerImpl(
            String thisNodeName,
            TopologyService topologyService,
            MessagingService messagingService,
            VaultManager vaultManager,
            ServerRestarter restarter,
            ClusterStateStorage clusterStateStorage
    ) {
        this.thisNodeName = thisNodeName;
        this.topologyService = topologyService;
        this.messagingService = messagingService;
        this.vaultManager = vaultManager;
        this.restarter = restarter;

        clusterStateStorageManager = new ClusterStateStorageManager(clusterStateStorage);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(SystemDisasterRecoveryMessageGroup.class, (message, sender, correlationId) -> {
            if (message instanceof ResetClusterMessage) {
                assert correlationId != null;
                handleResetClusterMessage((ResetClusterMessage) message, sender, correlationId);
            } else {
                assert false : "Unsupported message " + message;
            }
        });

        return nullCompletedFuture();
    }

    private void handleResetClusterMessage(ResetClusterMessage message, ClusterNode sender, long correlationId) {
        vaultManager.put(new ByteArray(RESET_CLUSTER_MESSAGE_VAULT_KEY), toBytes(message));

        messagingService.respond(sender, cmgMessagesFactory.successResponseMessage().build(), correlationId)
                .thenRun(() -> {
                    if (!thisNodeName.equals(message.conductor())) {
                        restarter.initiateRestart();
                    }
                });
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public void saveClusterName(String clusterName) {
        ByteArray key = new ByteArray(CLUSTER_NAME_VAULT_KEY);
        byte[] newBytes = clusterName.getBytes(UTF_8);

        VaultEntry existingEntry = vaultManager.get(key);
        if (existingEntry != null) {
            if (!Arrays.equals(existingEntry.value(), newBytes)) {
                throw new IgniteInternalException(
                        INTERNAL_ERR,
                        "Cluster name is different: old one is '" + new String(existingEntry.value(), UTF_8)
                                + "', new one is '" + clusterName + "'."
                );
            }
        } else {
            vaultManager.put(key, newBytes);
        }
    }

    @Override
    public void markNodeInitialized() {
        vaultManager.put(new ByteArray(NODE_INITIALIZED_VAULT_KEY), new byte[]{1});
    }

    @Override
    public CompletableFuture<Void> resetCluster(List<String> proposedCmgConsistentIds) {
        try {
            return doResetCluster(proposedCmgConsistentIds);
        } catch (ClusterResetException e) {
            return failedFuture(e);
        }
    }

    private CompletableFuture<Void> doResetCluster(List<String> proposedCmgConsistentIds) {
        ensureNoRepetitions(proposedCmgConsistentIds);
        ensureContainsThisNodeName(proposedCmgConsistentIds);

        Collection<ClusterNode> nodesInTopology = topologyService.allMembers();
        ensureAllProposedCmgNodesAreInTopology(proposedCmgConsistentIds, nodesInTopology);

        String clusterName = ensureClusterNameIsSaved();
        ensureNodeIsInitialized();
        ClusterState clusterState = ensureClusterStateIsPresent();

        ResetClusterMessage message = buildResetClusterMessage(
                proposedCmgConsistentIds,
                clusterName,
                clusterState
        );

        Map<String, CompletableFuture<NetworkMessage>> responseFutures = sendResetClusterMessageTo(
                nodesInTopology,
                message
        );

        return allOf(responseFutures.values()).handle((res, ex) -> {
            if (isMajorityOfCmgAreSuccesses(proposedCmgConsistentIds, responseFutures)) {
                restarter.initiateRestart();

                return null;
            } else {
                throw new ClusterResetException("Did not get successful responses from new CMG majority, failing cluster reset.");
            }
        });
    }

    private Map<String, CompletableFuture<NetworkMessage>> sendResetClusterMessageTo(Collection<ClusterNode> nodesInTopology,
            ResetClusterMessage message) {
        Map<String, CompletableFuture<NetworkMessage>> responseFutures = new HashMap<>();
        for (ClusterNode node : nodesInTopology) {
            responseFutures.put(node.name(), messagingService.invoke(node, message, 10_000));
        }
        return responseFutures;
    }

    private void ensureNodeIsInitialized() {
        VaultEntry initializedEntry = vaultManager.get(new ByteArray(NODE_INITIALIZED_VAULT_KEY));
        if (initializedEntry == null) {
            throw new ClusterResetException("Node is not initialized and cannot serve as a cluster reset conductor.");
        }
    }

    private static void ensureNoRepetitions(List<String> proposedCmgConsistentIds) {
        if (new HashSet<>(proposedCmgConsistentIds).size() != proposedCmgConsistentIds.size()) {
            throw new ClusterResetException("New CMG node consistentIds have repetitions: " + proposedCmgConsistentIds + ".");
        }
    }

    private void ensureContainsThisNodeName(List<String> proposedCmgConsistentIds) {
        if (!new HashSet<>(proposedCmgConsistentIds).contains(thisNodeName)) {
            throw new ClusterResetException("Current node is not contained in the new CMG, so it cannot conduct a cluster reset.");
        }
    }

    private static void ensureAllProposedCmgNodesAreInTopology(
            List<String> proposedCmgConsistentIds,
            Collection<ClusterNode> nodesInTopology
    ) {
        Set<String> consistentIdsOfNodesInTopology = nodesInTopology.stream().map(ClusterNode::name).collect(toSet());

        HashSet<String> notInTopology = new HashSet<>(proposedCmgConsistentIds);
        notInTopology.removeAll(consistentIdsOfNodesInTopology);

        if (!notInTopology.isEmpty()) {
            throw new ClusterResetException("Some of proposed CMG nodes are not online: " + notInTopology + ".");
        }
    }

    private String ensureClusterNameIsSaved() {
        VaultEntry clusterNameEntry = vaultManager.get(new ByteArray(CLUSTER_NAME_VAULT_KEY));
        if (clusterNameEntry == null) {
            throw new ClusterResetException("Node does not have cluster name saved and cannot serve as a cluster reset conductor.");
        }
        return new String(clusterNameEntry.value(), UTF_8);
    }

    private ClusterState ensureClusterStateIsPresent() {
        ClusterState clusterState = clusterStateStorageManager.getClusterState();
        if (clusterState == null) {
            throw new ClusterResetException("Node does not have cluster state.");
        }
        return clusterState;
    }

    private ResetClusterMessage buildResetClusterMessage(
            List<String> proposedCmgConsistentIds,
            String clusterName,
            ClusterState clusterState
    ) {
        List<UUID> formerClusterIds = new ArrayList<>(requireNonNullElse(clusterState.formerClusterIds(), new ArrayList<>()));
        formerClusterIds.add(clusterState.clusterTag().clusterId());

        return messagesFactory.resetClusterMessage()
                .conductor(thisNodeName)
                .cmgNodes(new HashSet<>(proposedCmgConsistentIds))
                .metaStorageNodes(clusterState.metaStorageNodes())
                .clusterName(clusterName)
                .clusterId(randomUUID())
                .formerClusterIds(formerClusterIds)
                .build();
    }

    private static boolean isMajorityOfCmgAreSuccesses(
            List<String> proposedCmgConsistentIds,
            Map<String, CompletableFuture<NetworkMessage>> responseFutures
    ) {
        Set<String> newCmgNodesSet = new HashSet<>(proposedCmgConsistentIds);
        List<CompletableFuture<NetworkMessage>> futuresFromNewCmg = responseFutures.entrySet().stream()
                .filter(entry -> newCmgNodesSet.contains(entry.getKey()))
                .map(Entry::getValue)
                .collect(toList());

        assert futuresFromNewCmg.size() == proposedCmgConsistentIds.size()
                : futuresFromNewCmg.size() + " futures, but " + proposedCmgConsistentIds.size() + " nodes";

        long successes = futuresFromNewCmg.stream()
                .filter(CompletableFutures::isCompletedSuccessfully)
                .count();

        return successes >= (futuresFromNewCmg.size() + 1) / 2;
    }
}
