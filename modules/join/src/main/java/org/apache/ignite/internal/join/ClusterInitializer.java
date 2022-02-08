/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.join;

import static org.apache.ignite.internal.join.Utils.resolveNodes;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.join.messages.CancelInitMessage;
import org.apache.ignite.internal.join.messages.CmgInitMessage;
import org.apache.ignite.internal.join.messages.InitErrorMessage;
import org.apache.ignite.internal.join.messages.InitMessagesFactory;
import org.apache.ignite.internal.join.messages.LeaderElectedMessage;
import org.apache.ignite.internal.join.messages.MetastorageInitMessage;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;

/**
 * Class for performing cluster initialization.
 */
public class ClusterInitializer {
    private static final IgniteLogger log = IgniteLogger.forClass(ClusterInitializer.class);

    private final ClusterService clusterService;

    /** Constructor. */
    ClusterInitializer(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames names of nodes that will host the Meta Storage. Cannot be empty.
     * @param cmgNodeNames names of nodes that will host the Cluster Management Group. Can be empty,
     *      in which case {@code metaStorageNodeNames} will be used instead.
     * @return future that resolves into leader node IDs if completed successfully.
     */
    public CompletableFuture<Leaders> initCluster(Collection<String> metaStorageNodeNames, Collection<String> cmgNodeNames) {
        try {
            if (metaStorageNodeNames.isEmpty()) {
                throw new IllegalArgumentException("List of metastorage nodes must no be empty");
            }

            cmgNodeNames = cmgNodeNames.isEmpty() ? metaStorageNodeNames : cmgNodeNames;

            @SuppressWarnings("unused")
            List<ClusterNode> metastorageNodes = resolveNodes(clusterService, metaStorageNodeNames);

            @SuppressWarnings("unused")
            List<ClusterNode> cmgNodes = resolveNodes(clusterService, cmgNodeNames);

            // TODO: init message should only be sent to metastorageNodes and cmgNodes respectively,
            //  https://issues.apache.org/jira/browse/IGNITE-16471
            Collection<ClusterNode> allMembers = clusterService.topologyService().allMembers();

            var msgFactory = new InitMessagesFactory();

            MetastorageInitMessage metaStorageInitMessage = msgFactory.metastorageInitMessage()
                    .metastorageNodes(metaStorageNodeNames.toArray(String[]::new))
                    .build();

            CompletableFuture<String> metaStorageInitFuture = invokeMessage(allMembers, metaStorageInitMessage);

            CmgInitMessage cmgInitMessage = msgFactory.cmgInitMessage()
                    .cmgNodes(cmgNodeNames.toArray(String[]::new))
                    .build();

            CompletableFuture<String> cmgInitFuture = invokeMessage(allMembers, cmgInitMessage);

            return metaStorageInitFuture
                    .thenCombine(cmgInitFuture, Leaders::new)
                    .whenComplete((leaders, e) -> {
                        if (e != null) {
                            log.error("Initialization failed, rolling back", e);

                            CancelInitMessage cancelMessage = msgFactory.cancelInitMessage()
                                    .reason(e.getMessage())
                                    .build();

                            cancelInit(allMembers, cancelMessage);
                        }
                    });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Sends a message to all provided nodes.
     *
     * @param nodes nodes to send message to.
     * @param message message to send.
     * @return future that either resolves to a leader node ID or fails if any of the nodes return an error response.
     */
    private CompletableFuture<String> invokeMessage(Collection<ClusterNode> nodes, NetworkMessage message) {
        List<CompletableFuture<String>> futures = nodes.stream()
                .map(node -> invokeMessage(node, message))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                // all futures should return the same response, unless they fail
                .thenCompose(v -> futures.get(0));
    }

    private CompletableFuture<String> invokeMessage(ClusterNode node, NetworkMessage message) {
        return clusterService.messagingService()
                .invoke(node, message, 10000)
                .thenApply(response -> {
                    if (response instanceof InitErrorMessage) {
                        throw new InitException(String.format(
                                "Got error response from node \"%s\": %s", node.name(), ((InitErrorMessage) response).cause()
                        ));
                    }

                    if (!(response instanceof LeaderElectedMessage)) {
                        throw new InitException(String.format(
                                "Unexpected response from node \"%s\": %s", node.name(), response.getClass()
                        ));
                    }

                    return ((LeaderElectedMessage) response).leaderName();
                });
    }

    private void cancelInit(Collection<ClusterNode> nodes, NetworkMessage message) {
        CompletableFuture<?>[] futures = nodes.stream()
                .map(node -> clusterService.messagingService().send(node, message))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).whenComplete((v, e) -> log.error("Error when canceling init", e));
    }
}
