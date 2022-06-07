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

package org.apache.ignite.internal.cluster.management;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.network.util.ClusterServiceUtils.resolveNodes;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.apache.ignite.internal.cluster.management.network.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.InitCompleteMessage;
import org.apache.ignite.internal.cluster.management.network.messages.InitErrorMessage;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;

/**
 * Class for performing cluster initialization.
 */
public class ClusterInitializer {
    private static final IgniteLogger LOG = IgniteLogger.forClass(ClusterInitializer.class);

    private final ClusterService clusterService;

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    /** Constructor. */
    public ClusterInitializer(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage. Cannot be empty.
     * @param cmgNodeNames Names of nodes that will host the Cluster Management Group. Can be empty, in which case {@code
     * metaStorageNodeNames} will be used instead.
     * @param clusterName Human-readable name of the cluster.
     * @return Future that represents the state of the operation.
     */
    public CompletableFuture<Void> initCluster(
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName
    ) {
        if (metaStorageNodeNames.isEmpty()) {
            throw new IllegalArgumentException("Meta Storage node names list must not be empty");
        }

        if (metaStorageNodeNames.stream().anyMatch(StringUtils::nullOrBlank)) {
            throw new IllegalArgumentException("Meta Storage node names must not contain blank strings: " + metaStorageNodeNames);
        }

        if (!cmgNodeNames.isEmpty() && cmgNodeNames.stream().anyMatch(StringUtils::nullOrBlank)) {
            throw new IllegalArgumentException("CMG node names must not contain blank strings: " + cmgNodeNames);
        }

        if (clusterName.isBlank()) {
            throw new IllegalArgumentException("Cluster name must not be empty");
        }

        try {
            metaStorageNodeNames = metaStorageNodeNames.stream().map(String::trim).collect(toUnmodifiableList());

            cmgNodeNames = cmgNodeNames.isEmpty()
                    ? metaStorageNodeNames
                    : cmgNodeNames.stream().map(String::trim).collect(toUnmodifiableList());

            // check that provided Meta Storage nodes are present in the topology
            List<ClusterNode> msNodes = resolveNodes(clusterService, metaStorageNodeNames);

            LOG.info("Resolved MetaStorage nodes: {}", msNodes);

            List<ClusterNode> cmgNodes = resolveNodes(clusterService, cmgNodeNames);

            LOG.info("Resolved CMG nodes: {}", cmgNodes);

            CmgInitMessage initMessage = msgFactory.cmgInitMessage()
                    .metaStorageNodes(metaStorageNodeNames)
                    .cmgNodes(cmgNodeNames)
                    .clusterName(clusterName)
                    .build();

            return invokeMessage(cmgNodes, initMessage)
                    .handle((v, e) -> {
                        if (e == null) {
                            LOG.info(
                                    "Init message sent successfully:\n\tCMG nodes: {}\n\tMetaStorage nodes: {}\n\tCluster name: {}",
                                    initMessage.cmgNodes(),
                                    initMessage.metaStorageNodes(),
                                    initMessage.clusterName()
                            );

                            return CompletableFuture.<Void>completedFuture(null);
                        } else {
                            if (e instanceof CompletionException) {
                                e = e.getCause();
                            }

                            LOG.error("Initialization failed: {}", e, e.getMessage());

                            if (e instanceof InternalInitException && !((InternalInitException) e).shouldCancelInit()) {
                                return CompletableFuture.<Void>failedFuture(e);
                            } else {
                                LOG.error("Critical error encountered, rolling back the init procedure");

                                return cancelInit(cmgNodes, e);
                            }
                        }
                    })
                    .thenCompose(Function.identity());
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    private CompletableFuture<Void> cancelInit(Collection<ClusterNode> nodes, Throwable e) {
        CancelInitMessage cancelMessage = msgFactory.cancelInitMessage()
                .reason(e.getMessage())
                .build();

        return sendMessage(nodes, cancelMessage)
                .exceptionally(nestedEx -> {
                    LOG.error("Error when canceling init", nestedEx);

                    e.addSuppressed(nestedEx);

                    return null;
                })
                .thenCompose(v -> failedFuture(e));
    }

    /**
     * Sends a message to all provided nodes.
     *
     * @param nodes nodes to send message to.
     * @param message message to send.
     * @return future that either resolves to a leader node ID or fails if any of the nodes return an error response.
     */
    private CompletableFuture<Void> invokeMessage(Collection<ClusterNode> nodes, NetworkMessage message) {
        return allOf(nodes, node ->
                clusterService.messagingService()
                        .invoke(node, message, 10000)
                        .thenAccept(response -> {
                            if (response instanceof InitErrorMessage) {
                                var errorResponse = (InitErrorMessage) response;

                                throw new InternalInitException(
                                        String.format("Got error response from node \"%s\": %s", node.name(), errorResponse.cause()),
                                        errorResponse.shouldCancel()
                                );
                            } else if (!(response instanceof InitCompleteMessage)) {
                                throw new InternalInitException(
                                        String.format("Unexpected response from node \"%s\": %s", node.name(), response.getClass()),
                                        true
                                );
                            }
                        })
        );
    }

    private CompletableFuture<Void> sendMessage(Collection<ClusterNode> nodes, NetworkMessage message) {
        return allOf(nodes, node -> clusterService.messagingService().send(node, message));
    }

    private static CompletableFuture<Void> allOf(
            Collection<ClusterNode> nodes,
            Function<ClusterNode, CompletableFuture<?>> futureProducer
    ) {
        CompletableFuture<?>[] futures = nodes.stream().map(futureProducer).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures);
    }
}
