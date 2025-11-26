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

package org.apache.ignite.internal.cluster.management;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationDynamicDefaultsPatcher;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.cluster.management.network.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.CmgPrepareInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.InitCompleteMessage;
import org.apache.ignite.internal.cluster.management.network.messages.InitErrorMessage;
import org.apache.ignite.internal.cluster.management.network.messages.PrepareInitCompleteMessage;
import org.apache.ignite.internal.configuration.validation.ConfigurationDuplicatesValidator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.StringUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Class for performing cluster initialization.
 */
public class ClusterInitializer {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterInitializer.class);

    private static final int INIT_MESSAGE_SEND_TIMEOUT_MILLIS = 10_000;

    private final ClusterService clusterService;

    private final ConfigurationDynamicDefaultsPatcher configurationDynamicDefaultsPatcher;

    private final ConfigurationValidator clusterConfigurationValidator;

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    /** Constructor. */
    public ClusterInitializer(
            ClusterService clusterService,
            ConfigurationDynamicDefaultsPatcher configurationDynamicDefaultsPatcher,
            ConfigurationValidator clusterConfigurationValidator
    ) {
        this.clusterService = clusterService;
        this.configurationDynamicDefaultsPatcher = configurationDynamicDefaultsPatcher;
        this.clusterConfigurationValidator = clusterConfigurationValidator;
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
        return initCluster(metaStorageNodeNames, cmgNodeNames, clusterName, null);
    }

    /**
     * Initializes the cluster for this node.
     *
     * <p>Node selection rules:
     * <ul>
     *     <li>If {@code cmgNodeNames} is empty, it defaults to {@code metaStorageNodeNames}.</li>
     *     <li>If {@code metaStorageNodeNames} is empty, it defaults to {@code cmgNodeNames}.</li>
     *     <li>If both are empty, nodes are auto-selected.</li>
     * </ul>
     *
     * <p>Auto-selection rules based on cluster size:
     * <ul>
     *     <li>For up to 3 nodes, select all available.</li>
     *     <li>For exactly 4 nodes, select 3 to maintain an odd count.</li>
     *     <li>For 5 or more nodes, select 5.</li>
     * </ul>
     * Nodes are chosen in alphabetical order.
     *
     * @param metaStorageNodeNames Nodes for Meta Storage.
     * @param cmgNodeNames Nodes for the Cluster Management Group.
     * @param clusterName Cluster name.
     * @param clusterConfiguration Optional cluster configuration.
     * @return Future representing the operation status.
     */
    public CompletableFuture<Void> initCluster(
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName,
            @Nullable String clusterConfiguration
    ) {
        if (metaStorageNodeNames.stream().anyMatch(StringUtils::nullOrBlank)) {
            throw new IllegalArgumentException("Meta Storage node names must not contain blank strings: " + metaStorageNodeNames);
        }

        Set<String> msNodeNameSet = metaStorageNodeNames.stream().map(String::trim).collect(toUnmodifiableSet());
        if (msNodeNameSet.size() != metaStorageNodeNames.size()) {
            throw new IllegalArgumentException("Meta Storage node names must not contain duplicates: " + metaStorageNodeNames);
        }

        if (cmgNodeNames.stream().anyMatch(StringUtils::nullOrBlank)) {
            throw new IllegalArgumentException("CMG node names must not contain blank strings: " + cmgNodeNames);
        }

        Set<String> cmgNodeNameSet = cmgNodeNames.stream().map(String::trim).collect(toUnmodifiableSet());
        if (cmgNodeNameSet.size() != cmgNodeNames.size()) {
            throw new IllegalArgumentException("CMG node names must not contain duplicates: " + metaStorageNodeNames);
        }

        // See Javadoc for the explanation of how the nodes are chosen.
        if (msNodeNameSet.isEmpty() && cmgNodeNameSet.isEmpty()) {
            Collection<InternalClusterNode> clusterNodes = clusterService.topologyService().allMembers();
            int topologySize = clusterNodes.size();

            // For efficiency, we limit the number of MS and CMG nodes chosen by default.
            // If the cluster has 3 or less nodes, use up to 3 MS/CMG nodes.
            // If the cluster has 5 or more nodes, use 5 MS/CMG nodes.
            // If the cluster has 4 nodes, use 3 MS/CMG nodes to maintain an odd number.
            int msNodesLimit = topologySize < 5 ? 3 : 5;
            Set<String> chosenNodes = clusterNodes.stream()
                    .map(InternalClusterNode::name)
                    .sorted()
                    .limit(msNodesLimit)
                    .collect(Collectors.toSet());
            msNodeNameSet = chosenNodes;
            cmgNodeNameSet = chosenNodes;
        } else if (msNodeNameSet.isEmpty()) {
            msNodeNameSet = cmgNodeNameSet;
        } else if (cmgNodeNames.isEmpty()) {
            cmgNodeNameSet = msNodeNameSet;
        }

        if (clusterName.isBlank()) {
            throw new IllegalArgumentException("Cluster name must not be empty");
        }

        try {
            Map<String, InternalClusterNode> nodesByConsistentId = getValidTopologySnapshot();

            // check that provided Meta Storage nodes are present in the topology
            List<InternalClusterNode> msNodes = resolveNodes(nodesByConsistentId, msNodeNameSet);

            LOG.info("Resolved MetaStorage nodes[nodes={}]", msNodes);

            List<InternalClusterNode> cmgNodes = resolveNodes(nodesByConsistentId, cmgNodeNameSet);

            LOG.info("Resolved CMG nodes[nodes={}]", cmgNodes);

            String patchedClusterConfiguration = patchClusterConfigurationWithDynamicDefaults(clusterConfiguration);

            validateConfiguration(patchedClusterConfiguration, clusterConfiguration);

            CmgPrepareInitMessage prepareInitMessage = msgFactory.cmgPrepareInitMessage()
                    .initInitiatorColocationEnabled(true)
                    .build();

            CmgInitMessage initMessage = msgFactory.cmgInitMessage()
                    .metaStorageNodes(msNodeNameSet)
                    .cmgNodes(cmgNodeNameSet)
                    .clusterName(clusterName)
                    .clusterId(UUID.randomUUID())
                    .initialClusterConfiguration(patchedClusterConfiguration)
                    .build();

            // Handler of prepareInitMessage validates that all CMG nodes have the same enabledColocation mode.
            return invokeMessage(cmgNodes, prepareInitMessage)
                    .thenCompose(ignored -> {
                        LOG.info("CMG initialization preparation completed, going to send init message [initMessage={}].", initMessage);

                        return invokeMessage(cmgNodes, initMessage)
                                .handle((v, e) -> {
                                    if (e == null) {
                                        LOG.info(
                                                "Cluster initialized [clusterName={}, cmgNodes={}, msNodes={}]",
                                                initMessage.clusterName(),
                                                initMessage.cmgNodes(),
                                                initMessage.metaStorageNodes()
                                        );

                                        return CompletableFutures.<Void>nullCompletedFuture();
                                    } else {
                                        if (e instanceof CompletionException) {
                                            e = e.getCause();
                                        }

                                        LOG.warn("Initialization failed [reason={}]", e, e.getMessage());

                                        if (e instanceof InternalInitException && !((InternalInitException) e).shouldCancelInit()) {
                                            return CompletableFuture.<Void>failedFuture(e);
                                        } else {
                                            LOG.debug("Critical error encountered, rolling back the init procedure");

                                            return cancelInit(cmgNodes, e);
                                        }
                                    }
                                })
                                .thenCompose(Function.identity());
                    });
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    /**
     * Validates physical topology before initialization for duplicate consistent ids. Throws {@link InternalInitException} if such
     * duplicate is found.
     *
     * @return A map from consistent id to node.
     */
    private Map<String, InternalClusterNode> getValidTopologySnapshot() {
        Map<String, InternalClusterNode> result = new HashMap<>();
        clusterService.topologyService().allMembers().forEach(node -> {
            if (result.put(node.name(), node) != null) {
                LOG.error("Initialization failed, node \"{}\" has duplicate in the physical topology", node.name());
                throw new InternalInitException(format("Duplicate node name \"{}\"", node.name()), true);
            }
        });
        return result;
    }

    private CompletableFuture<Void> cancelInit(Collection<InternalClusterNode> nodes, Throwable e) {
        CancelInitMessage cancelMessage = msgFactory.cancelInitMessage()
                .reason(e.getMessage())
                .build();

        return sendMessage(nodes, cancelMessage)
                .exceptionally(nestedEx -> {
                    LOG.debug("Error when canceling init", nestedEx);

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
    private CompletableFuture<Void> invokeMessage(Collection<InternalClusterNode> nodes, NetworkMessage message) {
        return allOf(nodes, node ->
                clusterService.messagingService()
                        .invoke(node, message, INIT_MESSAGE_SEND_TIMEOUT_MILLIS)
                        .thenAccept(response -> {
                            if (response instanceof InitErrorMessage) {
                                var errorResponse = (InitErrorMessage) response;

                                LOG.warn("Got error response from node \"{}\": {}", node.name(), errorResponse.cause());

                                throw new InternalInitException(
                                        String.format("Initialization of node \"%s\" failed: %s", node.name(), errorResponse.cause()),
                                        errorResponse.shouldCancel()
                                );
                            } else if (!(response instanceof InitCompleteMessage || response instanceof PrepareInitCompleteMessage)) {
                                throw new InternalInitException(
                                        String.format("Unexpected response from node \"%s\": %s", node.name(), response.getClass()),
                                        true
                                );
                            }
                        })
        );
    }

    private CompletableFuture<Void> sendMessage(Collection<InternalClusterNode> nodes, NetworkMessage message) {
        return allOf(nodes, node -> clusterService.messagingService().send(node, message));
    }

    private static CompletableFuture<Void> allOf(
            Collection<InternalClusterNode> nodes,
            Function<InternalClusterNode, CompletableFuture<?>> futureProducer
    ) {
        CompletableFuture<?>[] futures = nodes.stream().map(futureProducer).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures);
    }

    private static List<InternalClusterNode> resolveNodes(
            Map<String, InternalClusterNode> nodesByConsistentId,
            Collection<String> consistentIds
    ) {
        return consistentIds.stream()
                .map(consistentId -> {
                    InternalClusterNode node = nodesByConsistentId.get(consistentId);

                    if (node == null) {
                        throw new IllegalArgumentException(String.format(
                                "Node \"%s\" is not present in the physical topology", consistentId
                        ));
                    }

                    return node;
                })
                .collect(Collectors.toList());
    }

    private String patchClusterConfigurationWithDynamicDefaults(@Nullable String hocon) {
        return configurationDynamicDefaultsPatcher.patchWithDynamicDefaults(hocon == null ? "" : hocon);
    }

    private void validateConfiguration(String patchedHocon, @Nullable String initialHocon) {
        List<ValidationIssue> issues = clusterConfigurationValidator.validateHocon(patchedHocon);

        if (initialHocon != null) {
            issues.addAll(ConfigurationDuplicatesValidator.validate(initialHocon));
        }

        if (!issues.isEmpty()) {
            throw new ConfigurationValidationException(issues);
        }
    }
}
