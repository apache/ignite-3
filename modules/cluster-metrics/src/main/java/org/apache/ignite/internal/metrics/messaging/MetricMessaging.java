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

package org.apache.ignite.internal.metrics.messaging;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.message.MetricDisableRequest;
import org.apache.ignite.internal.metrics.message.MetricDisableResponse;
import org.apache.ignite.internal.metrics.message.MetricEnableRequest;
import org.apache.ignite.internal.metrics.message.MetricEnableResponse;
import org.apache.ignite.internal.metrics.message.MetricSourceDto;
import org.apache.ignite.internal.metrics.message.MetricSourcesRequest;
import org.apache.ignite.internal.metrics.message.MetricSourcesResponse;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Metrics messaging service.
 */
public class MetricMessaging implements IgniteComponent {
    private static final long NETWORK_TIMEOUT_MILLIS = Long.MAX_VALUE;

    private final MetricMessagesFactory messagesFactory = new MetricMessagesFactory();

    private final MetricManager metricManager;

    private final MessagingService messagingService;

    private final TopologyService topologyService;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * Constructor.
     *
     * @param metricManager Metric manager.
     * @param messagingService Messaging service.
     * @param topologyService Topology service.
     */
    public MetricMessaging(MetricManager metricManager, MessagingService messagingService, TopologyService topologyService) {
        this.metricManager = metricManager;
        this.messagingService = messagingService;
        this.topologyService = topologyService;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(MetricMessageTypes.class, (message, sender, correlationId) -> {
            assert correlationId != null;

            if (!busyLock.enterBusy()) {
                sendException(
                        message,
                        sender,
                        requireNonNull(correlationId, "correlationId is null"),
                        new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException())
                );
                return;
            }

            try {
                processRequest(message, sender, requireNonNull(correlationId));
            } finally {
                busyLock.leaveBusy();
            }
        });
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        busyLock.block();
        return nullCompletedFuture();
    }

    private void sendException(NetworkMessage message, InternalClusterNode sender, long correlationId, IgniteInternalException ex) {
        if (message instanceof MetricEnableRequest) {
            sendEnableResponse(ex, sender, correlationId);
        } else if (message instanceof MetricDisableRequest) {
            sendDisableResponse(ex, sender, correlationId);
        } else if (message instanceof MetricSourcesRequest) {
            sendSourcesResponse(null, ex, sender, correlationId);
        }
    }

    private void processRequest(NetworkMessage message, InternalClusterNode sender, long correlationId) {
        if (message instanceof MetricEnableRequest) {
            processEnableRequest((MetricEnableRequest) message, sender, correlationId);
        } else if (message instanceof MetricDisableRequest) {
            processDisableRequest((MetricDisableRequest) message, sender, correlationId);
        } else if (message instanceof MetricSourcesRequest) {
            processSourcesRequest(sender, correlationId);
        }
    }

    private void processEnableRequest(MetricEnableRequest request, InternalClusterNode sender, long correlationId) {
        try {
            metricManager.enable(request.sourceName());
            sendEnableResponse(null, sender, correlationId);
        } catch (IllegalStateException e) {
            sendEnableResponse(e, sender, correlationId);
        }
    }

    private void sendEnableResponse(@Nullable Throwable ex, InternalClusterNode sender, long correlationId) {
        MetricEnableResponse enableResponse = messagesFactory.metricEnableResponse()
                .throwable(ex)
                .build();

        respond(sender, enableResponse, correlationId);
    }

    private void processDisableRequest(MetricDisableRequest request, InternalClusterNode sender, long correlationId) {
        try {
            metricManager.disable(request.sourceName());
            sendDisableResponse(null, sender, correlationId);
        } catch (IllegalStateException e) {
            sendDisableResponse(e, sender, correlationId);
        }
    }

    private void sendDisableResponse(@Nullable Throwable ex, InternalClusterNode sender, long correlationId) {
        MetricDisableResponse disableResponse = messagesFactory.metricDisableResponse()
                .throwable(ex)
                .build();

        respond(sender, disableResponse, correlationId);
    }

    private void processSourcesRequest(InternalClusterNode sender, long correlationId) {
        List<MetricSourceDto> sources = metricManager.metricSources().stream()
                .map(source -> new MetricSourceDto(source.name(), source.enabled()))
                .collect(toList());
        sendSourcesResponse(sources, null, sender, correlationId);
    }

    private void sendSourcesResponse(
            @Nullable Collection<MetricSourceDto> sources,
            @Nullable Throwable ex,
            InternalClusterNode sender,
            long correlationId
    ) {
        MetricSourcesResponse disableResponse = messagesFactory.metricSourcesResponse()
                .sources(sources)
                .throwable(ex)
                .build();

        respond(sender, disableResponse, correlationId);
    }

    /**
     * Broadcasts metric source enable request to all nodes in the cluster.
     *
     * @param sourceName Metric source name.
     * @return The future which will be completed when all nodes responds.
     */
    public CompletableFuture<Void> broadcastMetricEnableAsync(String sourceName) {
        return broadcastAsync(node -> remoteMetricEnableAsync(node, sourceName));
    }

    private CompletableFuture<Void> remoteMetricEnableAsync(InternalClusterNode remoteNode, String sourceName) {
        MetricEnableRequest metricEnableRequest = messagesFactory.metricEnableRequest()
                .sourceName(sourceName)
                .build();

        return invoke(remoteNode, metricEnableRequest).thenCompose(MetricMessaging::fromEnableResponse);
    }

    private static CompletableFuture<Void> fromEnableResponse(NetworkMessage response) {
        Throwable throwable = ((MetricEnableResponse) response).throwable();
        return throwable != null ? failedFuture(throwable) : nullCompletedFuture();
    }

    /**
     * Broadcasts metric source disable request to all nodes in the cluster.
     *
     * @param sourceName Metric source name.
     * @return The future which will be completed when all nodes responds.
     */
    public CompletableFuture<Void> broadcastMetricDisableAsync(String sourceName) {
        return broadcastAsync(node -> remoteMetricDisableAsync(node, sourceName));
    }

    private CompletableFuture<Void> remoteMetricDisableAsync(InternalClusterNode remoteNode, String sourceName) {
        MetricDisableRequest metricDisableRequest = messagesFactory.metricDisableRequest()
                .sourceName(sourceName)
                .build();

        return invoke(remoteNode, metricDisableRequest).thenCompose(MetricMessaging::fromDisableResponse);
    }

    private static CompletableFuture<Void> fromDisableResponse(NetworkMessage response) {
        Throwable throwable = ((MetricDisableResponse) response).throwable();
        return throwable != null ? failedFuture(throwable) : nullCompletedFuture();
    }

    /**
     * Broadcasts metric source list request to all nodes in the cluster. Combines source lists to the map from the node consistent id to
     * the list of metric sources.
     *
     * @return The future which will be completed with the map from the node consistent id to the list of the metric sources for this node.
     */
    public CompletableFuture<Map<String, Collection<MetricSourceDto>>> broadcastMetricSourcesAsync() {
        List<InternalClusterNode> allMembers = new ArrayList<>(topologyService.allMembers());
        //noinspection unchecked
        CompletableFuture<Collection<MetricSourceDto>>[] futures = allMembers.stream()
                .map(this::remoteMetricSourcesAsync)
                .toArray(CompletableFuture[]::new);

        return allOfToList(futures)
                .thenApply(sources -> {
                    Map<String, Collection<MetricSourceDto>> result = new HashMap<>();
                    for (int i = 0; i < allMembers.size(); i++) {
                        InternalClusterNode node = allMembers.get(i);
                        result.put(node.name(), sources.get(i));
                    }
                    return result;
                });
    }

    private CompletableFuture<Collection<MetricSourceDto>> remoteMetricSourcesAsync(InternalClusterNode remoteNode) {
        return invoke(remoteNode, messagesFactory.metricSourcesRequest().build())
                .thenCompose(response -> sourcesFromSourcesResponse((MetricSourcesResponse) response));
    }

    private static CompletableFuture<Collection<MetricSourceDto>> sourcesFromSourcesResponse(MetricSourcesResponse metricSourcesResponse) {
        Throwable throwable = metricSourcesResponse.throwable();
        return throwable != null ? failedFuture(throwable) : completedFuture(metricSourcesResponse.sources());
    }

    /**
     * Broadcasts a request to all nodes in the cluster.
     *
     * @param request Function which maps a node to the request future.
     * @return The future which will be completed when request is processed.
     */
    private CompletableFuture<Void> broadcastAsync(Function<InternalClusterNode, CompletableFuture<Void>> request) {
        CompletableFuture<?>[] futures = topologyService.allMembers().stream()
                .map(request)
                .toArray(CompletableFuture[]::new);

        return allOf(futures);
    }

    private CompletableFuture<NetworkMessage> invoke(InternalClusterNode remoteNode, NetworkMessage msg) {
        return messagingService.invoke(remoteNode.name(), msg, NETWORK_TIMEOUT_MILLIS);
    }

    private void respond(InternalClusterNode sender, NetworkMessage msg, long correlationId) {
        messagingService.respond(sender.name(), msg, correlationId);
    }
}
