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

import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.ignite.network.util.ClusterServiceUtils.resolveNodes;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.cluster.management.network.CmgMessageHandlerFactory;
import org.apache.ignite.internal.cluster.management.network.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.ClusterStateMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.raft.ClusterState;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.CmgRaftGroupListener;
import org.apache.ignite.internal.cluster.management.raft.CmgRaftService;
import org.apache.ignite.internal.cluster.management.rest.InitCommandHandler;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.rest.RestComponent;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TopologyEventHandler;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite component responsible for cluster initialization and managing the Cluster Management Raft Group.
 *
 * <p>Refer to
 * <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">IEP-77</a>
 * for the description of the Cluster Management Group and its responsibilities.
 */
public class ClusterManagementGroupManager implements IgniteComponent {
    // TODO: timeout should be configurable, see https://issues.apache.org/jira/browse/IGNITE-16785
    private static final int NETWORK_INVOKE_TIMEOUT = 500;

    private static final IgniteLogger log = IgniteLogger.forClass(ClusterManagementGroupManager.class);

    /** CMG Raft group name. */
    private static final String CMG_RAFT_GROUP_NAME = "cmg_raft_group";

    /** Init REST endpoint path. */
    private static final String REST_ENDPOINT = "/management/v1/cluster/init";

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Future that resolves into a CMG Raft service. Can be {@code null} if the Raft service has not been started. */
    @Nullable
    private CompletableFuture<CmgRaftService> raftService;

    /** Lock for the {@code raftService} field. */
    private final Object raftServiceLock = new Object();

    /** Future that resolves into a list of node names that host the Meta Storage. */
    private final CompletableFuture<Collection<String>> metaStorageNodes = new CompletableFuture<>();

    /** Message factory. */
    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    /** Delayed executor. */
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    private final ClusterService clusterService;

    private final Loza raftManager;

    private final RestComponent restComponent;

    private final ClusterStateStorage clusterStateStorage;

    /** Local state. */
    private final LocalStateStorage localStateStorage;

    /** Handles cluster initialization flow. */
    private final ClusterInitializer clusterInitializer;

    /** Constructor. */
    public ClusterManagementGroupManager(
            VaultManager vault,
            ClusterService clusterService,
            Loza raftManager,
            RestComponent restComponent,
            ClusterStateStorage clusterStateStorage
    ) {
        this.clusterService = clusterService;
        this.raftManager = raftManager;
        this.restComponent = restComponent;
        this.clusterStateStorage = clusterStateStorage;
        this.localStateStorage = new LocalStateStorage(vault);
        this.clusterInitializer = new ClusterInitializer(clusterService);
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames names of nodes that will host the Meta Storage.
     * @param cmgNodeNames names of nodes that will host the Cluster Management Group.
     */
    public void initCluster(Collection<String> metaStorageNodeNames, Collection<String> cmgNodeNames) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            clusterInitializer.initCluster(metaStorageNodeNames, cmgNodeNames).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new InitException("Interrupted while initializing the cluster", e);
        } catch (ExecutionException e) {
            throw new InitException("Unable to initialize the cluster", e.getCause());
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void start() {
        var messageHandlerFactory = new CmgMessageHandlerFactory(busyLock, msgFactory, clusterService);

        // register the ClusterState handler first, because local state recovery might send such messages
        clusterService.messagingService().addMessageHandler(
                CmgMessageGroup.class,
                messageHandlerFactory.wrapHandler((message, senderAddr, correlationId) -> {
                    if (message instanceof ClusterStateMessage) {
                        assert correlationId != null;

                        handleClusterState((ClusterStateMessage) message, senderAddr, correlationId);
                    }
                })
        );

        synchronized (raftServiceLock) {
            raftService = recoverLocalState();
        }

        // register the Init handler second in order to handle the command differently, depending on the local state
        clusterService.messagingService().addMessageHandler(
                CmgMessageGroup.class,
                messageHandlerFactory.wrapHandler((message, senderAddr, correlationId) -> {
                    if (message instanceof CancelInitMessage) {
                        handleCancelInit((CancelInitMessage) message);
                    } else if (message instanceof CmgInitMessage) {
                        assert correlationId != null;

                        handleInit((CmgInitMessage) message, senderAddr, correlationId);
                    }
                })
        );

        restComponent.registerHandlers(routes ->
                routes.post(REST_ENDPOINT, APPLICATION_JSON.toString(), new InitCommandHandler(clusterInitializer))
        );
    }

    /**
     * Extracts the local state (if any) and starts the CMG.
     *
     * @return Future that resolves into the CMG Raft service or {@code null} if the local state is empty.
     */
    @Nullable
    private CompletableFuture<CmgRaftService> recoverLocalState() {
        Collection<String> cmgNodes;

        try {
            cmgNodes = localStateStorage.cmgNodeNames().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException("Interrupted while retrieving local CMG state", e);
        } catch (ExecutionException e) {
            throw new IgniteInternalException("Error while retrieving local CMG state", e);
        }

        if (cmgNodes.isEmpty()) {
            return null;
        }

        log.info("Local CMG state recovered, starting the CMG");

        return startCmgRaftService(cmgNodes)
                .thenCompose(service -> service.isCurrentNodeLeader()
                        .thenCompose(isLeader -> {
                            if (isLeader) {
                                return service.readClusterState()
                                        // Raft state might not have been initialized in case of leader failure during cluster init
                                        // TODO: properly handle this case, see https://issues.apache.org/jira/browse/IGNITE-16819
                                        .thenCompose(state -> state == null ? completedFuture(null) : onLeaderElected(service, state));
                            } else {
                                return completedFuture(null);
                            }
                        })
                        .thenApply(v -> service));
    }

    /**
     * Handles the Init command.
     *
     * <p>This method needs to take the following possibilities into account, depending on the local state and the Raft state:
     * <ol>
     *     <li>No local state found (the Raft service future is {@code null}) - this means that the current node has never been initialized
     *     before.</li>
     *     <li>Local state found (the Raft service future has been started and therefore is not {@code null}),
     *     but no CMG state present in the Raft storage - this means that the node has failed somewhere during
     *     the init process. In this case we need to check the consistency of the local state and the received message and complete
     *     the init process.</li>
     *     <li>Local state found and CMG state is present in the Raft storage - this means that the node has been initialized successfully
     *     and a user may be retrying the init in case the successful response was lost. To make the init message idempotent
     *     we simply check that the Raft state and the received message are the same.</li>
     * </ol>
     */
    private void handleInit(CmgInitMessage msg, NetworkAddress addr, long correlationId) {
        var newState = new ClusterState(msg.cmgNodes(), msg.metaStorageNodes());

        // This future is needed to add a completion listener at the end of the method
        CompletableFuture<?> resultHook;

        synchronized (raftServiceLock) {
            if (raftService == null) {
                // Raft service has not been started
                log.info("Init command received, starting the CMG: " + newState);

                raftService = initCmgRaftService(newState);

                resultHook = raftService;
            } else {
                // Raft service has been started, which means that this node has already received an init command at least once, but
                // we still need to check that the initialization has completed successfully.
                log.info("Init command received, but the CMG has already been started");

                resultHook = raftService.thenCompose(service ->
                        service.readClusterState()
                                .thenCompose(state -> {
                                    if (state == null) {
                                        // Raft state is empty, perform re-initialization
                                        log.info("CMG state is missing, completing initialization");

                                        if (service.nodeNames().equals(newState.cmgNodes())) {
                                            return service.isCurrentNodeLeader()
                                                    .thenCompose(isCurrentNodeLeader ->
                                                            isCurrentNodeLeader ? initCmgState(service, newState) : completedFuture(null));
                                        } else {
                                            throw new IllegalInitArgumentException(String.format(
                                                    "CMG has already been initialized with %s, but the new state is different: %s",
                                                    service.nodeNames(), newState.cmgNodes()
                                            ));
                                        }
                                    } else {
                                        // Node is fully initialized, just check some invariants
                                        log.info("Node has already been initialized");

                                        if (state.equals(newState)) {
                                            return completedFuture(null);
                                        } else {
                                            throw new IllegalInitArgumentException(String.format(
                                                    "CMG has already been initialized with %s, but the new state is different: %s",
                                                    state, newState
                                            ));
                                        }
                                    }
                                }));
            }
        }

        resultHook.whenComplete((v, e) -> {
            NetworkMessage response;

            if (e == null) {
                response = msgFactory.initCompleteMessage().build();
            } else {
                response = msgFactory.initErrorMessage()
                        .cause(e.getMessage())
                        .shouldCancel(!(e instanceof IllegalInitArgumentException))
                        .build();
            }

            clusterService.messagingService().respond(addr, response, correlationId);
        });
    }

    /**
     * Starts the CMG Raft service and writes the given {@code state} to the storage.
     */
    private CompletableFuture<CmgRaftService> initCmgRaftService(ClusterState state) {
        return localStateStorage.putCmgNodeNames(state.cmgNodes())
                .thenCompose(v -> startCmgRaftService(state.cmgNodes()))
                .thenCompose(service -> service.isCurrentNodeLeader()
                        .thenCompose(isLeader -> isLeader ? initCmgState(service, state) : completedFuture(null))
                        .thenApply(v -> service));
    }

    /**
     * Writes the given state to the CMG's STM and executes some necessary on-leader logic.
     */
    private CompletableFuture<Void> initCmgState(CmgRaftService service, ClusterState state) {
        return service.writeClusterState(state).thenCompose(v -> onLeaderElected(service, state));
    }

    /**
     * Executes the following actions when a CMG leader is elected.
     * <ol>
     *     <li>Updates the logical topology in case some nodes have gone offline during leader election.</li>
     *     <li>Broadcasts the current CMG state to all nodes in the physical topology.</li>
     * </ol>
     */
    private CompletableFuture<Void> onLeaderElected(CmgRaftService service, ClusterState state) {
        return updateLogicalTopology(service)
                .thenRun(() -> {
                    // TODO: remove listeners if leadership is lost, see https://issues.apache.org/jira/browse/IGNITE-16842
                    clusterService.topologyService().addEventHandler(cmgLeaderTopologyEventHandler(service));

                    sendClusterState(state, clusterService.topologyService().allMembers());
                });
    }

    /**
     * This method must be executed upon CMG leader election in order to regain logical topology consistency in case some nodes left the
     * physical topology during the election. Newly appeared nodes will be added automatically after the new leader broadcasts the current
     * cluster state.
     */
    private CompletableFuture<Void> updateLogicalTopology(CmgRaftService service) {
        return service.logicalTopology()
                .thenAccept(logicalTopology -> {
                    Collection<String> physicalTopologyIds = clusterService.topologyService().allMembers()
                            .stream()
                            .map(ClusterNode::id)
                            .collect(toSet());

                    Set<ClusterNode> nodesToRemove = logicalTopology.stream()
                            .filter(node -> !physicalTopologyIds.contains(node.id()))
                            .collect(toUnmodifiableSet());

                    service.removeFromCluster(nodesToRemove);
                });
    }

    private void handleCancelInit(CancelInitMessage msg) {
        log.info("CMG initialization cancelled, reason: " + msg.reason());

        destroyCmg();
    }

    /**
     * Completely destroys the local CMG Raft service.
     */
    private void destroyCmg() {
        synchronized (raftServiceLock) {
            try {
                if (raftService != null) {
                    raftService.cancel(true);

                    raftService = null;
                }

                raftManager.stopRaftGroup(CMG_RAFT_GROUP_NAME);

                if (clusterStateStorage.isStarted()) {
                    clusterStateStorage.destroy();
                }

                localStateStorage.clear().get();
            } catch (Exception e) {
                throw new IgniteInternalException("Error when cleaning the CMG state", e);
            }
        }
    }

    /**
     * Handler for the {@link ClusterStateMessage}.
     */
    private void handleClusterState(ClusterStateMessage msg, NetworkAddress addr, long correlationId) {
        clusterService.messagingService().respond(addr, msgFactory.successResponseMessage().build(), correlationId);

        var state = new ClusterState(msg.cmgNodes(), msg.metaStorageNodes());

        synchronized (raftServiceLock) {
            if (raftService == null) {
                raftService = initCmgRaftService(state);
            } else {
                // Raft service might have been started on wrong CMG nodes, because CMG state can change while a node is offline. In this
                // case we need to re-create the service.
                raftService = raftService.thenCompose(service -> {
                    if (service.nodeNames().equals(state.cmgNodes())) {
                        return completedFuture(service);
                    } else {
                        if (log.isInfoEnabled()) {
                            log.info("CMG has been started on {}, but the cluster state is different: {}. "
                                    + "Re-creating the CMG Raft service", service.nodeNames(), state.cmgNodes());
                        }

                        destroyCmg();

                        return initCmgRaftService(state);
                    }
                });
            }

            raftService
                    .thenCompose(CmgRaftService::joinCluster)
                    .thenRun(() -> metaStorageNodes.complete(state.metaStorageNodes()));
        }
    }

    /**
     * Starts the CMG Raft service using the provided node names as its peers.
     */
    private CompletableFuture<CmgRaftService> startCmgRaftService(Collection<String> nodeNames) {
        // TODO: wait for nodes to appear, see https://issues.apache.org/jira/browse/IGNITE-16811
        List<ClusterNode> nodes = resolveNodes(clusterService, nodeNames);

        try {
            return raftManager
                    .prepareRaftGroup(CMG_RAFT_GROUP_NAME, nodes, () -> {
                        clusterStateStorage.start();

                        return new CmgRaftGroupListener(clusterStateStorage);
                    })
                    .thenApply(service -> new CmgRaftService(service, clusterService));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private TopologyEventHandler cmgLeaderTopologyEventHandler(CmgRaftService raftService) {
        return new TopologyEventHandler() {
            @Override
            public void onAppeared(ClusterNode member) {
                raftService.readClusterState()
                        .thenAccept(state -> {
                            if (state != null) {
                                sendClusterState(state, List.of(member));
                            } else if (log.isWarnEnabled()) {
                                log.warn("Cannot send the cluster state to a newly added node {} because cluster state is empty", member);
                            }
                        });
            }

            @Override
            public void onDisappeared(ClusterNode member) {
                scheduleRemoveFromLogicalTopology(raftService, member);
            }
        };
    }

    private void scheduleRemoveFromLogicalTopology(CmgRaftService raftService, ClusterNode node) {
        // TODO: delay should be configurable, see https://issues.apache.org/jira/browse/IGNITE-16785
        scheduledExecutor.schedule(() -> {
            ClusterNode physicalTopologyNode = clusterService.topologyService().getByConsistentId(node.name());

            if (physicalTopologyNode == null || !physicalTopologyNode.id().equals(node.id())) {
                raftService.removeFromCluster(Set.of(node));
            }
        }, 0, TimeUnit.MILLISECONDS);
    }

    private void sendClusterState(ClusterState clusterState, Collection<ClusterNode> nodes) {
        // TODO: add validation information, see https://issues.apache.org/jira/browse/IGNITE-16717
        NetworkMessage msg = msgFactory.clusterStateMessage()
                .cmgNodes(clusterState.cmgNodes())
                .metaStorageNodes(clusterState.metaStorageNodes())
                .build();

        for (ClusterNode node : nodes) {
            sendWithRetry(node, msg);
        }
    }

    private CompletableFuture<NetworkMessage> sendWithRetry(ClusterNode node, NetworkMessage msg) {
        var result = new CompletableFuture<NetworkMessage>();

        sendWithRetry(node, msg, result, 5);

        return result;
    }

    private void sendWithRetry(ClusterNode node, NetworkMessage msg, CompletableFuture<NetworkMessage> result, int attempts) {
        clusterService.messagingService().invoke(node, msg, NETWORK_INVOKE_TIMEOUT)
                .whenComplete((response, e) -> {
                    if (e == null) {
                        result.complete(response);
                    } else if (attempts == 1) {
                        result.completeExceptionally(e);
                    } else {
                        log.info("Exception when sending message to " + node.name() + ", retrying", e);

                        scheduledExecutor.schedule(() -> sendWithRetry(node, msg, result, attempts - 1), 500, TimeUnit.MILLISECONDS);
                    }
                });
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        IgniteUtils.shutdownAndAwaitTermination(scheduledExecutor, 10, TimeUnit.SECONDS);

        IgniteUtils.closeAll(
                () -> raftManager.stopRaftGroup(CMG_RAFT_GROUP_NAME),
                clusterStateStorage
        );
    }

    /**
     * Returns a future that, when complete, resolves into a list of node names that host the Meta Storage.
     *
     * @return Future that, when complete, resolves into a list of node names that host the Meta Storage.
     */
    public CompletableFuture<Collection<String>> metaStorageNodes() {
        return metaStorageNodes;
    }

    /**
     * Returns a future that, when complete, resolves into a list of nodes that comprise the logical topology.
     *
     * @return Future that, when complete, resolves into a list of nodes that comprise the logical topology.
     */
    public CompletableFuture<Collection<ClusterNode>> logicalTopology() {
        synchronized (raftServiceLock) {
            return raftService.thenCompose(CmgRaftService::logicalTopology);
        }
    }
}
