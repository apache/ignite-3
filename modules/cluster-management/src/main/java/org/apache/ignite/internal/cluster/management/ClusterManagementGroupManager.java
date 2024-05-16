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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.ignite.internal.cluster.management.ClusterTag.clusterTag;
import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.cancelOrConsume;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.cluster.management.LocalStateStorage.LocalState;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.events.BeforeStartRaftGroupEventParameters;
import org.apache.ignite.internal.cluster.management.events.ClusterManagerGroupEvent;
import org.apache.ignite.internal.cluster.management.events.EmptyEventParameters;
import org.apache.ignite.internal.cluster.management.network.CmgMessageHandlerFactory;
import org.apache.ignite.internal.cluster.management.network.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.ClusterStateMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.CmgRaftGroupListener;
import org.apache.ignite.internal.cluster.management.raft.CmgRaftService;
import org.apache.ignite.internal.cluster.management.raft.IllegalInitArgumentException;
import org.apache.ignite.internal.cluster.management.raft.JoinDeniedException;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.event.EventParameters;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Ignite component responsible for cluster initialization and managing the Cluster Management Raft Group.
 *
 * <p>Refer to
 * <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">IEP-77</a>
 * for the description of the Cluster Management Group and its responsibilities.
 */
public class ClusterManagementGroupManager extends AbstractEventProducer<ClusterManagerGroupEvent, EventParameters>
        implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterManagementGroupManager.class);

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Future that resolves into a CMG Raft service. Can be {@code null} if the Raft service has not been started. */
    @Nullable
    private volatile CompletableFuture<CmgRaftService> raftService;

    /** Lock for the {@code raftService} field. */
    private final Object raftServiceLock = new Object();

    /**
     * Future that resolves after the node has been validated on the CMG leader.
     */
    private final CompletableFuture<Void> joinFuture = new CompletableFuture<>();

    /** Message factory. */
    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    /** Delayed executor. */
    private final ScheduledExecutorService scheduledExecutor;

    private final ClusterService clusterService;

    private final RaftManager raftManager;

    private final ClusterStateStorage clusterStateStorage;

    private final LogicalTopology logicalTopology;

    private final ClusterManagementConfiguration configuration;

    /** Local state. */
    private final LocalStateStorage localStateStorage;

    /** Handles cluster initialization flow. */
    private final ClusterInitializer clusterInitializer;

    /** Local node's attributes. */
    private final NodeAttributes nodeAttributes;

    /** Future that resolves into the initial cluster configuration in HOCON format. */
    private final CompletableFuture<String> initialClusterConfigurationFuture = new CompletableFuture<>();

    /** Failure processor that is used to handle critical errors. */
    private final FailureProcessor failureProcessor;

    /** Constructor. */
    public ClusterManagementGroupManager(
            VaultManager vault,
            ClusterService clusterService,
            ClusterInitializer clusterInitializer,
            RaftManager raftManager,
            ClusterStateStorage clusterStateStorage,
            LogicalTopology logicalTopology,
            ClusterManagementConfiguration configuration,
            NodeAttributes nodeAttributes,
            FailureProcessor failureProcessor
    ) {
        this.clusterService = clusterService;
        this.clusterInitializer = clusterInitializer;
        this.raftManager = raftManager;
        this.clusterStateStorage = clusterStateStorage;
        this.logicalTopology = logicalTopology;
        this.configuration = configuration;
        this.localStateStorage = new LocalStateStorage(vault);
        this.nodeAttributes = nodeAttributes;
        this.failureProcessor = failureProcessor;

        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(clusterService.nodeName(), "cmg-manager", LOG)
        );
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @param cmgNodeNames Names of nodes that will host the Cluster Management Group.
     * @param clusterName Human-readable name of the cluster.
     */
    public void initCluster(
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName
    ) throws NodeStoppingException {
        initCluster(metaStorageNodeNames, cmgNodeNames, clusterName, null);
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @param cmgNodeNames Names of nodes that will host the Cluster Management Group.
     * @param clusterName Human-readable name of the cluster.
     * @param clusterConfiguration Cluster configuration.
     */
    public void initCluster(
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName,
            @Nullable String clusterConfiguration
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            clusterInitializer.initCluster(metaStorageNodeNames, cmgNodeNames, clusterName, clusterConfiguration).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new InitException("Interrupted while initializing the cluster", e);
        } catch (ExecutionException e) {
            throw new InitException("Unable to initialize the cluster: " + e.getCause().getMessage(), e.getCause());
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        synchronized (raftServiceLock) {
            raftService = recoverLocalState();
        }

        var messageHandlerFactory = new CmgMessageHandlerFactory(busyLock, msgFactory, clusterService);

        clusterService.messagingService().addMessageHandler(
                CmgMessageGroup.class,
                messageHandlerFactory.wrapHandler((message, sender, correlationId) -> {
                    if (message instanceof ClusterStateMessage) {
                        assert correlationId != null;

                        handleClusterState((ClusterStateMessage) message, sender, correlationId);
                    } else if (message instanceof CancelInitMessage) {
                        handleCancelInit((CancelInitMessage) message);
                    } else if (message instanceof CmgInitMessage) {
                        assert correlationId != null;

                        handleInit((CmgInitMessage) message, sender, correlationId);
                    }
                })
        );

        return nullCompletedFuture();
    }

    /**
     * Returns the cluster state future or the future that will be resolved to null if the cluster is not initialized yet.
     */
    public CompletableFuture<ClusterState> clusterState() {
        CompletableFuture<CmgRaftService> serviceFuture = raftService;

        return serviceFuture == null ? nullCompletedFuture() : serviceFuture.thenCompose(CmgRaftService::readClusterState);
    }

    /**
     * Extracts the local state (if any) and starts the CMG.
     *
     * @return Future, that resolves into the CMG Raft service, or {@code null} if the local state is empty.
     */
    @Nullable
    private CompletableFuture<CmgRaftService> recoverLocalState() {
        LocalState localState = localStateStorage.getLocalState();

        if (localState == null) {
            return null;
        }

        LOG.info("Local CMG state recovered, starting the CMG");

        // Since we recovered state we do not supply a new initialClusterConfig.
        return startCmgRaftServiceWithEvents(localState.cmgNodeNames(), null)
                .thenCompose(service -> joinCluster(service, localState.clusterTag()));
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
    private void handleInit(CmgInitMessage msg, ClusterNode sender, long correlationId) {
        synchronized (raftServiceLock) {
            CompletableFuture<CmgRaftService> serviceFuture = raftService;

            if (serviceFuture == null) {
                // Raft service has not been started
                LOG.info("Init command received, starting the CMG [nodes={}]", msg.cmgNodes());

                serviceFuture = startCmgRaftServiceWithEvents(msg.cmgNodes(), msg.initialClusterConfiguration());
            } else {
                // Raft service has been started, which means that this node has already received an init command at least once.
                LOG.info("Init command received, but the CMG has already been started");
            }

            // Every node, that receives the init command, tries to initialize the CMG state. Raft listener will correctly
            // handle this case by applying only the first attempt and returning the actual cluster state for all other
            // attempts.
            raftService = serviceFuture
                    .thenCompose(service -> doInit(service, msg)
                            .handle((v, e) -> {
                                NetworkMessage response;

                                if (e == null) {
                                    LOG.info("CMG initialized successfully");

                                    response = msgFactory.initCompleteMessage().build();
                                } else {
                                    if (e instanceof CompletionException) {
                                        e = e.getCause();
                                    }

                                    LOG.debug("Error when initializing the CMG", e);

                                    response = msgFactory.initErrorMessage()
                                            .cause(e.getMessage())
                                            .shouldCancel(!(e instanceof IllegalInitArgumentException))
                                            .build();
                                }

                                clusterService.messagingService().respond(sender, response, correlationId);

                                return service;
                            }));
        }
    }

    private CompletableFuture<CmgRaftService> doInit(CmgRaftService service, CmgInitMessage msg) {
        return service.initClusterState(createClusterState(msg))
                .thenCompose(state -> {
                    var localState = new LocalState(state.cmgNodes(), state.clusterTag());

                    localStateStorage.saveLocalState(localState);

                    return joinCluster(service, state.clusterTag());
                });
    }

    private ClusterState createClusterState(CmgInitMessage msg) {
        return msgFactory.clusterState()
                .cmgNodes(Set.copyOf(msg.cmgNodes()))
                .metaStorageNodes(Set.copyOf(msg.metaStorageNodes()))
                .version(IgniteProductVersion.CURRENT_VERSION.toString())
                .clusterTag(clusterTag(msgFactory, msg.clusterName()))
                .initialClusterConfiguration(msg.initialClusterConfiguration())
                .build();
    }

    /**
     * Executes the following actions when this node is elected as a CMG leader.
     * <ol>
     *     <li>Updates the logical topology in case some nodes have gone offline during leader election.</li>
     *     <li>Broadcasts the current CMG state to all nodes in the physical topology.</li>
     * </ol>
     */
    private void onElectedAsLeader(long term) {
        if (!busyLock.enterBusy()) {
            LOG.info("Skipping onLeaderElected callback, because the node is stopping");

            return;
        }

        try {
            LOG.info("CMG leader has been elected, executing onLeaderElected callback");

            // The cluster state is broadcast via the messaging service; hence, the future must be completed here on the leader node.
            raftServiceAfterJoin().thenAccept(service -> inBusyLock(busyLock, () -> {
                service.readClusterState()
                        .thenAccept(state -> initialClusterConfigurationFuture.complete(state.initialClusterConfiguration()));

                updateLogicalTopology(service)
                        .thenCompose(v -> inBusyLock(busyLock, () -> service.updateLearners(term)))
                        .thenAccept(v -> inBusyLock(busyLock, () -> {
                            // Register a listener to send ClusterState messages to new nodes.
                            TopologyService topologyService = clusterService.topologyService();

                            // TODO: remove listeners if leadership is lost, see https://issues.apache.org/jira/browse/IGNITE-16842
                            topologyService.addEventHandler(cmgLeaderTopologyEventHandler(service));

                            // Send the ClusterStateMessage to all members of the physical topology. We do not wait for the send operation
                            // because being unable to send ClusterState messages should not fail the CMG service startup.
                            ClusterNode thisNode = topologyService.localMember();

                            Collection<ClusterNode> otherNodes = topologyService.allMembers().stream()
                                    .filter(node -> !thisNode.equals(node))
                                    .collect(toList());

                            sendClusterState(service, otherNodes);
                        }))
                        .whenComplete((v, e) -> {
                            if (e != null) {
                                if (unwrapCause(e) instanceof NodeStoppingException) {
                                    LOG.info("Unable to execute onLeaderElected callback, because the node is stopping", e);
                                } else {
                                    LOG.error("Error when executing onLeaderElected callback", e);
                                }
                            } else {
                                LOG.info("onLeaderElected callback executed successfully");
                            }
                        });
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * This method must be executed upon CMG leader election in order to regain logical topology consistency in case some nodes left the
     * physical topology during the election. Newly appeared nodes will be added automatically after the new leader broadcasts the current
     * cluster state.
     */
    private CompletableFuture<Void> updateLogicalTopology(CmgRaftService service) {
        return service.logicalTopology()
                .thenCompose(logicalTopology -> inBusyLock(busyLock, () -> {
                    Set<String> physicalTopologyIds = clusterService.topologyService().allMembers()
                            .stream()
                            .map(ClusterNode::id)
                            .collect(toSet());

                    Set<ClusterNode> nodesToRemove = logicalTopology.nodes().stream()
                            .filter(node -> !physicalTopologyIds.contains(node.id()))
                            .collect(toUnmodifiableSet());

                    // TODO: IGNITE-18681 - respect removal timeout.
                    return nodesToRemove.isEmpty() ? nullCompletedFuture() : service.removeFromCluster(nodesToRemove);
                }));
    }

    private void handleCancelInit(CancelInitMessage msg) {
        LOG.info("CMG initialization cancelled [reason={}]", msg.reason());
        this.scheduledExecutor.execute(this::destroyCmgWithEvents);
    }

    /** Delegates call to {@link #destroyCmg()} but fires the associated events. */
    private CompletableFuture<Void> destroyCmgWithEvents() {
        LOG.info("CMG cancellation procedure started");
        return inBusyLockAsync(busyLock,
                () -> fireEvent(ClusterManagerGroupEvent.BEFORE_DESTROY_RAFT_GROUP, EmptyEventParameters.INSTANCE)
                    .thenRunAsync(this::destroyCmg, this.scheduledExecutor)
                    .exceptionally(err -> {
                        failureProcessor.process(new FailureContext(CRITICAL_ERROR, err));
                        throw (err instanceof RuntimeException) ? (RuntimeException) err : new CompletionException(err);
                    })
        );
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

                raftManager.stopRaftNodes(CmgGroupId.INSTANCE);

                localStateStorage.clear();
            } catch (Exception e) {
                throw new IgniteInternalException("Error when cleaning the CMG state", e);
            }
        }
    }

    /**
     * Handler for the {@link ClusterStateMessage}.
     */
    private void handleClusterState(ClusterStateMessage msg, ClusterNode sender, long correlationId) {
        clusterService.messagingService().respond(sender, msgFactory.successResponseMessage().build(), correlationId);

        ClusterState state = msg.clusterState();

        // Complete the initialClusterConfigurationFuture to initialize the cluster configuration on the local node.
        initialClusterConfigurationFuture.complete(state.initialClusterConfiguration());

        synchronized (raftServiceLock) {
            if (raftService == null) {
                LOG.info("ClusterStateMessage received, starting the CMG [nodes={}]", state.cmgNodes());

                raftService = initCmgRaftService(state);
            } else {
                // Raft service might have been started on wrong CMG nodes, because CMG state can change while a node is offline. In this
                // case we need to re-create the service.
                raftService = raftService
                        .handle((service, e) -> {
                            if (service != null && service.nodeNames().equals(state.cmgNodes())) {
                                LOG.info("ClusterStateMessage received, but the CMG service is already started");

                                return completedFuture(service);
                            }

                            if (service == null) {
                                // Service could not be started for some reason, which might be due to starting on incorrect CMG nodes
                                assert e != null;

                                if (e instanceof CompletionException) {
                                    e = e.getCause();
                                }

                                // Nothing can be done if the node has not passed validation.
                                if (e instanceof JoinDeniedException) {
                                    return CompletableFuture.<CmgRaftService>failedFuture(e);
                                }

                                LOG.warn("CMG service could not be started on previous attempts. "
                                        + "Re-creating the CMG Raft service [reason={}]", e, e.getMessage());

                                return initCmgRaftService(state);
                            } else {
                                LOG.warn("CMG service started, but the cluster state is different. "
                                                + "Re-creating the CMG Raft service [localState={}, clusterState={}]",
                                        service.nodeNames(), state.cmgNodes());

                                return destroyCmgWithEvents()
                                        .thenCompose(none -> initCmgRaftService(state));
                            }
                        })
                        .thenCompose(Function.identity());
            }
        }
    }

    private CompletableFuture<CmgRaftService> joinCluster(CmgRaftService service, ClusterTag clusterTag) {
        return service.startJoinCluster(clusterTag, nodeAttributes)
                .thenApply(v -> service)
                .whenComplete((v, e) -> {
                    if (e == null) {
                        LOG.info("Successfully joined the cluster [name={}]", clusterTag.clusterName());

                        joinFuture.complete(null);
                    } else {
                        joinFuture.completeExceptionally(e);
                    }
                });
    }

    /**
     * Delegates call to {@link #startCmgRaftService(Set)} but fires the associated events.
     *
     * @param initialClusterConfig the initial cluster configuration provided by the
     *         {@link CmgInitMessage#initialClusterConfiguration()} if the cluster is being initialized for the first time, as part of a
     *         cluster init. Otherwise {@code null}, if starting after recovering state of an already initialized cluster.
     */
    private CompletableFuture<CmgRaftService> startCmgRaftServiceWithEvents(Set<String> nodeNames, @Nullable String initialClusterConfig) {
        BeforeStartRaftGroupEventParameters params = new BeforeStartRaftGroupEventParameters(nodeNames, initialClusterConfig);
        return fireEvent(ClusterManagerGroupEvent.BEFORE_START_RAFT_GROUP, params)
                .thenCompose(v -> startCmgRaftService(nodeNames));
    }

    /**
     * Starts the CMG Raft service using the provided node names as its peers.
     */
    private CompletableFuture<CmgRaftService> startCmgRaftService(Set<String> nodeNames) {
        String thisNodeConsistentId = clusterService.topologyService().localMember().name();

        // If we are not in the CMG, we must be a learner. List of learners will be updated by a leader accordingly,
        // but just to start a RAFT service we must include ourselves in the initial learners list, that's why we
        // pass Set.of(we) as learners list if we are not in the CMG.
        boolean isLearner = !nodeNames.contains(thisNodeConsistentId);

        Set<String> learnerNames = isLearner ? Set.of(thisNodeConsistentId) : Set.of();

        PeersAndLearners raftConfiguration = PeersAndLearners.fromConsistentIds(nodeNames, learnerNames);

        Peer serverPeer = isLearner ? raftConfiguration.learner(thisNodeConsistentId) : raftConfiguration.peer(thisNodeConsistentId);

        assert serverPeer != null;

        try {
            return raftManager
                    .startRaftGroupNodeAndWaitNodeReadyFuture(
                            new RaftNodeId(CmgGroupId.INSTANCE, serverPeer),
                            raftConfiguration,
                            new CmgRaftGroupListener(clusterStateStorage, logicalTopology, this::onLogicalTopologyChanged),
                            this::onElectedAsLeader
                    )
                    .thenApply(service -> new CmgRaftService(service, clusterService, logicalTopology));
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    private void onLogicalTopologyChanged(long term) {
        // We don't do it under lock to avoid deadlocks during node restart.

        CompletableFuture<CmgRaftService> serviceFuture = raftService;

        // If the future is not here yet, this means we are still starting, so learners will be updated after start
        // (if we happen to become a leader).

        if (serviceFuture != null) {
            serviceFuture.thenCompose(service -> service.isCurrentNodeLeader().thenCompose(isLeader -> {
                if (!isLeader) {
                    return nullCompletedFuture();
                }

                return service.updateLearners(term);
            }));
        }
    }

    /**
     * Starts the CMG Raft service using the given {@code state} and persists it to the local storage.
     */
    private CompletableFuture<CmgRaftService> initCmgRaftService(ClusterState state) {
        return startCmgRaftServiceWithEvents(state.cmgNodes(), state.initialClusterConfiguration())
                .thenCompose(service -> {
                    var localState = new LocalState(state.cmgNodes(), state.clusterTag());

                    localStateStorage.saveLocalState(localState);

                    return joinCluster(service, state.clusterTag());
                });
    }

    private TopologyEventHandler cmgLeaderTopologyEventHandler(CmgRaftService raftService) {
        return new TopologyEventHandler() {
            @Override
            public void onAppeared(ClusterNode member) {
                raftService.isCurrentNodeLeader()
                        .thenAccept(isLeader -> {
                            if (isLeader) {
                                sendClusterState(raftService, member);
                            }
                        });
            }

            @Override
            public void onDisappeared(ClusterNode member) {
                raftService.removeFromCluster(Set.of(member));
            }
        };
    }

    private void sendClusterState(CmgRaftService raftService, ClusterNode node) {
        sendClusterState(raftService, List.of(node));
    }

    private void sendClusterState(CmgRaftService raftService, Collection<ClusterNode> nodes) {
        raftService.logicalTopology()
                .thenCompose(topology -> {
                    // Only send the ClusterStateMessage to nodes not already present in the Logical Topology.
                    Collection<ClusterNode> recipients = nodes.stream()
                            .filter(node -> !topology.nodes().contains(node))
                            .collect(toList());

                    if (recipients.isEmpty()) {
                        return nullCompletedFuture();
                    }

                    return raftService.readClusterState()
                            .thenAccept(state -> {
                                // Raft state might not have been initialized in case of leader failure during cluster init
                                // TODO: properly handle this case, see https://issues.apache.org/jira/browse/IGNITE-16819
                                if (state == null) {
                                    throw new IllegalStateException("Cluster state is empty");
                                }

                                NetworkMessage msg = msgFactory.clusterStateMessage()
                                        .clusterState(state)
                                        .build();

                                for (ClusterNode node : recipients) {
                                    sendWithRetry(node, msg);
                                }
                            });
                })
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Unable to send cluster state", e);
                    }
                });
    }

    private CompletableFuture<Void> sendWithRetry(ClusterNode node, NetworkMessage msg) {
        var result = new CompletableFuture<Void>();

        sendWithRetry(node, msg, result, 5);

        return result.whenComplete((v, e) -> {
            if (e != null) {
                LOG.warn("Unable to send message [msg={}, target={}]", e, msg.getClass(), node);
            }
        });
    }

    private void sendWithRetry(ClusterNode node, NetworkMessage msg, CompletableFuture<Void> result, int attempts) {
        clusterService.messagingService().invoke(node, msg, configuration.networkInvokeTimeout().value())
                .whenComplete((response, e) -> {
                    if (e == null) {
                        result.complete(null);
                    } else if (attempts == 1) {
                        result.completeExceptionally(e);
                    } else {
                        LOG.debug("Unable to send message, going to retry [targetNode={}]", e, node.name());

                        scheduledExecutor.schedule(() -> sendWithRetry(node, msg, result, attempts - 1), 500, TimeUnit.MILLISECONDS);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        CompletableFuture<CmgRaftService> serviceFuture = raftService;
        if (serviceFuture != null) {
            cancelOrConsume(serviceFuture, CmgRaftService::close);
        }

        IgniteUtils.shutdownAndAwaitTermination(scheduledExecutor, 10, TimeUnit.SECONDS);

        try {
            raftManager.stopRaftNodes(CmgGroupId.INSTANCE);
        } catch (NodeStoppingException e) {
            return failedFuture(e);
        }

        // Fail the futures to unblock dependent operations
        joinFuture.completeExceptionally(new NodeStoppingException());

        initialClusterConfigurationFuture.completeExceptionally(new NodeStoppingException());

        return fireEvent(ClusterManagerGroupEvent.AFTER_STOP_RAFT_GROUP, EmptyEventParameters.INSTANCE);
    }

    /**
     * Returns a future that resolves after the join request finishes either successfully or with an error.
     *
     * @return Future that represents the state of the join.
     */
    public CompletableFuture<Void> joinFuture() {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return joinFuture;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns a future that, when complete, resolves into a list of node names that host the Meta Storage.
     *
     * @return Future that, when complete, resolves into a list of node names that host the Meta Storage.
     */
    public CompletableFuture<Set<String>> metaStorageNodes() {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return raftServiceAfterJoin()
                    .thenCompose(CmgRaftService::readClusterState)
                    .thenApply(ClusterState::metaStorageNodes);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns a future that, when complete, resolves into a list of node names of the majority of the voting peers in the CMG including a
     * leader.
     *
     * @return Future that, when complete, resolves into a list of node names of the majority of the voting peers in the CMG including a
     *         leader.
     */
    public CompletableFuture<Set<String>> majority() {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return raftServiceAfterJoin()
                    .thenCompose(CmgRaftService::majority);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns a future that, when complete, resolves into a logical topology snapshot.
     *
     * @return Future that, when complete, resolves into a logical topology snapshot.
     */
    public CompletableFuture<LogicalTopologySnapshot> logicalTopology() {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return raftServiceAfterJoin().thenCompose(CmgRaftService::logicalTopology);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns a future that, when complete, resolves into a list of validated nodes. This list includes all nodes currently present in the
     * Logical Topology as well as nodes that only have passed the validation step.
     */
    public CompletableFuture<Set<ClusterNode>> validatedNodes() {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return raftServiceAfterJoin().thenCompose(CmgRaftService::validatedNodes);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Issues the {@link JoinReadyCommand} thus completing the join procedure. If this method succeeds, the node will be added to the
     * logical topology.
     *
     * @return Future that represents the state of the operation.
     */
    public CompletableFuture<Void> onJoinReady() {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return raftServiceAfterJoin().thenCompose(svc -> svc.completeJoinCluster(nodeAttributes));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns a future resolving to the initial cluster configuration in HOCON format. The resulting configuration may be {@code null} if
     * not provided by the user.
     *
     * @return a CompletableFuture that, upon completion, provides the initial cluster configuration, which may be {@code null}.
     */
    public CompletableFuture<String> initialClusterConfigurationFuture() {
        return initialClusterConfigurationFuture;
    }

    /**
     * Returns a future that resolves to {@code true} if the current node is the CMG leader.
     */
    @TestOnly
    CompletableFuture<Boolean> isCmgLeader() {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return raftServiceAfterJoin().thenCompose(CmgRaftService::isCurrentNodeLeader);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<CmgRaftService> raftServiceAfterJoin() {
        // There exists a guarantee, that if "joinFuture" completes successfully, then
        // the CMG Raft service must have already been started (reference to "raftService" is not null).
        return joinFuture
                .thenCompose(v -> {
                    CompletableFuture<CmgRaftService> serviceFuture = raftService;

                    assert serviceFuture != null;

                    return serviceFuture;
                });
    }

    @TestOnly
    LogicalTopologyImpl logicalTopologyImpl() {
        return (LogicalTopologyImpl) logicalTopology;
    }
}
