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
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.failOrConsume;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Rest.CLUSTER_NOT_INIT_ERR;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.LocalStateStorage.LocalState;
import org.apache.ignite.internal.cluster.management.events.BeforeStartRaftGroupEventParameters;
import org.apache.ignite.internal.cluster.management.events.ClusterManagerGroupEvent;
import org.apache.ignite.internal.cluster.management.events.EmptyEventParameters;
import org.apache.ignite.internal.cluster.management.metrics.ClusterTopologyMetricsSource;
import org.apache.ignite.internal.cluster.management.metrics.LocalTopologyMetricsSource;
import org.apache.ignite.internal.cluster.management.network.CmgMessageCallback;
import org.apache.ignite.internal.cluster.management.network.CmgMessageHandler;
import org.apache.ignite.internal.cluster.management.network.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.ClusterStateMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.CmgPrepareInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.InitErrorMessage;
import org.apache.ignite.internal.cluster.management.network.messages.RefuseJoinMessage;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorageManager;
import org.apache.ignite.internal.cluster.management.raft.CmgRaftGroupListener;
import org.apache.ignite.internal.cluster.management.raft.CmgRaftService;
import org.apache.ignite.internal.cluster.management.raft.IllegalInitArgumentException;
import org.apache.ignite.internal.cluster.management.raft.JoinDeniedException;
import org.apache.ignite.internal.cluster.management.raft.ValidationManager;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.storage.ClusterResetStorage;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.event.EventParameters;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.IgniteException;
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

    private static final int NETWORK_INVOKE_TIMEOUT_MS = 3000;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Future that resolves into a CMG Raft service. Can be {@code null} if the Raft service has not been started. */
    @Nullable
    private volatile CompletableFuture<CmgRaftService> raftService;

    /** Lock for the {@code raftService} field. */
    private final Object raftServiceLock = new Object();

    /** Current updateLearners operation to ensure linearization of updates. */
    private CompletableFuture<Void> currentUpdateLearners = nullCompletedFuture();

    /** Lock for linearizing updateLearners operations. */
    private final Object updateLearnersLock = new Object();

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

    private final ClusterStateStorageManager clusterStateStorageMgr;

    private final LogicalTopology logicalTopology;

    private final ValidationManager validationManager;

    /** Local state. */
    private final LocalStateStorage localStateStorage;

    /** Handles cluster initialization flow. */
    private final ClusterInitializer clusterInitializer;

    /** Local node's attributes. */
    private final NodeAttributes nodeAttributes;

    /** Failure processor that is used to handle critical errors. */
    private final FailureProcessor failureProcessor;

    private final ClusterIdStore clusterIdStore;

    private final ClusterResetStorage clusterResetStorage;

    /** Future that resolves into the initial cluster configuration in HOCON format. */
    private final CompletableFuture<String> initialClusterConfigurationFuture = new CompletableFuture<>();

    private final CmgMessageHandler cmgMessageHandler;

    private final RaftGroupOptionsConfigurer raftGroupOptionsConfigurer;

    private final MetricManager metricsManager;

    private final ClusterTopologyMetricsSource clusterTopologyMetricsSource;

    private final LocalTopologyMetricsSource localTopologyMetricsSource;

    private final Consumer<RaftGroupConfiguration> onConfigurationCommittedListener;

    /** Constructor. */
    public ClusterManagementGroupManager(
            VaultManager vault,
            ClusterResetStorage clusterResetStorage,
            ClusterService clusterService,
            ClusterInitializer clusterInitializer,
            RaftManager raftManager,
            ClusterStateStorageManager clusterStateStorageMgr,
            LogicalTopology logicalTopology,
            ValidationManager validationManager,
            NodeAttributes nodeAttributes,
            FailureProcessor failureProcessor,
            ClusterIdStore clusterIdStore,
            RaftGroupOptionsConfigurer raftGroupOptionsConfigurer,
            MetricManager metricManager
    ) {
        this(
                vault,
                clusterResetStorage,
                clusterService,
                clusterInitializer,
                raftManager,
                clusterStateStorageMgr,
                logicalTopology,
                validationManager,
                nodeAttributes,
                failureProcessor,
                clusterIdStore,
                raftGroupOptionsConfigurer,
                metricManager,
                config -> {}
        );
    }

    /** Constructor. */
    public ClusterManagementGroupManager(
            VaultManager vault,
            ClusterResetStorage clusterResetStorage,
            ClusterService clusterService,
            ClusterInitializer clusterInitializer,
            RaftManager raftManager,
            ClusterStateStorageManager clusterStateStorageMgr,
            LogicalTopology logicalTopology,
            ValidationManager validationManager,
            NodeAttributes nodeAttributes,
            FailureProcessor failureProcessor,
            ClusterIdStore clusterIdStore,
            RaftGroupOptionsConfigurer raftGroupOptionsConfigurer,
            MetricManager metricManager,
            Consumer<RaftGroupConfiguration> onConfigurationCommittedListener
    ) {
        this.clusterResetStorage = clusterResetStorage;
        this.clusterService = clusterService;
        this.clusterInitializer = clusterInitializer;
        this.raftManager = raftManager;
        this.clusterStateStorageMgr = clusterStateStorageMgr;
        this.logicalTopology = logicalTopology;
        this.validationManager = validationManager;
        this.localStateStorage = new LocalStateStorage(vault);
        this.nodeAttributes = nodeAttributes;
        this.failureProcessor = failureProcessor;
        this.clusterIdStore = clusterIdStore;
        this.raftGroupOptionsConfigurer = raftGroupOptionsConfigurer;
        this.metricsManager = metricManager;

        this.clusterTopologyMetricsSource = new ClusterTopologyMetricsSource(logicalTopology, () -> {
            LocalState localState = localStateStorage.getLocalState();

            if (localState == null) {
                return null;
            }

            return localState.clusterTag();
        });
        this.localTopologyMetricsSource = new LocalTopologyMetricsSource(clusterService.topologyService());

        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                IgniteThreadFactory.create(clusterService.nodeName(), "cmg-manager", LOG)
        );

        cmgMessageHandler = createMessageHandler();

        clusterService.messagingService().addMessageHandler(CmgMessageGroup.class, message -> scheduledExecutor, cmgMessageHandler);

        this.onConfigurationCommittedListener = onConfigurationCommittedListener;
    }

    private CmgMessageHandler createMessageHandler() {
        var messageCallback = new CmgMessageCallback() {
            @Override
            public void onClusterStateMessageReceived(
                    ClusterStateMessage message,
                    InternalClusterNode sender,
                    @Nullable Long correlationId
            ) {
                assert correlationId != null : sender;

                handleClusterState(message, sender, correlationId);
            }

            @Override
            public void onCancelInitMessageReceived(CancelInitMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
                handleCancelInit(message);
            }

            @Override
            public void onRefuseJoinMessageReceived(RefuseJoinMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
                handleRefuseJoin(message);
            }

            @Override
            public void onCmgInitMessageReceived(CmgInitMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
                assert correlationId != null : sender;

                handleInit(message, sender, correlationId);
            }

            @Override
            public void onCmgPrepareInitMessageReceived(
                    CmgPrepareInitMessage message,
                    InternalClusterNode sender,
                    @Nullable Long correlationId
            ) {
                assert correlationId != null : sender;

                handlePrepareInit(message, sender, correlationId);
            }
        };

        return new CmgMessageHandler(busyLock, msgFactory, clusterService, failureProcessor, messageCallback);
    }

    /** Constructor. */
    @TestOnly
    public ClusterManagementGroupManager(
            VaultManager vault,
            ClusterResetStorage clusterResetStorage,
            ClusterService clusterService,
            ClusterInitializer clusterInitializer,
            RaftManager raftManager,
            ClusterStateStorage clusterStateStorage,
            LogicalTopology logicalTopology,
            NodeAttributes nodeAttributes,
            FailureProcessor failureProcessor,
            ClusterIdStore clusterIdStore,
            RaftGroupOptionsConfigurer raftGroupOptionsConfigurer,
            MetricManager metricManager
    ) {
        this(
                vault,
                clusterResetStorage,
                clusterService,
                clusterInitializer,
                raftManager,
                new ClusterStateStorageManager(clusterStateStorage),
                logicalTopology,
                new ValidationManager(new ClusterStateStorageManager(clusterStateStorage), logicalTopology),
                nodeAttributes,
                failureProcessor,
                clusterIdStore,
                raftGroupOptionsConfigurer,
                metricManager
        );
    }

    /** Constructor. */
    @TestOnly
    public ClusterManagementGroupManager(
            VaultManager vault,
            ClusterResetStorage clusterResetStorage,
            ClusterService clusterService,
            ClusterInitializer clusterInitializer,
            RaftManager raftManager,
            ClusterStateStorage clusterStateStorage,
            LogicalTopology logicalTopology,
            NodeAttributes nodeAttributes,
            FailureProcessor failureProcessor,
            ClusterIdStore clusterIdStore,
            RaftGroupOptionsConfigurer raftGroupOptionsConfigurer,
            MetricManager metricManager,
            Consumer<RaftGroupConfiguration> onConfigurationCommittedListener
    ) {
        this(
                vault,
                clusterResetStorage,
                clusterService,
                clusterInitializer,
                raftManager,
                new ClusterStateStorageManager(clusterStateStorage),
                logicalTopology,
                new ValidationManager(new ClusterStateStorageManager(clusterStateStorage), logicalTopology),
                nodeAttributes,
                failureProcessor,
                clusterIdStore,
                raftGroupOptionsConfigurer,
                metricManager,
                onConfigurationCommittedListener
        );
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @param cmgNodeNames Names of nodes that will host the Cluster Management Group.
     * @param clusterName Human-readable name of the cluster.
     */
    public void initCluster(Collection<String> metaStorageNodeNames, Collection<String> cmgNodeNames, String clusterName)
            throws NodeStoppingException {
        sync(initClusterAsync(metaStorageNodeNames, cmgNodeNames, clusterName));
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
        sync(initClusterAsync(metaStorageNodeNames, cmgNodeNames, clusterName, clusterConfiguration));
    }

    private static void sync(CompletableFuture<Void> future) {
        try {
            future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(unwrapCause(e));
        }
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @param cmgNodeNames Names of nodes that will host the Cluster Management Group.
     * @param clusterName Human-readable name of the cluster.
     * @return Future which completes when cluster is initialized.
     */
    public CompletableFuture<Void> initClusterAsync(
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName
    ) throws NodeStoppingException {
        return initClusterAsync(metaStorageNodeNames, cmgNodeNames, clusterName, null);
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @param cmgNodeNames Names of nodes that will host the Cluster Management Group.
     * @param clusterName Human-readable name of the cluster.
     * @param clusterConfiguration Cluster configuration.
     * @return Future which completes when cluster is initialized.
     */
    public CompletableFuture<Void> initClusterAsync(
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName,
            @Nullable String clusterConfiguration
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return clusterInitializer.initCluster(metaStorageNodeNames, cmgNodeNames, clusterName, clusterConfiguration)
                    .handle((res, e) -> {
                        if (e == null) {
                            return res;
                        }
                        if (e instanceof InterruptedException) {
                            throw new InitException("Interrupted while initializing the cluster", e);
                        }
                        throw new InitException("Unable to initialize the cluster: " + e.getMessage(), e);
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        metricsManager.registerSource(clusterTopologyMetricsSource);
        metricsManager.registerSource(localTopologyMetricsSource);

        metricsManager.enable(clusterTopologyMetricsSource);
        metricsManager.enable(localTopologyMetricsSource);

        ResetClusterMessage resetClusterMessage = clusterResetStorage.readResetClusterMessage();
        if (resetClusterMessage != null) {
            return doClusterReset(resetClusterMessage);
        }

        synchronized (raftServiceLock) {
            raftService = recoverLocalState();
        }

        cmgMessageHandler.onRecoveryComplete();

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> doClusterReset(ResetClusterMessage resetClusterMessage) {
        LOG.info("Found a ResetClusterMessage in storage, going to do cluster reset [message={}]", resetClusterMessage);

        return destroyCmgWithEvents()
                .thenCompose(unused -> {
                    if (resetClusterMessage.newCmgNodes().contains(clusterService.nodeName())) {
                        return doReinit(resetClusterMessage);
                    } else {
                        // Let's just wait for new CMG nodes to establish a majority and send us an invitation to join.
                        cmgMessageHandler.onRecoveryComplete();
                        return nullCompletedFuture();
                    }
                })
                .thenRun(clusterResetStorage::removeResetClusterMessage)
                .thenRun(() -> clusterResetStorage.saveVolatileResetClusterMessage(resetClusterMessage));
    }

    private CompletableFuture<CmgRaftService> doReinit(ResetClusterMessage resetClusterMessage) {
        CompletableFuture<CmgRaftService> serviceFuture;

        synchronized (raftServiceLock) {
            // Disaster recovery means that the Repair Conductor has ensured the cluster was initialized,
            // so we can just pass null as initialClusterConfig.
            serviceFuture = startCmgRaftServiceWithEvents(resetClusterMessage.newCmgNodes(), null);
            raftService = serviceFuture;
        }

        cmgMessageHandler.onRecoveryComplete();

        return serviceFuture
                .thenCompose(
                        service -> doInit(
                                service,
                                cmgInitMessageFromResetClusterMessage(resetClusterMessage),
                                resetClusterMessage.formerClusterIds()
                        )
                );
    }

    private CmgInitMessage cmgInitMessageFromResetClusterMessage(ResetClusterMessage resetClusterMessage) {
        return msgFactory.cmgInitMessage()
                .cmgNodes(resetClusterMessage.newCmgNodes())
                .metaStorageNodes(resetClusterMessage.currentMetaStorageNodes())
                .clusterName(resetClusterMessage.clusterName())
                .clusterId(resetClusterMessage.clusterId())
                .initialClusterConfiguration(resetClusterMessage.initialClusterConfiguration())
                .build();
    }

    /**
     * Returns the cluster state future or the future that will be resolved to {@code null} if the cluster is not initialized yet or the
     * local CMG raft service stops (for example, due to unsuccessful completion of cluster initialization).
     */
    public CompletableFuture<@Nullable ClusterState> clusterState() {
        CompletableFuture<CmgRaftService> serviceFuture = raftService;

        return serviceFuture == null ? nullCompletedFuture() : serviceFuture.thenCompose(CmgRaftService::readClusterState);
    }

    /**
     * Renames the cluster with the provided name.
     *
     * @param newName the new name for the cluster.
     * @return Completable future that will be completed when cluster is initialized.
     */
    public CompletableFuture<Void> renameCluster(String newName) {
        CompletableFuture<CmgRaftService> serviceFuture = raftService;

        return serviceFuture == null
                ? failedFuture(new IgniteException(CLUSTER_NOT_INIT_ERR,
                "Cluster has not yet been initialized or the node is in the process of being stopped."))
                : serviceFuture.thenCompose(cmgRaftService -> cmgRaftService.changeClusterName(newName));
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
            LOG.info("No local CMG state exists, going to wait for the cluster state or the init command");

            return null;
        }

        LOG.info("Local CMG state recovered, starting the CMG");

        // Since we recovered state we do not supply a new initialClusterConfig.
        return startCmgRaftServiceWithEvents(localState.cmgNodeNames(), null)
                .thenCompose(service -> validateAgainstCluster(service, localState.clusterTag()));
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
    private void handleInit(CmgInitMessage msg, InternalClusterNode sender, long correlationId) {
        synchronized (raftServiceLock) {
            CompletableFuture<CmgRaftService> serviceFuture = raftService;

            if (serviceFuture == null) {
                // Raft service has not been started
                LOG.info("Init command received, starting the CMG [nodes={}]", msg.cmgNodes());

                serviceFuture = startCmgRaftServiceWithEvents(msg.cmgNodes(), msg.initialClusterConfiguration())
                        .whenComplete((v, e) -> inBusyLock(() -> {
                            if (e != null) {
                                Throwable finalEx = unwrapCause(e);

                                LOG.error("Unable to start CMG Raft service", finalEx);

                                NetworkMessage response = initErrorMessage(finalEx);

                                clusterService.messagingService().respond(sender, response, correlationId);
                            } else {
                                LOG.info("CMG Raft service started successfully.");
                            }
                        }));
            } else {
                // Raft service has been started, which means that this node has already received an init command at least once.
                LOG.info("Init command received, but the CMG has already been started");
            }

            // Every node, that receives the init command, tries to initialize the CMG state. Raft listener will correctly
            // handle this case by applying only the first attempt and returning the actual cluster state for all other
            // attempts.
            raftService = serviceFuture
                    .thenCompose(service -> inBusyLockAsync(() -> doInit(service, msg, null))
                            .handle((v, e) -> inBusyLock(() -> {
                                NetworkMessage response;

                                if (e == null) {
                                    LOG.info("CMG initialized successfully");

                                    response = msgFactory.initCompleteMessage().build();
                                } else {
                                    Throwable finalEx = unwrapCause(e);

                                    LOG.warn("Error when initializing the CMG", finalEx);

                                    response = initErrorMessage(finalEx);
                                }

                                clusterService.messagingService().respond(sender, response, correlationId);

                                return service;
                            })))
                    .whenComplete((cmgRaftService, e) -> {
                        if (e != null) {
                            LOG.warn("Error when handling the CMG Init", e);
                        }
                    });
        }
    }

    /**
     * Handles the prepare init messages.
     *
     * <p>If both initiator node and recipient have the same colocation mode, a PrepareInitCompleteMessage is sent,
     * otherwise an InitErrorMessage is sent.
     */
    private void handlePrepareInit(CmgPrepareInitMessage msg, InternalClusterNode sender, long correlationId) {
        LOG.info("CmgPrepareInitMessage message received [sender={}, colocationEnabled={}]", sender.name(),
                msg.initInitiatorColocationEnabled());

        NetworkMessage response;
        if (!msg.initInitiatorColocationEnabled()) {
            String colocationEnabledMismatchResponseMessage = IgniteStringFormatter.format(
                    "Colocation modes do not match [initInitiatorNodeName={}, initInitiatorColocationMode={}, "
                            + "recipientColocationMode={}].",
                    sender.name(),
                    msg.initInitiatorColocationEnabled(),
                    Boolean.TRUE
            );

            response = preparePhaseInitErrorMessage(colocationEnabledMismatchResponseMessage);
        } else {
            response = msgFactory.prepareInitCompleteMessage().build();
        }

        clusterService.messagingService().respond(sender, response, correlationId);
    }

    private CompletableFuture<CmgRaftService> doInit(CmgRaftService service, CmgInitMessage msg, @Nullable List<UUID> formerClusterIds) {
        return service.initClusterState(createClusterState(msg, formerClusterIds))
                .thenCompose(state -> {
                    var localState = new LocalState(state.cmgNodes(), state.clusterTag());

                    localStateStorage.saveLocalState(localState);

                    return validateAgainstCluster(service, state.clusterTag());
                });
    }

    private ClusterState createClusterState(CmgInitMessage msg, @Nullable List<UUID> formerClusterIds) {
        return msgFactory.clusterState()
                .cmgNodes(Set.copyOf(msg.cmgNodes()))
                .metaStorageNodes(Set.copyOf(msg.metaStorageNodes()))
                .version(IgniteProductVersion.CURRENT_VERSION.toString())
                .clusterTag(clusterTag(msgFactory, msg.clusterName(), msg.clusterId()))
                .initialClusterConfiguration(msg.initialClusterConfiguration())
                .formerClusterIds(formerClusterIds)
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
            raftServiceAfterJoin().thenAccept(service -> inBusyLock(() -> {
                service.readClusterState()
                        .thenAccept(state -> initialClusterConfigurationFuture.complete(state.initialClusterConfiguration()));

                updateLogicalTopology(service)
                        .thenCompose(v -> inBusyLock(() -> updateLearnersSerially(service, term, false)))
                        .thenAccept(v -> inBusyLock(() -> {
                            // Register a listener to send ClusterState messages to new nodes.
                            TopologyService topologyService = clusterService.topologyService();

                            // TODO: remove listeners if leadership is lost, see https://issues.apache.org/jira/browse/IGNITE-16842
                            topologyService.addEventHandler(cmgLeaderTopologyEventHandler(service));

                            // Send the ClusterStateMessage to all members of the physical topology. We do not wait for the send operation
                            // because being unable to send ClusterState messages should not fail the CMG service startup.
                            InternalClusterNode thisNode = topologyService.localMember();

                            Collection<InternalClusterNode> otherNodes = topologyService.allMembers().stream()
                                    .filter(node -> !thisNode.equals(node))
                                    .collect(toList());

                            sendClusterState(service, otherNodes);
                        }))
                        .whenComplete((v, e) -> {
                            if (e != null) {
                                if (hasCause(e, NodeStoppingException.class)) {
                                    LOG.info("Unable to execute onLeaderElected callback, because the node is stopping", e);
                                } else {
                                    failureProcessor.process(new FailureContext(
                                            e,
                                            "Error when executing onLeaderElected callback"
                                    ));
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

    private InitErrorMessage initErrorMessage(Throwable finalEx) {
        return msgFactory.initErrorMessage()
                .cause(finalEx.getMessage())
                .shouldCancel(!(finalEx instanceof IllegalInitArgumentException))
                .build();
    }

    private InitErrorMessage preparePhaseInitErrorMessage(String responseMessage) {
        return msgFactory.initErrorMessage()
                .cause(responseMessage)
                .shouldCancel(false)
                .build();
    }

    /**
     * This method must be executed upon CMG leader election in order to regain logical topology consistency in case some nodes left the
     * physical topology during the election. Newly appeared nodes will be added automatically after the new leader broadcasts the current
     * cluster state.
     */
    private CompletableFuture<Void> updateLogicalTopology(CmgRaftService service) {
        return service.logicalTopology()
                .thenCompose(logicalTopology -> inBusyLock(() -> {
                    Set<UUID> physicalTopologyIds = clusterService.topologyService().allMembers()
                            .stream()
                            .map(InternalClusterNode::id)
                            .collect(toSet());

                    Set<InternalClusterNode> nodesToRemove = logicalTopology.nodes().stream()
                            .filter(node -> !physicalTopologyIds.contains(node.id()))
                            .collect(toUnmodifiableSet());

                    return nodesToRemove.isEmpty() ? nullCompletedFuture() : service.removeFromCluster(nodesToRemove);
                }));
    }

    private void handleCancelInit(CancelInitMessage msg) {
        LOG.info("CMG initialization cancelled [reason={}]", msg.reason());
        this.scheduledExecutor.execute(this::destroyCmgWithEvents);
    }

    private void handleRefuseJoin(RefuseJoinMessage msg) {
        LOG.info("Join refused [reason={}]", msg.reason());
        joinFuture.completeExceptionally(new InitException(msg.reason()));
    }

    /** Delegates call to {@link #destroyCmg()} but fires the associated events. */
    private CompletableFuture<Void> destroyCmgWithEvents() {
        LOG.info("CMG destruction procedure started");
        return inBusyLockAsync(
                () -> fireEvent(ClusterManagerGroupEvent.BEFORE_DESTROY_RAFT_GROUP, EmptyEventParameters.INSTANCE)
                        .thenRunAsync(this::destroyCmg, this.scheduledExecutor)
                        .whenComplete((v, e) -> {
                            if (e != null) {
                                failureProcessor.process(new FailureContext(CRITICAL_ERROR, e));
                            }
                        })
        );
    }

    /**
     * Completely destroys the local CMG Raft service.
     */
    private void destroyCmg() {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            synchronized (raftServiceLock) {
                if (raftService != null) {
                    raftService.cancel(true);

                    raftService = null;
                }

                raftManager.stopRaftNodes(CmgGroupId.INSTANCE);

                RaftNodeId nodeId = raftNodeId(new Peer(clusterService.nodeName()));
                raftManager.destroyRaftNodeStorages(nodeId, raftGroupOptionsConfigurer);

                localStateStorage.clear();
            }
        } catch (NodeStoppingException e) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, "Error when cleaning the CMG state", e);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Handler for the {@link ClusterStateMessage}.
     */
    private void handleClusterState(ClusterStateMessage msg, InternalClusterNode sender, long correlationId) {
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
                        .handle((service, e) -> inBusyLockAsync(() -> {
                            if (service != null && service.nodeNames().equals(state.cmgNodes())) {
                                LOG.info("ClusterStateMessage received, but the CMG service is already started");

                                return completedFuture(service);
                            }

                            if (service == null) {
                                // Service could not be started for some reason, which might be due to starting on incorrect CMG nodes
                                assert e != null;

                                Throwable finalEx = e instanceof CompletionException ? e.getCause() : e;

                                // Nothing can be done if the node has not passed validation.
                                if (finalEx instanceof JoinDeniedException) {
                                    return failedFuture(finalEx);
                                }

                                LOG.warn("CMG service could not be started on previous attempts. "
                                        + "Re-creating the CMG Raft service [reason={}]", finalEx, finalEx.getMessage());

                                return initCmgRaftService(state);
                            } else {
                                LOG.warn("CMG service started, but the cluster state is different. "
                                                + "Re-creating the CMG Raft service [localState={}, clusterState={}]",
                                        service.nodeNames(), state.cmgNodes());

                                return destroyCmgWithEvents()
                                        .thenCompose(none -> initCmgRaftService(state));
                            }
                        }))
                        .thenCompose(Function.identity());
            }
        }
    }

    private CompletableFuture<CmgRaftService> validateAgainstCluster(CmgRaftService service, ClusterTag clusterTag) {
        return service.startJoinCluster(clusterTag, nodeAttributes)
                .thenApply(v -> service)
                .whenComplete((v, e) -> {
                    if (e == null) {
                        LOG.info("Successfully validated against the cluster [name={}]", clusterTag.clusterName());

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
                .thenApplyAsync(v -> startCmgRaftService(nodeNames), scheduledExecutor)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.warn("Error when initializing the CMG", e);
                    }
                });
    }

    /**
     * Starts the CMG Raft service using the provided node names as its peers.
     */
    private CmgRaftService startCmgRaftService(Set<String> nodeNames) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            String thisNodeConsistentId = clusterService.nodeName();

            // If we are not in the CMG, we must be a learner. List of learners will be updated by a leader accordingly,
            // but just to start a RAFT service we must include ourselves in the initial learners list, that's why we
            // pass Set.of(we) as learners list if we are not in the CMG.
            boolean isLearner = !nodeNames.contains(thisNodeConsistentId);

            Set<String> learnerNames = isLearner ? Set.of(thisNodeConsistentId) : Set.of();

            PeersAndLearners raftConfiguration = PeersAndLearners.fromConsistentIds(nodeNames, learnerNames);

            Peer serverPeer = isLearner ? raftConfiguration.learner(thisNodeConsistentId) : raftConfiguration.peer(thisNodeConsistentId);

            assert serverPeer != null;

            LOG.info("Starting CMG Raft service [isLearner={}, nodeNames={}, serverPeer={}]", isLearner, nodeNames, serverPeer);

            RaftGroupService service = raftManager.startSystemRaftGroupNodeAndWaitNodeReady(
                    raftNodeId(serverPeer),
                    raftConfiguration,
                    new CmgRaftGroupListener(
                            clusterStateStorageMgr,
                            logicalTopology,
                            validationManager,
                            this::onLogicalTopologyChanged,
                            clusterIdStore,
                            failureProcessor,
                            onConfigurationCommittedListener
                    ),
                    (term, configurationTerm, configurationIndex, configuration, sequenceToken) -> {
                        onElectedAsLeader(term);
                    },
                    null,
                    raftGroupOptionsConfigurer
            );

            return new CmgRaftService(service, clusterService.topologyService(), logicalTopology);
        } catch (NodeStoppingException e) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, e);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static RaftNodeId raftNodeId(Peer serverPeer) {
        return new RaftNodeId(CmgGroupId.INSTANCE, serverPeer);
    }

    /**
     * Serializes calls to updateLearners to prevent race conditions between multiple topology changes
     * and leader election callbacks.
     *
     * @param service CMG Raft service.
     * @param term RAFT term.
     * @param checkLeadership Whether to check leadership before updating learners.
     */
    private CompletableFuture<Void> updateLearnersSerially(CmgRaftService service, long term, boolean checkLeadership) {
        synchronized (updateLearnersLock) {
            currentUpdateLearners = currentUpdateLearners
                    .thenCompose(v -> {
                        if (checkLeadership) {
                            return service.isCurrentNodeLeader().thenCompose(isLeader -> {
                                if (!isLeader) {
                                    return nullCompletedFuture();
                                }
                                return service.updateLearners(term);
                            });
                        } else {
                            return service.updateLearners(term);
                        }
                    })
                    .exceptionally(e -> {
                        LOG.warn("Failed to update learners for term {}", e, term);

                        return null;
                    });
            return currentUpdateLearners;
        }
    }

    private void onLogicalTopologyChanged(long term) {
        // We don't do it under lock to avoid deadlocks during node restart.
        CompletableFuture<CmgRaftService> serviceFuture = raftService;

        // If the future is not here yet, this means we are still starting, so learners will be updated after start
        // (if we happen to become a leader).
        if (serviceFuture == null) {
            return;
        }

        serviceFuture.thenCompose(service -> updateLearnersSerially(service, term, true));
    }

    /**
     * Starts the CMG Raft service using the given {@code state} and persists it to the local storage.
     */
    private CompletableFuture<CmgRaftService> initCmgRaftService(ClusterState state) {
        return startCmgRaftServiceWithEvents(state.cmgNodes(), state.initialClusterConfiguration())
                .thenCompose(service -> inBusyLockAsync(() -> {
                    var localState = new LocalState(state.cmgNodes(), state.clusterTag());

                    localStateStorage.saveLocalState(localState);

                    return validateAgainstCluster(service, state.clusterTag());
                }));
    }

    private TopologyEventHandler cmgLeaderTopologyEventHandler(CmgRaftService raftService) {
        return new TopologyEventHandler() {
            @Override
            public void onAppeared(InternalClusterNode member) {
                raftService.isCurrentNodeLeader()
                        .thenAccept(isLeader -> {
                            if (isLeader) {
                                sendClusterState(raftService, member);
                            }
                        });
            }

            @Override
            public void onDisappeared(InternalClusterNode member) {
                raftService.removeFromCluster(Set.of(member));
            }
        };
    }

    private void sendClusterState(CmgRaftService raftService, InternalClusterNode node) {
        sendClusterState(raftService, List.of(node));
    }

    private void sendClusterState(CmgRaftService raftService, Collection<InternalClusterNode> nodes) {
        raftService.logicalTopology()
                .thenCompose(topology -> {
                    // TODO https://issues.apache.org/jira/browse/IGNITE-24769
                    Set<InternalClusterNode> logicalTopology = topology.nodes().stream()
                            .map(node -> new ClusterNodeImpl(node.id(), node.name(), node.address(), node.nodeMetadata()))
                            .collect(toSet());
                    // Only send the ClusterStateMessage to nodes not already present in the Logical Topology.
                    Set<InternalClusterNode> recipients = nodes.stream()
                            .filter(node -> !logicalTopology.contains(node))
                            .collect(toSet());

                    if (recipients.isEmpty()) {
                        return nullCompletedFuture();
                    }

                    Set<InternalClusterNode> duplicates = findDuplicateConsistentIdsOfExistingNodes(logicalTopology, recipients);
                    for (InternalClusterNode duplicate : duplicates) {
                        RefuseJoinMessage msg = msgFactory.refuseJoinMessage()
                                .reason("Duplicate node name \"" + duplicate.name() + "\"")
                                .build();
                        sendWithRetry(duplicate, msg);
                        recipients.remove(duplicate);
                    }

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

                                ClusterStateMessage msg = msgFactory.clusterStateMessage()
                                        .clusterState(state)
                                        .build();

                                for (InternalClusterNode node : recipients) {
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

    private static Set<InternalClusterNode> findDuplicateConsistentIdsOfExistingNodes(
            Set<InternalClusterNode> existingTopology,
            Collection<InternalClusterNode> candidatesForAddition
    ) {
        Set<String> existingConsistentIds = existingTopology.stream()
                .map(InternalClusterNode::name)
                .collect(toSet());

        return candidatesForAddition.stream()
                .filter(node -> existingConsistentIds.contains(node.name()))
                .collect(toSet());
    }

    private CompletableFuture<Void> sendWithRetry(InternalClusterNode node, NetworkMessage msg) {
        var result = new CompletableFuture<Void>();

        sendWithRetry(node, msg, result, 5);

        return result.whenComplete((v, e) -> {
            if (e != null) {
                LOG.warn("Unable to send message [msg={}, target={}]", e, msg.getClass(), node);
            }
        });
    }

    private void sendWithRetry(InternalClusterNode node, NetworkMessage msg, CompletableFuture<Void> result, int attempts) {
        clusterService.messagingService().invoke(node, msg, NETWORK_INVOKE_TIMEOUT_MS)
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
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        CompletableFuture<CmgRaftService> serviceFuture = raftService;
        if (serviceFuture != null) {
            failOrConsume(serviceFuture, new NodeStoppingException(), CmgRaftService::close);
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
        return metaStorageInfo()
                .thenApply(MetaStorageInfo::metaStorageNodes);
    }

    /**
     * Returns a future that, when complete, resolves into a Meta storage info.
     */
    public CompletableFuture<MetaStorageInfo> metaStorageInfo() {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return raftServiceAfterJoin()
                    .thenCompose(CmgRaftService::readMetaStorageInfo);
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
     * Returns a future that, when complete, resolves into a list of learner node names in the CMG.
     */
    @TestOnly
    public CompletableFuture<Set<String>> learnerNodes() {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return raftServiceAfterJoin()
                    .thenCompose(CmgRaftService::learners);
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
    public CompletableFuture<Set<InternalClusterNode>> validatedNodes() {
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
     * Changes metastorage nodes in the CMG for the graceful Metastorage reconfiguration procedure.
     *
     * @param newMetastorageNodes Metastorage node names to set.
     * @return Future that completes when the command is executed by the CMG.
     */
    public CompletableFuture<Void> changeMetastorageNodes(Set<String> newMetastorageNodes) {
        return changeMetastorageNodesInternal(newMetastorageNodes, null);
    }

    /**
     * Changes metastorage nodes in the CMG for the forceful (with repair) Metastorage reconfiguration procedure.
     *
     * @param newMetastorageNodes Metastorage node names to set.
     * @param metastorageRepairingConfigIndex Raft index in the Metastorage group under which the forced configuration is
     *     (or will be) saved.
     * @return Future that completes when the command is executed by the CMG.
     */
    public CompletableFuture<Void> changeMetastorageNodes(Set<String> newMetastorageNodes, long metastorageRepairingConfigIndex) {
        return changeMetastorageNodesInternal(newMetastorageNodes, metastorageRepairingConfigIndex);
    }

    private CompletableFuture<Void> changeMetastorageNodesInternal(
            Set<String> newMetastorageNodes,
            @Nullable Long metastorageRepairingConfigIndex
    ) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return raftServiceAfterJoin()
                    .thenCompose(service -> service.changeMetastorageNodes(newMetastorageNodes, metastorageRepairingConfigIndex));
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

    private void inBusyLock(Runnable action) {
        IgniteUtils.inBusyLock(busyLock, action);
    }

    private <T> T inBusyLock(Supplier<T> action) {
        return IgniteUtils.inBusyLock(busyLock, action);
    }

    private <T> CompletableFuture<T> inBusyLockAsync(Supplier<CompletableFuture<T>> action) {
        return IgniteUtils.inBusyLockAsync(busyLock, action);
    }

    @TestOnly
    LogicalTopologyImpl logicalTopologyImpl() {
        return (LogicalTopologyImpl) logicalTopology;
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-26085 Remove, tmp hack
    /**
     * Mark component as stopping.
     */
    public void markAsStopping() {
        var raftService0 = raftService;

        if (raftService0 == null) {
            return;
        }

        raftService0.thenAccept(CmgRaftService::markAsStopping);
    }
}
