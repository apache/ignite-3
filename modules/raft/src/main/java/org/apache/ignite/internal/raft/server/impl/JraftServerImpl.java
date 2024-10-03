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

package org.apache.ignite.internal.raft.server.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.thread.ThreadOperation.PROCESS_RAFT_REQ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.sources.RaftMetricSource;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.CommittedConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.IgniteJraftServiceFactory;
import org.apache.ignite.internal.raft.storage.impl.StripeAwareLogManager.Stripe;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.core.FSMCallerImpl.ApplyTask;
import org.apache.ignite.raft.jraft.core.NodeImpl.LogEntryAndClosure;
import org.apache.ignite.raft.jraft.core.ReadOnlyServiceImpl.ReadIndexEvent;
import org.apache.ignite.raft.jraft.core.StateMachineAdapter;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.ActionRequestInterceptor;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.NullActionRequestInterceptor;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestInterceptor;
import org.apache.ignite.raft.jraft.rpc.impl.core.NullAppendEntriesRequestInterceptor;
import org.apache.ignite.raft.jraft.storage.impl.LogManagerImpl.StableClosureEvent;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.ExponentialBackoffTimeoutStrategy;
import org.apache.ignite.raft.jraft.util.Utils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Raft server implementation on top of forked JRaft library.
 */
public class JraftServerImpl implements RaftServer {
    private static final IgniteLogger LOG = Loggers.forClass(JraftServerImpl.class);

    /** Cluster service. */
    private final ClusterService service;

    /** Failure processor that is used to handle critical errors. */
    private final FailureManager failureManager;

    /** Server instance. */
    private IgniteRpcServer rpcServer;

    /** Started nodes. */
    private final ConcurrentMap<RaftNodeId, RaftGroupService> nodes = new ConcurrentHashMap<>();

    /** Lock storage with predefined monitor objects,
     * needed to prevent concurrent start of the same raft group. */
    private final List<Object> startGroupInProgressMonitors;

    /** Node manager. */
    private final NodeManager nodeManager;

    /** Options. */
    private final NodeOptions opts;

    private final RaftGroupEventsClientListener raftGroupEventsClientListener;

    /** Request executor. */
    private ExecutorService requestExecutor;

    /** Raft service event interceptor. */
    private final RaftServiceEventInterceptor serviceEventInterceptor;

    /** Interceptor for AppendEntriesRequests. Not thread-safe, should be assigned and read in the same thread. */
    private AppendEntriesRequestInterceptor appendEntriesRequestInterceptor = new NullAppendEntriesRequestInterceptor();

    /** Interceptor for ActionRequests. Not thread-safe, should be assigned and read in the same thread. */
    private ActionRequestInterceptor actionRequestInterceptor = new NullActionRequestInterceptor();

    /** The number of parallel raft groups starts. */
    private static final int SIMULTANEOUS_GROUP_START_PARALLELISM = Math.min(Utils.cpus() * 3, 25);

    /**
     * The constructor.
     *
     * @param service Cluster service.
     * @param opts Default node options.
     * @param raftGroupEventsClientListener Raft events listener.
     * @param failureManager Failure processor that is used to handle critical errors.
     */
    public JraftServerImpl(
            ClusterService service,
            NodeOptions opts,
            RaftGroupEventsClientListener raftGroupEventsClientListener,
            FailureManager failureManager
    ) {
        this.service = service;
        this.nodeManager = new NodeManager();

        this.opts = opts;
        this.raftGroupEventsClientListener = raftGroupEventsClientListener;
        this.failureManager = failureManager;

        // Auto-adjust options.
        this.opts.setRpcConnectTimeoutMs(this.opts.getElectionTimeoutMs() / 3);
        this.opts.setRpcDefaultTimeout(this.opts.getElectionTimeoutMs() / 2);
        this.opts.setSharedPools(true);

        if (opts.getServerName() == null) {
            this.opts.setServerName(service.nodeName());
        }

        /*
         Timeout increasing strategy for election timeout. Adjusting happens according to
         {@link org.apache.ignite.raft.jraft.util.ExponentialBackoffTimeoutStrategy} when a leader is not elected, after several
         consecutive unsuccessful leader elections, which could be controlled through {@code roundsWithoutAdjusting} parameter of
         {@link org.apache.ignite.raft.jraft.util.ExponentialBackoffTimeoutStrategy}.
         Max timeout value that {@link org.apache.ignite.raft.jraft.util.ExponentialBackoffTimeoutStrategy} could produce
         must be more than timeout of a membership protocol to remove failed node from the cluster.
         In our case, we may assume that 11s could be enough as far as 11s is greater
         than suspicion timeout for the 1000 nodes cluster with ping interval equals 500ms.
         */
        this.opts.setElectionTimeoutStrategy(new ExponentialBackoffTimeoutStrategy(11_000, 3));

        var monitors = new ArrayList<>(SIMULTANEOUS_GROUP_START_PARALLELISM);

        for (int i = 0; i < SIMULTANEOUS_GROUP_START_PARALLELISM; i++) {
            monitors.add(new Object());
        }

        startGroupInProgressMonitors = Collections.unmodifiableList(monitors);

        serviceEventInterceptor = new RaftServiceEventInterceptor();
    }

    /**
     * Sets {@link AppendEntriesRequestInterceptor} to use. Should only be called from the same thread that is used
     * to {@link #startAsync(ComponentContext)} the component.
     *
     * @param appendEntriesRequestInterceptor Interceptor to use.
     */
    public void appendEntriesRequestInterceptor(AppendEntriesRequestInterceptor appendEntriesRequestInterceptor) {
        this.appendEntriesRequestInterceptor = appendEntriesRequestInterceptor;
    }

    /**
     * Sets {@link ActionRequestInterceptor} to use. Should only be called from the same thread that is used to
     * {@link #startAsync(ComponentContext)} the component.
     *
     * @param actionRequestInterceptor Interceptor to use.
     */
    public void actionRequestInterceptor(ActionRequestInterceptor actionRequestInterceptor) {
        this.actionRequestInterceptor = actionRequestInterceptor;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        assert opts.isSharedPools() : "RAFT server is supposed to run in shared pools mode";

        // Pre-create all pools in shared mode.
        if (opts.getCommonExecutor() == null) {
            opts.setCommonExecutor(JRaftUtils.createCommonExecutor(opts));
        }

        if (opts.getStripedExecutor() == null) {
            opts.setStripedExecutor(JRaftUtils.createAppendEntriesExecutor(opts));
        }

        if (opts.getScheduler() == null) {
            opts.setScheduler(JRaftUtils.createScheduler(opts));
        }

        if (opts.getClientExecutor() == null) {
            opts.setClientExecutor(JRaftUtils.createClientExecutor(opts, opts.getServerName()));
        }

        if (opts.getVoteTimer() == null) {
            opts.setVoteTimer(JRaftUtils.createTimer(opts, "JRaft-VoteTimer"));
        }

        if (opts.getElectionTimer() == null) {
            opts.setElectionTimer(JRaftUtils.createTimer(opts, "JRaft-ElectionTimer"));
        }

        if (opts.getStepDownTimer() == null) {
            opts.setStepDownTimer(JRaftUtils.createTimer(opts, "JRaft-StepDownTimer"));
        }

        if (opts.getSnapshotTimer() == null) {
            opts.setSnapshotTimer(JRaftUtils.createTimer(opts, "JRaft-SnapshotTimer"));
        }

        requestExecutor = Executors.newFixedThreadPool(
                opts.getRaftRpcThreadPoolSize(),
                IgniteThreadFactory.create(opts.getServerName(), "JRaft-Request-Processor", LOG, PROCESS_RAFT_REQ)
        );

        rpcServer = new IgniteRpcServer(
                service,
                nodeManager,
                opts.getRaftMessagesFactory(),
                requestExecutor,
                serviceEventInterceptor,
                raftGroupEventsClientListener,
                appendEntriesRequestInterceptor,
                actionRequestInterceptor
        );

        if (opts.getRaftMetrics() == null) {
            opts.setRaftMetrics(new RaftMetricSource(opts.getStripes(), opts.getLogStripesCount()));
        }

        if (opts.getfSMCallerExecutorDisruptor() == null) {
            opts.setfSMCallerExecutorDisruptor(new StripedDisruptor<>(
                    opts.getServerName(),
                    "JRaft-FSMCaller-Disruptor",
                    (nodeName, stripeName) -> IgniteThreadFactory.create(nodeName, stripeName, true, LOG, STORAGE_READ, STORAGE_WRITE),
                    opts.getRaftOptions().getDisruptorBufferSize(),
                    ApplyTask::new,
                    opts.getStripes(),
                    false,
                    false,
                    opts.getRaftMetrics().disruptorMetrics("raft.fsmcaller.disruptor")
            ));
        }

        if (opts.getNodeApplyDisruptor() == null) {
            opts.setNodeApplyDisruptor(new StripedDisruptor<>(
                    opts.getServerName(),
                    "JRaft-NodeImpl-Disruptor",
                    opts.getRaftOptions().getDisruptorBufferSize(),
                    LogEntryAndClosure::new,
                    opts.getStripes(),
                    false,
                    false,
                    opts.getRaftMetrics().disruptorMetrics("raft.nodeimpl.disruptor")
            ));
        }

        if (opts.getReadOnlyServiceDisruptor() == null) {
            opts.setReadOnlyServiceDisruptor(new StripedDisruptor<>(
                    opts.getServerName(),
                    "JRaft-ReadOnlyService-Disruptor",
                    opts.getRaftOptions().getDisruptorBufferSize(),
                    ReadIndexEvent::new,
                    opts.getStripes(),
                    false,
                    false,
                    opts.getRaftMetrics().disruptorMetrics("raft.readonlyservice.disruptor")
            ));
        }

        if (opts.getLogManagerDisruptor() == null) {
            opts.setLogManagerDisruptor(new StripedDisruptor<>(
                    opts.getServerName(),
                    "JRaft-LogManager-Disruptor",
                    opts.getRaftOptions().getDisruptorBufferSize(),
                    StableClosureEvent::new,
                    opts.getLogStripesCount(),
                    true,
                    opts.isLogYieldStrategy(),
                    opts.getRaftMetrics().disruptorMetrics("raft.logmanager.disruptor")
            ));

            opts.setLogStripes(IntStream.range(0, opts.getLogStripesCount()).mapToObj(i -> new Stripe()).collect(toList()));
        }

        rpcServer.init(null);

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        assert nodes.isEmpty() : IgniteStringFormatter.format("Raft nodes {} are still running on the Ignite node {}", nodes.keySet(),
                service.topologyService().localMember().name());

        rpcServer.shutdown();

        if (opts.getfSMCallerExecutorDisruptor() != null) {
            opts.getfSMCallerExecutorDisruptor().shutdown();
        }

        if (opts.getNodeApplyDisruptor() != null) {
            opts.getNodeApplyDisruptor().shutdown();
        }

        if (opts.getReadOnlyServiceDisruptor() != null) {
            opts.getReadOnlyServiceDisruptor().shutdown();
        }

        if (opts.getLogManagerDisruptor() != null) {
            opts.getLogManagerDisruptor().shutdown();
        }

        if (opts.getCommonExecutor() != null) {
            ExecutorServiceHelper.shutdownAndAwaitTermination(opts.getCommonExecutor());
        }

        if (opts.getStripedExecutor() != null) {
            opts.getStripedExecutor().shutdownGracefully();
        }

        if (opts.getScheduler() != null) {
            opts.getScheduler().shutdown();
        }

        if (opts.getElectionTimer() != null) {
            opts.getElectionTimer().stop();
        }

        if (opts.getVoteTimer() != null) {
            opts.getVoteTimer().stop();
        }

        if (opts.getStepDownTimer() != null) {
            opts.getStepDownTimer().stop();
        }

        if (opts.getSnapshotTimer() != null) {
            opts.getSnapshotTimer().stop();
        }

        if (opts.getClientExecutor() != null) {
            ExecutorServiceHelper.shutdownAndAwaitTermination(opts.getClientExecutor());
        }

        IgniteUtils.shutdownAndAwaitTermination(requestExecutor, 10, SECONDS);

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public ClusterService clusterService() {
        return service;
    }

    public static Path getServerDataPath(Path basePath, RaftNodeId nodeId) {
        return basePath.resolve(nodeIdStr(nodeId));
    }

    private static String nodeIdStr(RaftNodeId nodeId) {
        return String.join(
                "_",
                nodeId.groupId().toString(),
                nodeId.peer().consistentId(),
                String.valueOf(nodeId.peer().idx())
        );
    }

    @Override
    public boolean startRaftNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupListener lsnr,
            RaftGroupOptions groupOptions
    ) {
        return startRaftNode(nodeId, configuration, RaftGroupEventsListener.noopLsnr, lsnr, groupOptions);
    }

    @Override
    public boolean startRaftNode(
            RaftNodeId nodeId,
            PeersAndLearners configuration,
            RaftGroupEventsListener evLsnr,
            RaftGroupListener lsnr,
            RaftGroupOptions groupOptions
    ) {
        assert nodeId.peer().consistentId().equals(service.topologyService().localMember().name());

        // fast track to check if node with the same ID is already created.
        if (nodes.containsKey(nodeId)) {
            return false;
        }

        synchronized (startNodeMonitor(nodeId)) {
            // double check if node wasn't created before receiving the lock.
            if (nodes.containsKey(nodeId)) {
                return false;
            }

            // Thread pools are shared by all raft groups.
            NodeOptions nodeOptions = opts.copy();

            nodeOptions.setLogUri(nodeIdStr(nodeId));

            Path serverDataPath = serverDataPathForNodeId(nodeId, groupOptions);

            if (!groupOptions.volatileStores()) {
                try {
                    Files.createDirectories(serverDataPath);
                } catch (IOException e) {
                    throw new IgniteInternalException(e);
                }
            }

            nodeOptions.setRaftMetaUri(serverDataPath.resolve("meta").toString());

            nodeOptions.setSnapshotUri(serverDataPath.resolve("snapshot").toString());

            if (groupOptions.commandsMarshaller() != null) {
                nodeOptions.setCommandsMarshaller(groupOptions.commandsMarshaller());
            }

            nodeOptions.setFsm(new DelegatingStateMachine(lsnr, nodeOptions.getCommandsMarshaller(), failureManager));

            nodeOptions.setRaftGrpEvtsLsnr(new RaftGroupEventsListenerAdapter(nodeId.groupId(), serviceEventInterceptor, evLsnr));

            LogStorageFactory logStorageFactory = groupOptions.getLogStorageFactory();

            assert logStorageFactory != null : "LogStorageFactory was not set.";

            IgniteJraftServiceFactory serviceFactory = new IgniteJraftServiceFactory(logStorageFactory);

            if (groupOptions.snapshotStorageFactory() != null) {
                serviceFactory.setSnapshotStorageFactory(groupOptions.snapshotStorageFactory());
            }

            if (groupOptions.raftMetaStorageFactory() != null) {
                serviceFactory.setRaftMetaStorageFactory(groupOptions.raftMetaStorageFactory());
            }

            nodeOptions.setServiceFactory(serviceFactory);

            List<PeerId> peerIds = configuration.peers().stream().map(PeerId::fromPeer).collect(toList());

            List<PeerId> learnerIds = configuration.learners().stream().map(PeerId::fromPeer).collect(toList());

            nodeOptions.setInitialConf(new Configuration(peerIds, learnerIds));

            IgniteRpcClient client = new IgniteRpcClient(service);

            nodeOptions.setRpcClient(client);

            nodeOptions.setExternallyEnforcedConfigIndex(groupOptions.externallyEnforcedConfigIndex());

            var server = new RaftGroupService(
                    nodeId.groupId().toString(),
                    PeerId.fromPeer(nodeId.peer()),
                    nodeOptions,
                    rpcServer,
                    nodeManager,
                    groupOptions.ownFsmCallerExecutorDisruptorConfig()
            );

            server.start();

            nodes.put(nodeId, server);

            return true;
        }
    }

    private static Path serverDataPathForNodeId(RaftNodeId nodeId, RaftGroupOptions groupOptions) {
        Path dataPath = groupOptions.serverDataPath();

        assert dataPath != null : "Raft metadata path was not set, nodeId is " + nodeId;

        return getServerDataPath(dataPath, nodeId);
    }

    @Override
    public boolean isStarted(RaftNodeId nodeId) {
        return nodes.containsKey(nodeId);
    }

    @Override
    public boolean stopRaftNode(RaftNodeId nodeId) {
        RaftGroupService svc = nodes.remove(nodeId);

        boolean stopped = svc != null;

        if (stopped) {
            svc.shutdown();
        }

        return stopped;
    }

    @Override
    public boolean stopRaftNodes(ReplicationGroupId groupId) {
        return nodes.entrySet().removeIf(e -> {
            RaftNodeId nodeId = e.getKey();
            RaftGroupService service = e.getValue();

            if (nodeId.groupId().equals(groupId)) {
                service.shutdown();

                return true;
            } else {
                return false;
            }
        });
    }

    @Override
    public void destroyRaftNodeStorages(RaftNodeId nodeId, RaftGroupOptions groupOptions) {
        // TODO: IGNITE-23079 - improve on what we do if it was not possible to destroy any of the storages.
        try {
            String uri = nodeIdStr(nodeId);
            groupOptions.getLogStorageFactory().destroyLogStorage(uri);
        } finally {
            Path serverDataPath = serverDataPathForNodeId(nodeId, groupOptions);

            // This destroys both meta storage and snapshots storage as they are stored under serverDataPath.
            IgniteUtils.deleteIfExists(serverDataPath);
        }
    }

    @Override
    public @Nullable IndexWithTerm raftNodeIndex(RaftNodeId nodeId) {
        RaftGroupService service = nodes.get(nodeId);

        if (service == null) {
            return null;
        }

        LogId logId = service.getRaftNode().lastLogIndexAndTerm();

        return new IndexWithTerm(logId.getIndex(), logId.getTerm());
    }

    /**
     * Performs a {@code resetPeers} operation on raft node.
     *
     * @param raftNodeId Raft node ID.
     * @param peersAndLearners New node configuration.
     */
    public void resetPeers(RaftNodeId raftNodeId, PeersAndLearners peersAndLearners) {
        RaftGroupService raftGroupService = nodes.get(raftNodeId);

        List<PeerId> peerIds = peersAndLearners.peers().stream().map(PeerId::fromPeer).collect(toList());

        List<PeerId> learnerIds = peersAndLearners.learners().stream().map(PeerId::fromPeer).collect(toList());

        raftGroupService.getRaftNode().resetPeers(new Configuration(peerIds, learnerIds));
    }

    /**
     * Iterates over all currently started raft services. Doesn't block the starting or stopping of other services, so consumer may
     * accidentally receive stopped service.
     *
     * @param consumer Closure to process each service.
     */
    public void forEach(BiConsumer<RaftNodeId, RaftGroupService> consumer) {
        nodes.forEach(consumer);
    }

    /** {@inheritDoc} */
    @Override
    public List<Peer> localPeers(ReplicationGroupId groupId) {
        return nodes.keySet().stream()
                .filter(nodeId -> nodeId.groupId().equals(groupId))
                .map(RaftNodeId::peer)
                .collect(toList());
    }

    /**
     * Returns service group.
     *
     * @param nodeId Node ID.
     * @return Service group.
     */
    public RaftGroupService raftGroupService(RaftNodeId nodeId) {
        return nodes.get(nodeId);
    }

    /** {@inheritDoc} */
    @Override
    public Set<RaftNodeId> localNodes() {
        return nodes.keySet();
    }

    /** {@inheritDoc} */
    @Override
    public NodeOptions options() {
        return opts;
    }

    /**
     * Blocks messages for raft group node according to provided predicate.
     *
     * @param nodeId Raft node ID.
     * @param predicate Predicate to block messages.
     */
    @TestOnly
    public void blockMessages(RaftNodeId nodeId, BiPredicate<Object, String> predicate) {
        IgniteRpcClient client = (IgniteRpcClient) nodes.get(nodeId).getNodeOptions().getRpcClient();

        client.blockMessages(predicate);
    }

    /**
     * Return currently blocked messages queue.
     *
     * @param nodeId Node id.
     * @return Blocked messages.
     */
    @TestOnly
    public Queue<Object[]> blockedMessages(RaftNodeId nodeId) {
        IgniteRpcClient client = (IgniteRpcClient) nodes.get(nodeId).getNodeOptions().getRpcClient();

        return client.blockedMessages();
    }

    /**
     * Stops blocking messages for raft group node.
     *
     * @param nodeId Raft node ID.
     */
    @TestOnly
    public void stopBlockMessages(RaftNodeId nodeId) {
        IgniteRpcClient client = (IgniteRpcClient) nodes.get(nodeId).getNodeOptions().getRpcClient();

        client.stopBlock();
    }

    /**
     * Returns the monitor object, which can be used to synchronize start operation by node ID.
     *
     * @param nodeId Node ID.
     * @return Monitor object.
     */
    private Object startNodeMonitor(RaftNodeId nodeId) {
        return startGroupInProgressMonitors.get(Math.abs(nodeId.hashCode() % SIMULTANEOUS_GROUP_START_PARALLELISM));
    }

    /**
     * Wrapper of {@link StateMachineAdapter}.
     */
    public static class DelegatingStateMachine extends StateMachineAdapter {
        private final RaftGroupListener listener;

        private final Marshaller marshaller;

        private final FailureManager failureManager;

        /**
         * Constructor.
         *
         * @param listener The listener.
         * @param marshaller Marshaller.
         * @param failureManager Failure processor that is used to handle critical errors.
         */
        public DelegatingStateMachine(RaftGroupListener listener, Marshaller marshaller, FailureManager failureManager) {
            this.listener = listener;
            this.marshaller = marshaller;
            this.failureManager = failureManager;
        }

        public RaftGroupListener getListener() {
            return listener;
        }

        /** {@inheritDoc} */
        @Override
        public void onApply(Iterator iter) {
            try {
                listener.onWrite(new java.util.Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public CommandClosure<WriteCommand> next() {
                        @Nullable CommandClosure<WriteCommand> done = (CommandClosure<WriteCommand>) iter.done();
                        ByteBuffer data = iter.getData();

                        WriteCommand command = done == null ? marshaller.unmarshall(data) : done.command();

                        long commandIndex = iter.getIndex();
                        long commandTerm = iter.getTerm();

                        return new CommandClosure<>() {
                            /** {@inheritDoc} */
                            @Override
                            public long index() {
                                return commandIndex;
                            }

                            /** {@inheritDoc} */
                            @Override
                            public long term() {
                                return commandTerm;
                            }

                            /** {@inheritDoc} */
                            @Override
                            public WriteCommand command() {
                                return command;
                            }

                            /** {@inheritDoc} */
                            @Override
                            public void result(Serializable res) {
                                if (done != null) {
                                    done.result(res);
                                }

                                iter.next();
                            }
                        };
                    }
                });
            } catch (Throwable err) {
                Status st;

                if (err.getMessage() != null) {
                    st = new Status(RaftError.ESTATEMACHINE, err.getMessage());
                } else {
                    st = new Status(RaftError.ESTATEMACHINE, "Unknown state machine error.");
                }

                if (iter.done() != null) {
                    iter.done().run(st);
                }

                iter.setErrorAndRollback(1, st);

                failureManager.process(new FailureContext(FailureType.CRITICAL_ERROR, err));
            }
        }

        @Override
        public void onRawConfigurationCommitted(ConfigurationEntry entry) {
            boolean hasOldConf = entry.getOldConf() != null && entry.getOldConf().getPeers() != null;

            CommittedConfiguration committedConf = new CommittedConfiguration(
                    entry.getId().getIndex(),
                    entry.getId().getTerm(),
                    peersIdsToStrings(entry.getConf().getPeers()),
                    peersIdsToStrings(entry.getConf().getLearners()),
                    hasOldConf ? peersIdsToStrings(entry.getOldConf().getPeers()) : null,
                    hasOldConf ? peersIdsToStrings(entry.getOldConf().getLearners()) : null
            );

            listener.onConfigurationCommitted(committedConf);
        }

        private static List<String> peersIdsToStrings(Collection<PeerId> peerIds) {
            return peerIds.stream().map(PeerId::toString).collect(toUnmodifiableList());
        }

        /** {@inheritDoc} */
        @Override
        public void onSnapshotSave(SnapshotWriter writer, Closure done) {
            try {
                listener.onSnapshotSave(Path.of(writer.getPath()), res -> {
                    if (res == null) {
                        File file = new File(writer.getPath());

                        File[] snapshotFiles = file.listFiles();

                        // Files array can be null if shanpshot folder doesn't exist.
                        if (snapshotFiles != null) {
                            for (File file0 : snapshotFiles) {
                                if (file0.isFile()) {
                                    writer.addFile(file0.getName(), null);
                                }
                            }
                        }

                        done.run(Status.OK());
                    } else {
                        done.run(new Status(RaftError.EIO, "Fail to save snapshot to %s, reason %s",
                                writer.getPath(), res.getMessage()));
                    }
                });
            } catch (Throwable e) {
                done.run(new Status(RaftError.EIO, "Fail to save snapshot %s", e.getMessage()));

                failureManager.process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
        }

        /** {@inheritDoc} */
        @Override
        public boolean onSnapshotLoad(SnapshotReader reader) {
            try {
                return listener.onSnapshotLoad(Path.of(reader.getPath()));
            } catch (Throwable err) {
                failureManager.process(new FailureContext(FailureType.CRITICAL_ERROR, err));

                return false;
            }
        }

        /** {@inheritDoc} */
        @Override
        public void onShutdown() {
            listener.onShutdown();
        }

        @Override
        public void onLeaderStart(long term) {
            super.onLeaderStart(term);

            listener.onLeaderStart();
        }
    }
}
