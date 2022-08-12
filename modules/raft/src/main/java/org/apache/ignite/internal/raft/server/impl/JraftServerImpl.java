/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft.server.impl;

import static org.apache.ignite.raft.jraft.JRaftUtils.addressFromEndpoint;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.raft.server.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.IgniteJraftServiceFactory;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.ElectionPriority;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.core.FSMCallerImpl;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.ReadOnlyServiceImpl;
import org.apache.ignite.raft.jraft.core.StateMachineAdapter;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.storage.impl.LogManagerImpl;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.ExponentialBackoffTimeoutStrategy;
import org.apache.ignite.raft.jraft.util.JDKMarshaller;
import org.apache.ignite.raft.jraft.util.Utils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Raft server implementation on top of forked JRaft library.
 */
public class JraftServerImpl implements RaftServer {
    /** Cluster service. */
    private final ClusterService service;

    /** Data path. */
    private final Path dataPath;

    /** Log storage provider. */
    private final LogStorageFactory logStorageFactory;

    /** Server instance. */
    private IgniteRpcServer rpcServer;

    /** Started groups. */
    private ConcurrentMap<String, RaftGroupService> groups = new ConcurrentHashMap<>();

    /** Lock storage with predefined monitor objects,
     * needed to prevent concurrent start of the same raft group. */
    private final List<Object> startGroupInProgressMonitors;

    /** Node manager. */
    private final NodeManager nodeManager;

    /** Options. */
    private final NodeOptions opts;

    /** Request executor. */
    private ExecutorService requestExecutor;

    /** The number of parallel raft groups starts. */
    private static final int SIMULTANEOUS_GROUP_START_PARALLELISM = Math.min(Utils.cpus() * 3, 25);

    /**
     * The constructor.
     *
     * @param service  Cluster service.
     * @param dataPath Data path.
     */
    public JraftServerImpl(ClusterService service, Path dataPath) {
        this(service, dataPath, new NodeOptions());
    }

    /**
     * The constructor.
     *
     * @param service  Cluster service.
     * @param dataPath Data path.
     * @param opts     Default node options.
     */
    public JraftServerImpl(ClusterService service, Path dataPath, NodeOptions opts) {
        this.service = service;
        this.dataPath = dataPath;
        this.nodeManager = new NodeManager();
        this.logStorageFactory = new DefaultLogStorageFactory(dataPath.resolve("log"));
        this.opts = opts;

        // Auto-adjust options.
        this.opts.setRpcConnectTimeoutMs(this.opts.getElectionTimeoutMs() / 3);
        this.opts.setRpcDefaultTimeout(this.opts.getElectionTimeoutMs() / 2);
        this.opts.setSharedPools(true);

        if (opts.getServerName() == null) {
            this.opts.setServerName(service.localConfiguration().getName());
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
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
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

        requestExecutor = JRaftUtils.createRequestExecutor(opts);

        rpcServer = new IgniteRpcServer(
                service,
                nodeManager,
                opts.getRaftMessagesFactory(),
                requestExecutor
        );

        if (opts.getfSMCallerExecutorDisruptor() == null) {
            opts.setfSMCallerExecutorDisruptor(new StripedDisruptor<>(
                    NamedThreadFactory.threadPrefix(opts.getServerName(), "JRaft-FSMCaller-Disruptor"),
                    opts.getRaftOptions().getDisruptorBufferSize(),
                    () -> new FSMCallerImpl.ApplyTask(),
                    opts.getStripes()));
        }

        if (opts.getNodeApplyDisruptor() == null) {
            opts.setNodeApplyDisruptor(new StripedDisruptor<>(
                    NamedThreadFactory.threadPrefix(opts.getServerName(), "JRaft-NodeImpl-Disruptor"),
                    opts.getRaftOptions().getDisruptorBufferSize(),
                    () -> new NodeImpl.LogEntryAndClosure(),
                    opts.getStripes()));
        }

        if (opts.getReadOnlyServiceDisruptor() == null) {
            opts.setReadOnlyServiceDisruptor(new StripedDisruptor<>(
                    NamedThreadFactory.threadPrefix(opts.getServerName(), "JRaft-ReadOnlyService-Disruptor"),
                    opts.getRaftOptions().getDisruptorBufferSize(),
                    () -> new ReadOnlyServiceImpl.ReadIndexEvent(),
                    opts.getStripes()));
        }

        if (opts.getLogManagerDisruptor() == null) {
            opts.setLogManagerDisruptor(new StripedDisruptor<LogManagerImpl.StableClosureEvent>(
                    NamedThreadFactory.threadPrefix(opts.getServerName(), "JRaft-LogManager-Disruptor"),
                    opts.getRaftOptions().getDisruptorBufferSize(),
                    () -> new LogManagerImpl.StableClosureEvent(),
                    opts.getStripes()));
        }

        logStorageFactory.start();

        rpcServer.init(null);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        assert groups.isEmpty() : IgniteStringFormatter.format("Raft groups {} are still running on the node {}", groups.keySet(),
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

        ExecutorServiceHelper.shutdownAndAwaitTermination(requestExecutor);

        logStorageFactory.close();
    }

    /** {@inheritDoc} */
    @Override
    public ClusterService clusterService() {
        return service;
    }

    /**
     * Returns path to persistence folder.
     *
     * @param groupId Group id.
     * @return The path to persistence folder.
     */
    public Path getServerDataPath(String groupId) {
        ClusterNode clusterNode = service.topologyService().localMember();

        String dirName = groupId + "_" + clusterNode.address().toString().replace(':', '_');

        return this.dataPath.resolve(dirName);
    }

    /** {@inheritDoc} */
    @Override
    public boolean startRaftGroup(
            String groupId,
            RaftGroupListener lsnr,
            @Nullable List<Peer> initialConf,
            RaftGroupOptions groupOptions
    ) {
        return startRaftGroup(groupId, RaftGroupEventsListener.noopLsnr, lsnr, initialConf, groupOptions);
    }

    /** {@inheritDoc} */
    @Override
    public boolean startRaftGroup(
            String grpId,
            RaftGroupEventsListener evLsnr,
            RaftGroupListener lsnr,
            @Nullable List<Peer> initialConf,
            RaftGroupOptions groupOptions
    ) {
        // fast track to check if group with the same name is already created.
        if (groups.containsKey(grpId)) {
            return false;
        }

        synchronized (groupMonitor(grpId)) {
            // double check if group wasn't created before receiving the lock.
            if (groups.containsKey(grpId)) {
                return false;
            }

            // Thread pools are shared by all raft groups.
            NodeOptions nodeOptions = opts.copy();

            // TODO: IGNITE-17083 - Do not create paths for volatile stores at all when we get rid of snapshot storage on FS.
            Path serverDataPath = getServerDataPath(grpId);

            try {
                Files.createDirectories(serverDataPath);
            } catch (IOException e) {
                throw new IgniteInternalException(e);
            }

            nodeOptions.setLogUri(grpId);

            nodeOptions.setRaftMetaUri(serverDataPath.resolve("meta").toString());

            nodeOptions.setSnapshotUri(serverDataPath.resolve("snapshot").toString());

            nodeOptions.setFsm(new DelegatingStateMachine(lsnr));

            nodeOptions.setRaftGrpEvtsLsnr(evLsnr);

            LogStorageFactory logStorageFactory = groupOptions.getLogStorageFactory() == null
                    ? this.logStorageFactory : groupOptions.getLogStorageFactory();

            IgniteJraftServiceFactory serviceFactory = new IgniteJraftServiceFactory(logStorageFactory);

            if (groupOptions.snapshotStorageFactory() != null) {
                serviceFactory.setSnapshotStorageFactory(groupOptions.snapshotStorageFactory());
            }

            if (groupOptions.raftMetaStorageFactory() != null) {
                serviceFactory.setRaftMetaStorageFactory(groupOptions.raftMetaStorageFactory());
            }

            nodeOptions.setServiceFactory(serviceFactory);

            if (initialConf != null) {
                List<PeerId> mapped = initialConf.stream().map(PeerId::fromPeer).collect(Collectors.toList());

                nodeOptions.setInitialConf(new Configuration(mapped, null));
            }

            IgniteRpcClient client = new IgniteRpcClient(service);

            nodeOptions.setRpcClient(client);

            NetworkAddress addr = service.topologyService().localMember().address();

            var peerId = new PeerId(addr.host(), addr.port(), 0, ElectionPriority.DISABLED);

            var server = new RaftGroupService(grpId, peerId, nodeOptions, rpcServer, nodeManager);

            server.start();

            groups.put(grpId, server);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean stopRaftGroup(String grpId) {
        RaftGroupService svc = groups.remove(grpId);

        boolean stopped = svc != null;

        if (stopped) {
            svc.shutdown();
        }

        return stopped;
    }

    /** {@inheritDoc} */
    @Override
    public Peer localPeer(String groupId) {
        RaftGroupService service = groups.get(groupId);

        if (service == null) {
            return null;
        }

        PeerId peerId = service.getRaftNode().getNodeId().getPeerId();

        return new Peer(addressFromEndpoint(peerId.getEndpoint()), peerId.getPriority());
    }

    /**
     * Returns service group.
     *
     * @param groupId Group id.
     * @return Service group.
     */
    public RaftGroupService raftGroupService(String groupId) {
        return groups.get(groupId);
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> startedGroups() {
        return groups.keySet();
    }

    /**
     * Blocks messages for raft group node according to provided predicate.
     *
     * @param groupId Raft group id.
     * @param predicate Predicate to block messages.
     */
    @TestOnly
    public void blockMessages(String groupId, BiPredicate<Object, String> predicate) {
        IgniteRpcClient client = (IgniteRpcClient) groups.get(groupId).getNodeOptions().getRpcClient();

        client.blockMessages(predicate);
    }

    /**
     * Stops blocking messages for raft group node.
     *
     * @param groupId Raft group id.
     */
    @TestOnly
    public void stopBlockMessages(String groupId) {
        IgniteRpcClient client = (IgniteRpcClient) groups.get(groupId).getNodeOptions().getRpcClient();

        client.stopBlock();
    }

    /**
     * Returns the monitor object, which can be used to synchronize start operation by group id.
     *
     * @param grpId Group id.
     * @return Monitor object.
     */
    private Object groupMonitor(String grpId) {
        return startGroupInProgressMonitors.get(Math.abs(grpId.hashCode() % SIMULTANEOUS_GROUP_START_PARALLELISM));
    }

    /**
     * Wrapper of {@link StateMachineAdapter}.
     */
    public static class DelegatingStateMachine extends StateMachineAdapter {
        private final RaftGroupListener listener;

        /**
         * Constructor.
         *
         * @param listener The listener.
         */
        DelegatingStateMachine(RaftGroupListener listener) {
            this.listener = listener;
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

                        WriteCommand command = done == null ? JDKMarshaller.DEFAULT.unmarshall(data.array()) : done.command();

                        long commandIndex = iter.getIndex();

                        return new CommandClosure<>() {
                            /** {@inheritDoc} */
                            @Override
                            public long index() {
                                return commandIndex;
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
            } catch (Exception err) {
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
            }
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
            } catch (Exception e) {
                done.run(new Status(RaftError.EIO, "Fail to save snapshot %s", e.getMessage()));
            }
        }

        /** {@inheritDoc} */
        @Override
        public boolean onSnapshotLoad(SnapshotReader reader) {
            return listener.onSnapshotLoad(Path.of(reader.getPath()));
        }

        /** {@inheritDoc} */
        @Override
        public void onShutdown() {
            listener.onShutdown();
        }
    }
}
