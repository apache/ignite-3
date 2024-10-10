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
package org.apache.ignite.raft.jraft.core;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.ArrayUtils.EMPTY_BYTE_BUFFER;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.sources.RaftMetricSource;
import org.apache.ignite.internal.raft.JraftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftNodeDisruptorConfiguration;
import org.apache.ignite.internal.raft.storage.impl.RocksDbSharedLogStorage;
import org.apache.ignite.internal.raft.storage.impl.StripeAwareLogManager;
import org.apache.ignite.internal.raft.storage.impl.StripeAwareLogManager.Stripe;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.FSMCaller;
import org.apache.ignite.raft.jraft.JRaftServiceFactory;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.ReadOnlyService;
import org.apache.ignite.raft.jraft.ReplicatorGroup;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.CatchUpClosure;
import org.apache.ignite.raft.jraft.closure.ClosureQueue;
import org.apache.ignite.raft.jraft.closure.ClosureQueueImpl;
import org.apache.ignite.raft.jraft.closure.ReadIndexClosure;
import org.apache.ignite.raft.jraft.closure.SynchronizedClosure;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.disruptor.DisruptorEventType;
import org.apache.ignite.raft.jraft.disruptor.NodeIdAware;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.Ballot;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LeaderChangeContext;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.entity.Task;
import org.apache.ignite.raft.jraft.entity.UserLog;
import org.apache.ignite.raft.jraft.error.LogIndexOutOfBoundsException;
import org.apache.ignite.raft.jraft.error.LogNotFoundException;
import org.apache.ignite.raft.jraft.error.OverloadException;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.option.BallotBoxOptions;
import org.apache.ignite.raft.jraft.option.BootstrapOptions;
import org.apache.ignite.raft.jraft.option.FSMCallerOptions;
import org.apache.ignite.raft.jraft.option.LogManagerOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftMetaStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.ReadOnlyOption;
import org.apache.ignite.raft.jraft.option.ReadOnlyServiceOptions;
import org.apache.ignite.raft.jraft.option.ReplicatorGroupOptions;
import org.apache.ignite.raft.jraft.option.SnapshotExecutorOptions;
import org.apache.ignite.raft.jraft.rpc.AppendEntriesResponseBuilder;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.ReadIndexResponseBuilder;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.RequestVoteRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.RequestVoteResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.TimeoutNowRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.TimeoutNowResponse;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosure;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosureAdapter;
import org.apache.ignite.raft.jraft.rpc.impl.core.DefaultRaftClientService;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.RaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.SnapshotExecutor;
import org.apache.ignite.raft.jraft.storage.impl.LogManagerImpl;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotExecutorImpl;
import org.apache.ignite.raft.jraft.util.Describer;
import org.apache.ignite.raft.jraft.util.DisruptorMetricSet;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.OnlyForTest;
import org.apache.ignite.raft.jraft.util.RepeatedTimer;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.apache.ignite.raft.jraft.util.SystemPropertyUtil;
import org.apache.ignite.raft.jraft.util.ThreadId;
import org.apache.ignite.raft.jraft.util.TimeoutStrategy;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.jraft.util.concurrent.LongHeldDetectingReadWriteLock;
import org.jetbrains.annotations.Nullable;

/**
 * The raft replica node implementation.
 */
public class NodeImpl implements Node, RaftServerService {
    private static final IgniteLogger LOG = Loggers.forClass(NodeImpl.class);

    public static final Status LEADER_STEPPED_DOWN = new Status(RaftError.EPERM, "Leader stepped down.");

    private volatile HybridClock clock;

    /**
     * Internal states
     */
    private final ReadWriteLock readWriteLock = new NodeReadWriteLock(
        this);
    protected final Lock writeLock = this.readWriteLock
        .writeLock();
    protected final Lock readLock = this.readWriteLock
        .readLock();
    private volatile State state;
    private volatile CountDownLatch shutdownLatch;
    private long currTerm;
    private volatile long lastLeaderTimestamp;
    private PeerId leaderId = new PeerId();
    private PeerId votedId;
    private final Ballot voteCtx = new Ballot();
    private final Ballot prevVoteCtx = new Ballot();
    private ConfigurationEntry conf;
    private StopTransferArg stopTransferArg;
    private boolean electionAdjusted;
    private long electionRound;
    private int initialElectionTimeout;

    /**
     * Raft group and node options and identifier
     */
    private final String groupId;
    private NodeOptions options;
    private RaftOptions raftOptions;
    private final PeerId serverId;

    /**
     * Other services
     */
    private final ConfigurationCtx confCtx;
    private LogStorage logStorage;
    private RaftMetaStorage metaStorage;
    private ClosureQueue closureQueue;
    private ConfigurationManager configManager;
    private LogManager logManager;
    private FSMCaller fsmCaller;
    private BallotBox ballotBox;
    private SnapshotExecutor snapshotExecutor;
    private ReplicatorGroup replicatorGroup;
    private RaftClientService rpcClientService;
    private ReadOnlyService readOnlyService;

    /**
     * Timers
     */
    private RepeatedTimer electionTimer;
    private RepeatedTimer voteTimer;
    private RepeatedTimer stepDownTimer; // Triggered on a leader each electionTimeoutMs / 2 millis to ensure the quorum.
    private RepeatedTimer snapshotTimer;
    private ScheduledFuture<?> transferTimer;
    private ThreadId wakingCandidate;
    /**
     * Disruptor to run node service
     */
    private StripedDisruptor<LogEntryAndClosure> applyDisruptor;
    private RingBuffer<LogEntryAndClosure> applyQueue;

    /**
     * Metrics
     */
    private NodeMetrics metrics;

    private NodeId nodeId;
    private JRaftServiceFactory serviceFactory;

    /**
     * ReplicatorStateListeners
     */
    private final CopyOnWriteArrayList<Replicator.ReplicatorStateListener> replicatorStateListeners =
        new CopyOnWriteArrayList<>();
    /**
     * Node's target leader election priority value
     */
    private volatile int targetPriority;
    /**
     * The number of elections time out for current node
     */
    private volatile int electionTimeoutCounter;

    /** Configuration of own striped disruptor for FSMCaller service of raft node, {@code null} means use shared disruptor. */
    private final @Nullable RaftNodeDisruptorConfiguration ownFsmCallerExecutorDisruptorConfig;

    private static class NodeReadWriteLock extends LongHeldDetectingReadWriteLock {
        static final long MAX_BLOCKING_MS_TO_REPORT = SystemPropertyUtil.getLong(
            "jraft.node.detecting.lock.max_blocking_ms_to_report", -1);

        private final Node node;

        NodeReadWriteLock(final Node node) {
            super(MAX_BLOCKING_MS_TO_REPORT, TimeUnit.MILLISECONDS);
            this.node = node;
        }

        @Override
        public void report(final AcquireMode acquireMode, final Thread heldThread,
            final Collection<Thread> queuedThreads, final long blockedNanos) {
            final long blockedMs = TimeUnit.NANOSECONDS.toMillis(blockedNanos);
            LOG.warn(
                "Raft-Node-Lock report: currentThread={}, acquireMode={}, heldThread={}, queuedThreads={}, blockedMs={}.",
                Thread.currentThread(), acquireMode, heldThread, queuedThreads, blockedMs);

            final NodeMetrics metrics = this.node.getNodeMetrics();
            if (metrics != null) {
                metrics.recordLatency("node-lock-blocked", blockedMs);
            }
        }
    }

    /**
     * Node service event.
     */
    public static class LogEntryAndClosure extends NodeIdAware {
        LogEntry entry;
        Closure done;
        long expectedTerm;
        CountDownLatch shutdownLatch;

        @Override
        public void reset() {
            super.reset();

            this.entry = null;
            this.done = null;
            this.expectedTerm = 0;
            this.shutdownLatch = null;
        }
    }

    /**
     * Event handler.
     */
    private class LogEntryAndClosureHandler implements EventHandler<LogEntryAndClosure> {
        // task list for batch
        private final List<LogEntryAndClosure> tasks = new ArrayList<>(NodeImpl.this.raftOptions.getApplyBatch());

        @Override
        public void onEvent(final LogEntryAndClosure event, final long sequence, final boolean endOfBatch) throws Exception {
            if (event.shutdownLatch != null) {
                if (!this.tasks.isEmpty()) {
                    executeApplyingTasks(this.tasks);
                    reset();
                }
                event.shutdownLatch.countDown();
                return;
            }

            this.tasks.add(event);
            if (this.tasks.size() >= NodeImpl.this.raftOptions.getApplyBatch() || endOfBatch) {
                executeApplyingTasks(this.tasks);
                reset();
            }
        }

        private void reset() {
            for (final LogEntryAndClosure task : tasks) {
                task.reset();
            }
            this.tasks.clear();
        }
    }

    /**
     * Configuration commit context.
     */
    private static class ConfigurationCtx {
        enum Stage {
            STAGE_NONE, // none stage
            STAGE_CATCHING_UP, // the node is catching-up
            STAGE_JOINT, // joint stage
            STAGE_STABLE // stable stage
        }

        final NodeImpl node;
        Stage stage;
        // Peers change times
        int nchanges;
        long version;
        // peers
        List<PeerId> newPeers = new ArrayList<>();
        List<PeerId> oldPeers = new ArrayList<>();
        List<PeerId> addingPeers = new ArrayList<>();
        // learners
        List<PeerId> newLearners = new ArrayList<>();
        List<PeerId> oldLearners = new ArrayList<>();
        Closure done;
        boolean async;

        ConfigurationCtx(final NodeImpl node) {
            super();
            this.node = node;
            this.stage = Stage.STAGE_NONE;
            this.version = 0;
            this.done = null;
        }

        /**
         * Start change configuration.
         */
        void start(final Configuration oldConf, final Configuration newConf, final Closure done, boolean async) {
            if (isBusy()) {
                if (done != null) {
                    Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), done, new Status(RaftError.EBUSY, "Already in busy stage."));
                }
                throw new IllegalStateException("Busy stage");
            }
            if (this.done != null) {
                if (done != null) {
                    Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), done, new Status(RaftError.EINVAL, "Already have done closure."));
                }
                throw new IllegalArgumentException("Already have done closure");
            }
            this.done = done;
            this.stage = Stage.STAGE_CATCHING_UP;
            this.async = async;
            if (async) {
                Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), done, Status.OK());
            }
            this.oldPeers = oldConf.listPeers();
            this.newPeers = newConf.listPeers();
            this.oldLearners = oldConf.listLearners();
            this.newLearners = newConf.listLearners();
            final Configuration adding = new Configuration();
            final Configuration removing = new Configuration();
            newConf.diff(oldConf, adding, removing);
            this.nchanges = adding.size() + removing.size();

            addNewLearners();
            if (adding.isEmpty()) {
                nextStage();
                return;
            }
            addNewPeers(adding);
        }

        private void addNewPeers(final Configuration adding) {
            this.addingPeers = adding.listPeers();
            LOG.info("Adding peers: {}.", this.addingPeers);
            for (final PeerId newPeer : this.addingPeers) {
                if (!this.node.replicatorGroup.addReplicator(newPeer)) {
                    LOG.error("Node {} start the replicator failed, peer={}.", this.node.getNodeId(), newPeer);
                    onCaughtUp(this.version, newPeer, false);
                    return;
                }
                final OnCaughtUp caughtUp = new OnCaughtUp(this.node, this.node.currTerm, newPeer, this.version);
                final long dueTime = Utils.nowMs() + this.node.options.getElectionTimeoutMs();
                if (!this.node.replicatorGroup.waitCaughtUp(newPeer, this.node.options.getCatchupMargin(), dueTime,
                    caughtUp)) {
                    LOG.error("Node {} waitCaughtUp, peer={}.", this.node.getNodeId(), newPeer);
                    onCaughtUp(this.version, newPeer, false);
                    return;
                }
            }
        }

        private void addNewLearners() {
            final Set<PeerId> addingLearners = new HashSet<>(this.newLearners);
            addingLearners.removeAll(this.oldLearners);
            LOG.info("Adding learners: {}.", addingLearners);
            for (final PeerId newLearner : addingLearners) {
                if (!this.node.replicatorGroup.addReplicator(newLearner, ReplicatorType.Learner)) {
                    LOG.error("Node {} start the learner replicator failed, peer={}.", this.node.getNodeId(),
                        newLearner);
                }
            }
        }

        void onCaughtUp(final long version, final PeerId peer, final boolean success) {
            if (version != this.version) {
                LOG.warn("Ignore onCaughtUp message, mismatch configuration context version, expect {}, but is {}.",
                    this.version, version);
                return;
            }
            Requires.requireTrue(this.stage == Stage.STAGE_CATCHING_UP, "Stage is not in STAGE_CATCHING_UP");
            if (success) {
                LOG.info("Catch up for peer={} was finished", peer);
                this.addingPeers.remove(peer);
                if (this.addingPeers.isEmpty()) {
                    nextStage();
                    return;
                }
                return;
            }
            LOG.warn("Node {} fail to catch up peer {} when trying to change peers from {} to {}.",
                this.node.getNodeId(), peer, this.oldPeers, this.newPeers);
            reset(new Status(RaftError.ECATCHUP, "Peer %s failed to catch up.", peer));
        }

        void reset() {
            reset(null);
        }

        void reset(final Status st) {
            if (st != null && st.isOk()) {
                this.node.stopReplicator(this.newPeers, this.oldPeers);
                this.node.stopReplicator(this.newLearners, this.oldLearners);
            }
            else {
                this.node.stopReplicator(this.oldPeers, this.newPeers);
                this.node.stopReplicator(this.oldLearners, this.newLearners);
            }

            // must be copied before clearing
            List<PeerId> resultPeerIds = List.copyOf(this.newPeers);
            List<PeerId> resultLearnerIds = List.copyOf(this.newLearners);

            clearPeers();
            clearLearners();

            this.version++;
            this.stage = Stage.STAGE_NONE;
            this.nchanges = 0;

            Closure oldDoneClosure = done;

            if (this.done != null) {
                Closure newDone = (Status status) -> {
                    JraftGroupEventsListener listener = node.getOptions().getRaftGrpEvtsLsnr();

                    if (listener != null) {
                        if (status.isOk()) {
                            listener.onNewPeersConfigurationApplied(resultPeerIds, resultLearnerIds);
                        } else {
                            listener.onReconfigurationError(status, resultPeerIds, resultLearnerIds, node.getCurrentTerm());
                        }
                    }

                    if (!this.async) {
                      oldDoneClosure.run(status);
                    }
                };

                // In case of changePeerAsync this invocation is used in order to trigger listener callbacks.
                Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), newDone, st != null ? st : LEADER_STEPPED_DOWN);
                this.done = null;
            }
        }

        private void clearLearners() {
            this.newLearners.clear();
            this.oldLearners.clear();
        }

        private void clearPeers() {
            this.newPeers.clear();
            this.oldPeers.clear();
            this.addingPeers.clear();
        }

        /**
         * Invoked when this node becomes the leader, write a configuration change log as the first log.
         */
        void flush(final Configuration conf, final Configuration oldConf) {
            Requires.requireTrue(!isBusy(), "Flush when busy");
            this.newPeers = conf.listPeers();
            this.newLearners = conf.listLearners();
            if (oldConf == null || oldConf.isEmpty()) {
                this.stage = Stage.STAGE_STABLE;
                this.oldPeers = this.newPeers;
                this.oldLearners = this.newLearners;
            }
            else {
                this.stage = Stage.STAGE_JOINT;
                this.oldPeers = oldConf.listPeers();
                this.oldLearners = oldConf.listLearners();
            }
            this.node.unsafeApplyConfiguration(conf, oldConf == null || oldConf.isEmpty() ? null : oldConf, true);
        }

        void nextStage() {
            Requires.requireTrue(isBusy(), "Not in busy stage");
            switch (this.stage) {
                case STAGE_CATCHING_UP:
                    LOG.info("Catch up phase to change peers from={} to={} was successfully finished", oldPeers, newPeers);
                    if (this.nchanges > 0) {
                        this.stage = Stage.STAGE_JOINT;
                        this.node.unsafeApplyConfiguration(new Configuration(this.newPeers, this.newLearners),
                            new Configuration(this.oldPeers), false);
                        return;
                    }
                    // fallthrough.
                case STAGE_JOINT:
                    this.stage = Stage.STAGE_STABLE;
                    this.node.unsafeApplyConfiguration(new Configuration(this.newPeers, this.newLearners), null, false);
                    break;
                case STAGE_STABLE:
                    final boolean shouldStepDown = !this.newPeers.contains(this.node.serverId);
                    reset(new Status());
                    if (shouldStepDown) {
                        this.node.stepDown(this.node.currTerm, true, new Status(RaftError.ELEADERREMOVED,
                            "This node was removed."));
                    }
                    break;
                case STAGE_NONE:
                    //noinspection ConstantConditions
                    Requires.requireTrue(false, "Can't reach here");
                    break;
            }
        }

        boolean isBusy() {
            return this.stage != Stage.STAGE_NONE;
        }
    }

    public NodeImpl(final String groupId, final PeerId serverId) {
        this(groupId, serverId, null);
    }

        public NodeImpl(
                final String groupId,
                final PeerId serverId,
                @Nullable RaftNodeDisruptorConfiguration ownFsmCallerExecutorDisruptorConfig
        ) {
            super();
            if (groupId != null) {
                Utils.verifyGroupId(groupId);
            }
            this.groupId = groupId;
            this.serverId = serverId != null ? serverId.copy() : null;
            this.state = State.STATE_UNINITIALIZED;
            this.currTerm = 0;
            updateLastLeaderTimestamp(Utils.monotonicMs());
            this.confCtx = new ConfigurationCtx(this);
            this.wakingCandidate = null;
            this.ownFsmCallerExecutorDisruptorConfig = ownFsmCallerExecutorDisruptorConfig;
        }

    public HybridClock clock() {
        return clock;
    }

    public HybridTimestamp clockNow() {
        return clock.now();
    }

    public HybridTimestamp clockUpdate(HybridTimestamp timestamp) {
        return clock.update(timestamp);
    }

    private boolean initSnapshotStorage() {
        if (StringUtils.isEmpty(this.options.getSnapshotUri())) {
            LOG.warn("Do not set snapshot uri, ignore initSnapshotStorage.");
            return true;
        }
        this.snapshotExecutor = new SnapshotExecutorImpl();
        final SnapshotExecutorOptions opts = new SnapshotExecutorOptions();
        opts.setUri(this.options.getSnapshotUri());
        opts.setFsmCaller(this.fsmCaller);
        opts.setNode(this);
        opts.setLogManager(this.logManager);
        opts.setPeerId(this.serverId);
        opts.setInitTerm(this.currTerm);
        opts.setFilterBeforeCopyRemote(this.options.isFilterBeforeCopyRemote());
        // get snapshot throttle
        opts.setSnapshotThrottle(this.options.getSnapshotThrottle());
        return this.snapshotExecutor.init(opts);
    }

    private boolean initLogStorage() {
        Requires.requireNonNull(this.fsmCaller, "Null fsm caller");
        this.logStorage = this.serviceFactory.createLogStorage(this.options.getLogUri(), this.raftOptions);
        this.logManager = new StripeAwareLogManager();

        LogManagerOptions opts = new LogManagerOptions();
        opts.setLogEntryCodecFactory(this.serviceFactory.createLogEntryCodecFactory());
        opts.setLogStorage(this.logStorage);
        opts.setConfigurationManager(this.configManager);
        opts.setNode(this);
        opts.setFsmCaller(this.fsmCaller);
        opts.setNodeMetrics(this.metrics);
        opts.setRaftOptions(this.raftOptions);
        opts.setLogManagerDisruptor(options.getLogManagerDisruptor());
        opts.setLogStripes(options.getLogStripes());

        return this.logManager.init(opts);
    }

    private boolean initMetaStorage() {
        this.metaStorage = this.serviceFactory.createRaftMetaStorage(this.options.getRaftMetaUri(), this.raftOptions);
        RaftMetaStorageOptions opts = new RaftMetaStorageOptions();
        opts.setNode(this);
        if (!this.metaStorage.init(opts)) {
            LOG.error("Node {} init meta storage failed, uri={}.", this.serverId, this.options.getRaftMetaUri());
            return false;
        }
        this.currTerm = this.metaStorage.getTerm();
        this.votedId = this.metaStorage.getVotedFor().copy();
        return true;
    }

    private void handleSnapshotTimeout() {
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                return;
            }
        }
        finally {
            this.writeLock.unlock();
        }
        // do_snapshot in another thread to avoid blocking the timer thread.
        Utils.runInThread(this.getOptions().getCommonExecutor(), () -> doSnapshot(null));
    }

    private void handleElectionTimeout() {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (this.state != State.STATE_FOLLOWER) {
                return;
            }
            if (isCurrentLeaderValid()) {
                return;
            }
            resetLeaderId(PeerId.emptyPeer(), new Status(RaftError.ERAFTTIMEDOUT, "Lost connection from leader %s.",
                this.leaderId));

            // Judge whether to launch a election.
            if (!allowLaunchElection()) {
                return;
            }

            doUnlock = false;
            adjustElectionTimeout();
            preVote();

        }
        finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * Method that adjusts election timeout after several consecutive unsuccessful leader elections according to {@link TimeoutStrategy}
     * <p>
     * Notes about general algorithm: The main idea is that in a stable cluster election timeout should be relatively small, but when
     * something prevents elections from completion, like an unstable network or long GC pauses, we don't want to have a lot of
     * elections, so election timeout is adjusted. Hence, the upper bound of the election timeout adjusting is the value, which is enough to
     * elect a leader or handle problems that prevent a successful leader election.
     * <p>
     * Leader election timeout is set to an initial value after a successful election of a leader.
     */
    private void adjustElectionTimeout() {
        electionRound++;

        if (electionRound > 1)
            LOG.info("Unsuccessful election round number {}", electionRound);

        if (!electionAdjusted) {
            initialElectionTimeout = options.getElectionTimeoutMs();
        }

        long timeout = options.getElectionTimeoutStrategy().nextTimeout(options.getElectionTimeoutMs(), electionRound);

        if (timeout != options.getElectionTimeoutMs()) {
            resetElectionTimeoutMs((int) timeout);
            LOG.info("Election timeout was adjusted according to {} ", options.getElectionTimeoutStrategy());
            electionAdjusted = true;
        }
    }

    /**
     * Method that resets election timeout to initial value after an adjusting.
     *
     * For more details see {@link NodeImpl#adjustElectionTimeout()}.
     */
    private void resetElectionTimeoutToInitial() {
        electionRound = 0;

        if (electionAdjusted) {
            LOG.info("Election timeout was reset to initial value.");
            resetElectionTimeoutMs(initialElectionTimeout);
            electionAdjusted = false;
        }
    }

    /**
     * Whether to allow for launching election or not by comparing node's priority with target priority. And at the same
     * time, if next leader is not elected until next election timeout, it decays its local target priority
     * exponentially.
     *
     * @return Whether current node will launch election or not.
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private boolean allowLaunchElection() {

        // Priority 0 is a special value so that a node will never participate in election.
        if (this.serverId.isPriorityNotElected()) {
            LOG.warn("Node {} will never participate in election, because it's priority={}.", getNodeId(),
                this.serverId.getPriority());
            return false;
        }

        // If this nodes disable priority election, then it can make a election.
        if (this.serverId.isPriorityDisabled()) {
            return true;
        }

        // If current node's priority < target_priority, it does not initiate leader,
        // election and waits for the next election timeout.
        if (this.serverId.getPriority() < this.targetPriority) {
            this.electionTimeoutCounter++;

            // If next leader is not elected until next election timeout, it
            // decays its local target priority exponentially.
            if (this.electionTimeoutCounter > 1) {
                decayTargetPriority();
                this.electionTimeoutCounter = 0;
            }

            if (this.electionTimeoutCounter == 1) {
                LOG.debug("Node {} does not initiate leader election and waits for the next election timeout.",
                    getNodeId());
                return false;
            }
        }

        return this.serverId.getPriority() >= this.targetPriority;
    }

    /**
     * Decay targetPriority value based on gap value.
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void decayTargetPriority() {
        // Default Gap value should be bigger than 10.
        final int decayPriorityGap = Math.max(this.options.getDecayPriorityGap(), 10);
        final int gap = Math.max(decayPriorityGap, (this.targetPriority / 5));

        final int prevTargetPriority = this.targetPriority;
        this.targetPriority = Math.max(ElectionPriority.MinValue, (this.targetPriority - gap));
        LOG.info("Node {} priority decay, from: {}, to: {}.", getNodeId(), prevTargetPriority, this.targetPriority);
    }

    /**
     * Check and set configuration for node.At the same time, if configuration is changed, then compute and update the
     * target priority value.
     *
     * @param inLock whether the writeLock has already been locked in other place.
     */
    private void checkAndSetConfiguration(final boolean inLock) {
        if (!inLock) {
            this.writeLock.lock();
        }
        try {
            final ConfigurationEntry prevConf = this.conf;
            this.conf = this.logManager.checkAndSetConfiguration(prevConf);
            refreshLeadershipAbstaining();

            if (this.conf != prevConf) {
                // Update target priority value
                final int prevTargetPriority = this.targetPriority;
                this.targetPriority = getMaxPriorityOfNodes(this.conf.getConf().getPeers());
                if (prevTargetPriority != this.targetPriority) {
                    LOG.info("Node {} target priority value has changed from: {}, to: {}.", getNodeId(),
                        prevTargetPriority, this.targetPriority);
                }
                this.electionTimeoutCounter = 0;
            }
        }
        finally {
            if (!inLock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * Get max priority value for all nodes in the same Raft group, and update current node's target priority value.
     *
     * @param peerIds peer nodes in the same Raft group
     */
    private int getMaxPriorityOfNodes(final List<PeerId> peerIds) {
        Requires.requireNonNull(peerIds, "Null peer list");

        int maxPriority = Integer.MIN_VALUE;
        for (final PeerId peerId : peerIds) {
            final int priorityVal = peerId.getPriority();
            maxPriority = Math.max(priorityVal, maxPriority);
        }

        return maxPriority;
    }

    private boolean initFSMCaller(final LogId bootstrapId) {
        if (this.fsmCaller == null) {
            LOG.error("Fail to init fsm caller, null instance, bootstrapId={}.", bootstrapId);
            return false;
        }
        this.closureQueue = new ClosureQueueImpl(this.getOptions());
        final FSMCallerOptions opts = new FSMCallerOptions();
        opts.setAfterShutdown(status -> afterShutdown());
        opts.setLogManager(this.logManager);
        opts.setFsm(this.options.getFsm());
        opts.setClosureQueue(this.closureQueue);
        opts.setNode(this);
        opts.setBootstrapId(bootstrapId);
        opts.setRaftMessagesFactory(raftOptions.getRaftMessagesFactory());
        opts.setfSMCallerExecutorDisruptor(options.getfSMCallerExecutorDisruptor());

        return this.fsmCaller.init(opts);
    }

    private static class BootstrapStableClosure extends LogManager.StableClosure {

        private final SynchronizedClosure done = new SynchronizedClosure();

        BootstrapStableClosure() {
            super(null);
        }

        public Status await() throws InterruptedException {
            return this.done.await();
        }

        @Override
        public void run(final Status status) {
            this.done.run(status);
        }
    }

    public boolean bootstrap(final BootstrapOptions opts) throws InterruptedException {
        if (opts.getLastLogIndex() > 0 && (opts.getGroupConf().isEmpty() || opts.getFsm() == null)) {
            LOG.error("Invalid arguments for bootstrap, groupConf={}, fsm={}, lastLogIndex={}.", opts.getGroupConf(),
                opts.getFsm(), opts.getLastLogIndex());
            return false;
        }
        if (opts.getGroupConf().isEmpty()) {
            LOG.error("Bootstrapping an empty node makes no sense.");
            return false;
        }
        Requires.requireNonNull(opts.getServiceFactory(), "Null jraft service factory");
        this.serviceFactory = opts.getServiceFactory();
        // Term is not an option since changing it is very dangerous
        final long bootstrapLogTerm = opts.getLastLogIndex() > 0 ? 1 : 0;
        final LogId bootstrapId = new LogId(opts.getLastLogIndex(), bootstrapLogTerm);
        this.options = opts.getNodeOptions() == null ? new NodeOptions() : opts.getNodeOptions();
        this.clock = options.getClock();
        this.raftOptions = this.options.getRaftOptions();
        this.metrics = new NodeMetrics(opts.isEnableMetrics());
        this.options.setFsm(opts.getFsm());
        this.options.setLogUri(opts.getLogUri());
        this.options.setRaftMetaUri(opts.getRaftMetaUri());
        this.options.setSnapshotUri(opts.getSnapshotUri());

        this.configManager = new ConfigurationManager();
        // Create fsmCaller at first as logManager needs it to report error
        this.fsmCaller = new FSMCallerImpl();

        initPools(options);

        if (!initLogStorage()) {
            LOG.error("Fail to init log storage.");
            return false;
        }
        if (!initMetaStorage()) {
            LOG.error("Fail to init meta storage.");
            return false;
        }
        if (this.currTerm == 0) {
            this.currTerm = 1;
            if (!this.metaStorage.setTermAndVotedFor(1, new PeerId())) {
                LOG.error("Fail to set term.");
                return false;
            }
        }

        if (opts.getFsm() != null && !initFSMCaller(bootstrapId)) {
            LOG.error("Fail to init fsm caller.");
            return false;
        }

        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.getId().setTerm(this.currTerm);
        entry.setPeers(opts.getGroupConf().listPeers());
        entry.setLearners(opts.getGroupConf().listLearners());

        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);

        final BootstrapStableClosure bootstrapDone = new BootstrapStableClosure();
        this.logManager.appendEntries(entries, bootstrapDone);
        if (!bootstrapDone.await().isOk()) {
            LOG.error("Fail to append configuration.");
            return false;
        }

        if (opts.getLastLogIndex() > 0) {
            if (!initSnapshotStorage()) {
                LOG.error("Fail to init snapshot storage.");
                return false;
            }
            final SynchronizedClosure snapshotDone = new SynchronizedClosure();
            this.snapshotExecutor.doSnapshot(snapshotDone);
            if (!snapshotDone.await().isOk()) {
                LOG.error("Fail to save snapshot, status={}.", snapshotDone.getStatus());
                return false;
            }
        }

        if (this.logManager.getFirstLogIndex() != opts.getLastLogIndex() + 1) {
            throw new IllegalStateException("First and last log index mismatch");
        }
        if (opts.getLastLogIndex() > 0) {
            if (this.logManager.getLastLogIndex() != opts.getLastLogIndex()) {
                throw new IllegalStateException("Last log index mismatch");
            }
        }
        else {
            if (this.logManager.getLastLogIndex() != opts.getLastLogIndex() + 1) {
                throw new IllegalStateException("Last log index mismatch");
            }
        }

        return true;
    }

    private int heartbeatTimeout(final int electionTimeout) {
        return Math.max(electionTimeout / this.raftOptions.getElectionHeartbeatFactor(), 10);
    }

    private int randomTimeout(final int timeoutMs) {
        return ThreadLocalRandom.current().nextInt(timeoutMs, timeoutMs + this.raftOptions.getMaxElectionDelayMs());
    }

    @Override
    public boolean init(final NodeOptions opts) {
        Requires.requireNonNull(opts, "Null node options");
        Requires.requireNonNull(opts.getRaftOptions(), "Null raft options");
        Requires.requireNonNull(opts.getServiceFactory(), "Null jraft service factory");
        Requires.requireNonNull(opts.getCommandsMarshaller(), "Null commands marshaller");
        this.serviceFactory = opts.getServiceFactory();
        this.clock = opts.getClock();
        this.options = opts;
        this.raftOptions = opts.getRaftOptions();
        this.metrics = new NodeMetrics(opts.isEnableMetrics());
        this.serverId.setPriority(opts.getElectionPriority());
        this.electionTimeoutCounter = 0;
        if (opts.getReplicationStateListeners() != null)
            this.replicatorStateListeners.addAll(opts.getReplicationStateListeners());

        if (this.serverId.isEmpty()) {
            LOG.error("Server ID is empty.");
            return false;
        }

        // Init timers.
        initTimers(opts);

        // Init pools.
        initPools(opts);

        this.configManager = new ConfigurationManager();

        applyDisruptor = opts.getNodeApplyDisruptor();

        applyQueue = applyDisruptor.subscribe(getNodeId(), new LogEntryAndClosureHandler());

        if (this.metrics.getMetricRegistry() != null) {
            this.metrics.getMetricRegistry().register("jraft-node-impl-disruptor",
                new DisruptorMetricSet(this.applyQueue));
        }

        this.fsmCaller = new FSMCallerImpl();
        if (!initLogStorage()) {
            LOG.error("Node {} initLogStorage failed.", getNodeId());
            return false;
        }
        if (!initMetaStorage()) {
            LOG.error("Node {} initMetaStorage failed.", getNodeId());
            return false;
        }
        if (!initFSMCaller(new LogId(0, 0))) {
            LOG.error("Node {} initFSMCaller failed.", getNodeId());
            return false;
        }

        if (!initSnapshotStorage()) {
            LOG.error("Node {} initSnapshotStorage failed.", getNodeId());
            return false;
        }

        final Status st = this.logManager.checkConsistency();
        if (!st.isOk()) {
            LOG.error("Node {} is initialized with inconsistent log, status={}.", getNodeId(), st);
            return false;
        }
        this.conf = new ConfigurationEntry();
        this.conf.setId(new LogId());
        // if have log using conf in log, else using conf in options
        if (this.logManager.getLastLogIndex() > 0) {
            checkAndSetConfiguration(false);
        }
        else {
            this.conf.setConf(this.options.getInitialConf());
            // initially set to max(priority of all nodes)
            this.targetPriority = getMaxPriorityOfNodes(this.conf.getConf().getPeers());
        }

        // It must be initialized after initializing conf and log storage.
        if (!initBallotBox()) {
            LOG.error("Node {} init ballotBox failed.", getNodeId());
            return false;
        }

        if (!this.conf.isEmpty()) {
            Requires.requireTrue(this.conf.isValid(), "Invalid conf: %s", this.conf);
        }
        else {
            LOG.info("Init node {} with empty conf.", this.serverId);
        }

        this.replicatorGroup = new ReplicatorGroupImpl();
        this.rpcClientService = new DefaultRaftClientService();
        final ReplicatorGroupOptions rgOpts = new ReplicatorGroupOptions();
        rgOpts.setHeartbeatTimeoutMs(heartbeatTimeout(this.options.getElectionTimeoutMs()));
        rgOpts.setElectionTimeoutMs(this.options.getElectionTimeoutMs());
        rgOpts.setLogManager(this.logManager);
        rgOpts.setBallotBox(this.ballotBox);
        rgOpts.setNode(this);
        rgOpts.setRaftRpcClientService(this.rpcClientService);
        rgOpts.setSnapshotStorage(this.snapshotExecutor != null ? this.snapshotExecutor.getSnapshotStorage() : null);
        rgOpts.setRaftOptions(this.raftOptions);
        rgOpts.setTimerManager(this.options.getScheduler());

        // Adds metric registry to RPC service.
        this.options.setMetricRegistry(this.metrics.getMetricRegistry());

        if (!this.rpcClientService.init(this.options)) {
            LOG.error("Fail to init rpc service.");
            return false;
        }
        this.replicatorGroup.init(new NodeId(this.groupId, this.serverId), rgOpts);

        this.readOnlyService = new ReadOnlyServiceImpl();
        final ReadOnlyServiceOptions rosOpts = new ReadOnlyServiceOptions();
        rosOpts.setFsmCaller(this.fsmCaller);
        rosOpts.setNode(this);
        rosOpts.setRaftOptions(this.raftOptions);
        rosOpts.setReadOnlyServiceDisruptor(opts.getReadOnlyServiceDisruptor());

        if (!this.readOnlyService.init(rosOpts)) {
            LOG.error("Fail to init readOnlyService.");
            return false;
        }


            // set state to follower
            this.state = State.STATE_FOLLOWER;

            if (LOG.isInfoEnabled()) {
                LOG.info("Node {} init, term={}, lastLogId={}, conf={}, oldConf={}.", getNodeId(), this.currTerm,
                    this.logManager.getLastLogId(false), this.conf.getConf(), this.conf.getOldConf());
            }

            if (this.snapshotExecutor != null && this.options.getSnapshotIntervalSecs() > 0) {
                LOG.debug("Node {} start snapshot timer, term={}.", getNodeId(), this.currTerm);
                this.snapshotTimer.start();
            }

            if (!this.conf.isEmpty()) {
                stepDown(this.currTerm, false, new Status());
            }

            // Now the raft node is started , have to acquire the writeLock to avoid race
            // conditions
            this.writeLock.lock();
            if (this.conf.isStable() && this.conf.getConf().size() == 1 && this.conf.getConf().contains(this.serverId)) {
                // The group contains only this server which must be the LEADER, trigger
                // the timer immediately.
                electSelf();
            }
            else {
                this.writeLock.unlock();
            }

        return true;
    }

    /**
     * If there is an externally enforced config index (in the {@link #options}), then the node abstains from becoming a leader
     * in configurations whose index precedes the externally enforced index.
     *
     * <p>The idea is that, if a Raft group was forcefully repaired (because it lost majority) using {@link #resetPeers( Configuration)},
     * the old majority nodes might come back online. If this happens and we do nothing, they might elect a leader from the old majority
     * that could hijack leadership and cause havoc in the repaired group.
     *
     * <p>To prevent this, on a starup or subsequent config changes, current voting set (aka peers) of the repaired group may be 'broken'
     * to make it impossible for the current node to become a leader. This is enabled by setting a non-null value to
     * {@link NodeOptions#getExternallyEnforcedConfigIndex ()}. When it's set, on each change of configuration (happening to this.conf),
     * including the one at startup (in {@link #init( NodeOptions)}), we check whether the applied config precedes the externally enforced
     * config (in which case this.conf.peers will be 'broken' to make sure current node does not become a leader) or not (in which case
     * the applied config will be used as is).
     */
    private void refreshLeadershipAbstaining() {
        Long externallyEnforcedConfigIndex = options.getExternallyEnforcedConfigIndex();
        if (externallyEnforcedConfigIndex == null) {
            return;
        }
        if (this.conf.getId().getIndex() >= externallyEnforcedConfigIndex) {
            // Already applied the externally enforced config, no need to abstain anymore.
            return;
        }

        LOG.info(
                "Will abstain from becoming a leader until a configuration with target index gets applied "
                        + "[nodeId={}, externallyEnforcedConfigIndex={}].",
                this.nodeId, externallyEnforcedConfigIndex
        );

        Configuration newConf = pseudoConfigToAbstainFromBecomingLeader();
        LOG.info("Node {} set config from {} to {} to abstain from becoming a leader.", getNodeId(), this.conf.getConf(), newConf);
        this.conf.setConf(newConf);
        this.conf.getOldConf().reset();
    }

    private Configuration pseudoConfigToAbstainFromBecomingLeader() {
        List<PeerId> peersWithoutThisNode = List.of(new PeerId("not-me-" + this.serverId.getConsistentId()));
        List<PeerId> learnersWithThisNode = List.of(this.serverId);
        return new Configuration(peersWithoutThisNode, learnersWithThisNode);
    }

    private boolean initBallotBox() {
        this.ballotBox = new BallotBox();
        final BallotBoxOptions ballotBoxOpts = new BallotBoxOptions();
        ballotBoxOpts.setWaiter(this.fsmCaller);
        ballotBoxOpts.setClosureQueue(this.closureQueue);
        // TODO: uncomment when backport related change https://issues.apache.org/jira/browse/IGNITE-22923
        //ballotBoxOpts.setNodeId(getNodeId());
         // Try to initialize the last committed index in BallotBox to be the last snapshot index.
        long lastCommittedIndex = 0;
        if (this.snapshotExecutor != null) {
            lastCommittedIndex = this.snapshotExecutor.getLastSnapshotIndex();
        }
        if (this.getQuorum() == 1) {
            // It is safe to initiate lastCommittedIndex as last log one because in case of single peer no one will discard
            // log records on leader election.
            // Fix https://github.com/sofastack/sofa-jraft/issues/1049
            lastCommittedIndex = Math.max(lastCommittedIndex, this.logManager.getLastLogIndex());
        }

        ballotBoxOpts.setLastCommittedIndex(lastCommittedIndex);
        LOG.info("Node {} init ballot box's lastCommittedIndex={}.", getNodeId(), lastCommittedIndex);
        return this.ballotBox.init(ballotBoxOpts);
    }

    /**
     * Validates a required option if shared pools are enabled.
     *
     * @param opts Options.
     * @param name Option name.
     */
    private boolean validateOption(NodeOptions opts, String name) {
        if (opts.isSharedPools())
            throw new IllegalArgumentException(name + " is required if shared pools are enabled");

        return true;
    }


    /**
     * Initialize timer pools.
     * @param opts The options.
     */
    private void initTimers(final NodeOptions opts) {
        if (opts.getScheduler() == null && validateOption(opts, "scheduler"))
            opts.setScheduler(JRaftUtils.createScheduler(opts));

        String name = "JRaft-VoteTimer";
        if (opts.getVoteTimer() == null && validateOption(opts, "voteTimer")) {
            opts.setVoteTimer(JRaftUtils.createTimer(opts, name));
        }

        this.voteTimer = new RepeatedTimer(name, NodeImpl.this.options.getElectionTimeoutMs(), opts.getVoteTimer()) {
            @Override
            protected void onTrigger() {
                handleVoteTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };

        name = "JRaft-ElectionTimer";
        if (opts.getElectionTimer() == null && validateOption(opts, "electionTimer"))
            opts.setElectionTimer(JRaftUtils.createTimer(opts, name));
        electionTimer = new RepeatedTimer(name, NodeImpl.this.options.getElectionTimeoutMs(), opts.getElectionTimer()) {
            @Override
            protected void onTrigger() {
                handleElectionTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };

        name = "JRaft-StepDownTimer";
        if (opts.getStepDownTimer() == null && validateOption(opts, "stepDownTimer"))
            opts.setStepDownTimer(JRaftUtils.createTimer(opts, name));
        stepDownTimer = new RepeatedTimer(name, NodeImpl.this.options.getElectionTimeoutMs() >> 1, opts.getStepDownTimer()) {
            @Override
            protected void onTrigger() {
                handleStepDownTimeout();
            }
        };

        name = "JRaft-SnapshotTimer";
        if (opts.getSnapshotTimer() == null && validateOption(opts, "snapshotTimer"))
            opts.setSnapshotTimer(JRaftUtils.createTimer(opts, name));
        snapshotTimer = new RepeatedTimer(name, NodeImpl.this.options.getSnapshotIntervalSecs() * 1000, opts.getSnapshotTimer()) {
            private volatile boolean firstSchedule = true;

            @Override
            protected void onTrigger() {
                handleSnapshotTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                if (!this.firstSchedule) {
                    return timeoutMs;
                }

                // Randomize the first snapshot trigger timeout
                this.firstSchedule = false;
                if (timeoutMs > 0) {
                    int half = timeoutMs / 2;
                    return half + ThreadLocalRandom.current().nextInt(half);
                }
                else {
                    return timeoutMs;
                }
            }
        };
    }

    /**
     * @param opts Options.
     */
    private void initPools(final NodeOptions opts) {
        if (opts.getCommonExecutor() == null && validateOption(opts, "commonExecutor"))
            opts.setCommonExecutor(JRaftUtils.createCommonExecutor(opts));

        if (opts.getStripedExecutor() == null && validateOption(opts, "stripedExecutor"))
            opts.setStripedExecutor(JRaftUtils.createAppendEntriesExecutor(opts));

        if (opts.getClientExecutor() == null && validateOption(opts, "clientExecutor"))
            opts.setClientExecutor(JRaftUtils.createClientExecutor(opts, opts.getServerName()));

        if (opts.getRaftMetrics() == null) {
            opts.setRaftMetrics(new RaftMetricSource(opts.getStripes(), opts.getLogStripesCount()));
        }

        if (opts.getfSMCallerExecutorDisruptor() == null) {
            opts.setfSMCallerExecutorDisruptor(new StripedDisruptor<FSMCallerImpl.ApplyTask>(
                opts.getServerName(),
                "JRaft-FSMCaller-Disruptor",
                (nodeName, stripeName) -> IgniteThreadFactory.create(nodeName, stripeName, true, LOG, STORAGE_READ, STORAGE_WRITE),
                opts.getRaftOptions().getDisruptorBufferSize(),
                () -> new FSMCallerImpl.ApplyTask(),
                opts.getStripes(),
                false,
                false,
                opts.getRaftMetrics().disruptorMetrics("raft.fsmcaller.disruptor")
            ));
        } else if (ownFsmCallerExecutorDisruptorConfig != null) {
            opts.setfSMCallerExecutorDisruptor(new StripedDisruptor<FSMCallerImpl.ApplyTask>(
                opts.getServerName(),
                "JRaft-FSMCaller-Disruptor" + ownFsmCallerExecutorDisruptorConfig.getThreadPostfix(),
                opts.getRaftOptions().getDisruptorBufferSize(),
                () -> new FSMCallerImpl.ApplyTask(),
                ownFsmCallerExecutorDisruptorConfig.getStripes(),
                false,
                false,
                null
            ));
        }

        if (opts.getNodeApplyDisruptor() == null) {
            opts.setNodeApplyDisruptor(new StripedDisruptor<NodeImpl.LogEntryAndClosure>(
                opts.getServerName(),
                "JRaft-NodeImpl-Disruptor",
                opts.getRaftOptions().getDisruptorBufferSize(),
                () -> new NodeImpl.LogEntryAndClosure(),
                opts.getStripes(),
                false,
                false,
                opts.getRaftMetrics().disruptorMetrics("raft.nodeimpl.disruptor")
            ));
        }

        if (opts.getReadOnlyServiceDisruptor() == null) {
            opts.setReadOnlyServiceDisruptor(new StripedDisruptor<ReadOnlyServiceImpl.ReadIndexEvent>(
                opts.getServerName(),
                "JRaft-ReadOnlyService-Disruptor",
                opts.getRaftOptions().getDisruptorBufferSize(),
                () -> new ReadOnlyServiceImpl.ReadIndexEvent(),
                opts.getStripes(),
                false,
                false,
                opts.getRaftMetrics().disruptorMetrics("raft.readonlyservice.disruptor")
            ));
        }

        if (opts.getLogManagerDisruptor() == null) {
            opts.setLogManagerDisruptor(new StripedDisruptor<LogManagerImpl.StableClosureEvent>(
                opts.getServerName(),
                "JRaft-LogManager-Disruptor",
                opts.getRaftOptions().getDisruptorBufferSize(),
                () -> new LogManagerImpl.StableClosureEvent(),
                opts.getLogStripesCount(),
                logStorage instanceof RocksDbSharedLogStorage,
                opts.isLogYieldStrategy(),
                opts.getRaftMetrics().disruptorMetrics("raft.logmanager.disruptor")
            ));

            opts.setLogStripes(IntStream.range(0, opts.getLogStripesCount()).mapToObj(i -> new Stripe()).collect(toList()));
        }
    }

    @OnlyForTest
    void tryElectSelf() {
        this.writeLock.lock();
        // unlock in electSelf
        electSelf();
    }

    // should be in writeLock
    private void electSelf() {
        long electSelfTerm;
        try {
            LOG.info("Node {} start vote and grant vote self, term={}.", getNodeId(), this.currTerm);
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do electSelf as it is not in {}.", getNodeId(), this.conf);
                return;
            }
            if (this.state == State.STATE_FOLLOWER) {
                LOG.debug("Node {} stop election timer, term={}.", getNodeId(), this.currTerm);
                this.electionTimer.stop();
            }
            resetLeaderId(PeerId.emptyPeer(), new Status(RaftError.ERAFTTIMEDOUT,
                "A follower's leader_id is reset to NULL as it begins to request_vote."));
            this.state = State.STATE_CANDIDATE;
            this.currTerm++;
            this.votedId = this.serverId.copy();
            LOG.debug("Node {} start vote timer, term={} .", getNodeId(), this.currTerm);
            this.voteTimer.start();
            this.voteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            electSelfTerm = this.currTerm;
        }
        finally {
            this.writeLock.unlock();
        }

        final LogId lastLogId = this.logManager.getLastLogId(true);

        this.writeLock.lock();
        try {
            // vote need defense ABA after unlock&writeLock
            if (electSelfTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when getLastLogId.", getNodeId(), this.currTerm);
                return;
            }
            for (final PeerId peer : this.conf.listPeers()) {
                if (peer.equals(this.serverId)) {
                    continue;
                }

                rpcClientService.connectAsync(peer).thenAccept(ok -> {
                    if (!ok) {
                        LOG.warn("Node {} failed to init channel, address={}.", getNodeId(), peer);
                        return ;
                    }
                    final OnRequestVoteRpcDone done = new OnRequestVoteRpcDone(peer, electSelfTerm, this);
                    done.request = raftOptions.getRaftMessagesFactory()
                            .requestVoteRequest()
                            .preVote(false) // It's not a pre-vote request.
                            .groupId(this.groupId)
                            .serverId(this.serverId.toString())
                            .peerId(peer.toString())
                            .term(electSelfTerm)
                            .lastLogIndex(lastLogId.getIndex())
                            .lastLogTerm(lastLogId.getTerm())
                            .build();
                    this.rpcClientService.requestVote(peer, done.request, done);
                });
            }

            this.metaStorage.setTermAndVotedFor(electSelfTerm, this.serverId);
            this.voteCtx.grant(this.serverId);
            if (this.voteCtx.isGranted()) {
                becomeLeader();
            }
        }
        finally {
            this.writeLock.unlock();
        }
    }

    private void resetLeaderId(final PeerId newLeaderId, final Status status) {
        if (newLeaderId.isEmpty()) {
            if (!this.leaderId.isEmpty() && this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                this.fsmCaller.onStopFollowing(new LeaderChangeContext(this.leaderId.copy(), this.currTerm, status));
            }
            this.leaderId = PeerId.emptyPeer();
        }
        else {
            if (this.leaderId == null || this.leaderId.isEmpty()) {
                this.fsmCaller.onStartFollowing(new LeaderChangeContext(newLeaderId, this.currTerm, status));
            }
            this.leaderId = newLeaderId.copy();

            resetElectionTimeoutToInitial();
        }
    }

    // in writeLock
    private void checkStepDown(final long requestTerm, final PeerId serverId) {
        final Status status = new Status();
        if (requestTerm > this.currTerm) {
            status.setError(RaftError.ENEWLEADER, "Raft node receives message from new leader with higher term.");
            stepDown(requestTerm, false, status);
        }
        else if (this.state != State.STATE_FOLLOWER) {
            status.setError(RaftError.ENEWLEADER, "Candidate receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        }
        else if (this.leaderId.isEmpty()) {
            status.setError(RaftError.ENEWLEADER, "Follower receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        }
        // save current leader
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            resetLeaderId(serverId, status);
        }
    }

    private void becomeLeader() {
        Requires.requireTrue(this.state == State.STATE_CANDIDATE, "Illegal state: " + this.state);
        LOG.info("Node {} become leader of group, term={}, conf={}, oldConf={}.", getNodeId(), this.currTerm,
            this.conf.getConf(), this.conf.getOldConf());
        // cancel candidate vote timer
        stopVoteTimer();
        this.state = State.STATE_LEADER;
        this.leaderId = this.serverId.copy();
        this.replicatorGroup.resetTerm(this.currTerm);
        // Start follower's replicators
        for (final PeerId peer : this.conf.listPeers()) {
            if (peer.equals(this.serverId)) {
                continue;
            }
            LOG.debug("Node {} add a replicator, term={}, peer={}.", getNodeId(), this.currTerm, peer);
            if (!this.replicatorGroup.addReplicator(peer)) {
                LOG.error("Fail to add a replicator, peer={}.", peer);
            }
        }

        // Start learner's replicators
        for (final PeerId peer : this.conf.listLearners()) {
            LOG.debug("Node {} add a learner replicator, term={}, peer={}.", getNodeId(), this.currTerm, peer);
            if (!this.replicatorGroup.addReplicator(peer, ReplicatorType.Learner)) {
                LOG.error("Fail to add a learner replicator, peer={}.", peer);
            }
        }

        // init commit manager
        this.ballotBox.resetPendingIndex(this.logManager.getLastLogIndex() + 1);
        // Register _conf_ctx to reject configuration changing before the first log
        // is committed.
        if (this.confCtx.isBusy()) {
            throw new IllegalStateException();
        }
        this.confCtx.flush(this.conf.getConf(), this.conf.getOldConf());

        resetElectionTimeoutToInitial();
        this.stepDownTimer.start();
    }

    // should be in writeLock
    private void stepDown(final long term, final boolean wakeupCandidate, final Status status) {
        LOG.debug("Node {} stepDown, term={}, newTerm={}, wakeupCandidate={}.", getNodeId(), this.currTerm, term,
            wakeupCandidate);
        if (!this.state.isActive()) {
            return;
        }
        if (this.state == State.STATE_CANDIDATE) {
            stopVoteTimer();
        }
        else if (this.state.compareTo(State.STATE_TRANSFERRING) <= 0) {
            stopStepDownTimer();
            this.ballotBox.clearPendingTasks();
            // signal fsm leader stop immediately
            if (this.state == State.STATE_LEADER) {
                onLeaderStop(status);
            }
        }
        // reset leader_id
        resetLeaderId(PeerId.emptyPeer(), status);

        // soft state in memory
        this.state = State.STATE_FOLLOWER;
        this.confCtx.reset();
        updateLastLeaderTimestamp(Utils.monotonicMs());
        if (this.snapshotExecutor != null) {
            this.snapshotExecutor.interruptDownloadingSnapshots(term);
        }

        // meta state
        if (term > this.currTerm) {
            this.currTerm = term;
            this.votedId = PeerId.emptyPeer();
            this.metaStorage.setTermAndVotedFor(term, this.votedId);
        }

        if (wakeupCandidate) {
            this.wakingCandidate = this.replicatorGroup.stopAllAndFindTheNextCandidate(this.conf);
            if (this.wakingCandidate != null) {
                Replicator.sendTimeoutNowAndStop(this.wakingCandidate, this.options.getElectionTimeoutMs());
            }
        }
        else {
            this.replicatorGroup.stopAll();
        }
        if (this.stopTransferArg != null) {
            if (this.transferTimer != null) {
                this.transferTimer.cancel(true);
            }
            // There is at most one StopTransferTimer at the same term, it's safe to
            // mark stopTransferArg to NULL
            this.stopTransferArg = null;
        }
        // Learner node will not trigger the election timer.
        if (!isLearner()) {
            this.electionTimer.restart();
        }
        else {
            LOG.info("Node {} is a learner, election timer is not started.", this.nodeId);
        }
    }

    // Should be in readLock
    private boolean isLearner() {
        return this.conf.listLearners().contains(this.serverId);
    }

    private void stopStepDownTimer() {
        if (this.stepDownTimer != null) {
            this.stepDownTimer.stop();
        }
    }

    private void stopVoteTimer() {
        if (this.voteTimer != null) {
            this.voteTimer.stop();
        }
    }

    class LeaderStableClosure extends LogManager.StableClosure {
        LeaderStableClosure(final List<LogEntry> entries) {
            super(entries);
        }

        @Override
        public void run(final Status status) {
            if (status.isOk()) {
                NodeImpl.this.ballotBox.commitAt(this.firstLogIndex, this.firstLogIndex + this.nEntries - 1,
                    NodeImpl.this.serverId);
            }
            else {
                LOG.error("Node {} append [{}, {}] failed, status={}.", getNodeId(), this.firstLogIndex,
                    this.firstLogIndex + this.nEntries - 1, status);
            }
        }
    }

    private void executeApplyingTasks(final List<LogEntryAndClosure> tasks) {
        if (!this.logManager.hasAvailableCapacityToAppendEntries(1)) {
            // It's overload, fail-fast
            final List<Closure> dones = tasks.stream().map(ele -> ele.done).filter(Objects::nonNull)
                     .collect(Collectors.toList());
            Utils.runInThread(this.getOptions().getCommonExecutor(), () -> {
                for (final Closure done : dones) {
                    done.run(new Status(RaftError.EBUSY, "Node %s log manager is busy.", this.getNodeId()));
                }
            });

            return;
        }

        this.writeLock.lock();
        try {
            final int size = tasks.size();
            State nodeState = this.state;
            if (nodeState != State.STATE_LEADER) {
                final Status st = cannotApplyBecauseNotLeaderStatus(nodeState);
                LOG.debug("Node {} can't apply, status={}.", getNodeId(), st);
                final List<Closure> dones = tasks.stream().map(ele -> ele.done)
                        .filter(Objects::nonNull).collect(Collectors.toList());
                Utils.runInThread(this.getOptions().getCommonExecutor(), () -> {
                    for (final Closure done : dones) {
                        done.run(st);
                    }
                });
                return;
            }
            final List<LogEntry> entries = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                final LogEntryAndClosure task = tasks.get(i);
                if (task.expectedTerm != -1 && task.expectedTerm != this.currTerm) {
                    LOG.debug("Node {} can't apply task whose expectedTerm={} doesn't match currTerm={}.", getNodeId(),
                        task.expectedTerm, this.currTerm);
                    if (task.done != null) {
                        final Status st = new Status(RaftError.EPERM, "expected_term=%d doesn't match current_term=%d",
                            task.expectedTerm, this.currTerm);
                        Utils.runClosureInThread(this.getOptions().getCommonExecutor(), task.done, st);
                        task.reset();
                    }
                    continue;
                }
                if (!this.ballotBox.appendPendingTask(this.conf.getConf(),
                    this.conf.isStable() ? null : this.conf.getOldConf(), task.done)) {
                    Utils.runClosureInThread(this.getOptions().getCommonExecutor(), task.done, new Status(RaftError.EINTERNAL, "Fail to append task."));
                    task.reset();
                    continue;
                }
                // set task entry info before adding to list.
                task.entry.getId().setTerm(this.currTerm);
                task.entry.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
                entries.add(task.entry);
                task.reset();
            }
            this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
            // update conf.first
            checkAndSetConfiguration(true);
        }
        finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Builds a status for 'Cannot apply because this node is not a leader' situation.
     *
     * @param nodeState Node state.
     */
    public static Status cannotApplyBecauseNotLeaderStatus(State nodeState) {
        final Status st = new Status();

        if (nodeState != State.STATE_TRANSFERRING) {
            st.setError(RaftError.EPERM, "Is not leader.");
        } else {
            st.setError(RaftError.EBUSY, "Is transferring leadership.");
        }

        return st;
    }

    /**
     * Returns the node metrics.
     *
     * @return returns metrics of current node.
     */
    @Override
    public NodeMetrics getNodeMetrics() {
        return this.metrics;
    }

    /**
     * Returns the JRaft service factory for current node.
     *
     * @return the service factory
     */
    public JRaftServiceFactory getServiceFactory() {
        return this.serviceFactory;
    }

    @Override
    public void readIndex(final byte[] requestContext, final ReadIndexClosure done) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(this.getOptions().getCommonExecutor(), done, new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(done, "Null closure");
        this.readOnlyService.addRequest(requestContext, done);
    }

    /**
     * ReadIndex response closure
     */
    private static class ReadIndexHeartbeatResponseClosure extends RpcResponseClosureAdapter<AppendEntriesResponse> {
        final ReadIndexResponseBuilder respBuilder;
        final RpcResponseClosure<ReadIndexResponse> closure;
        final int quorum;
        final int failPeersThreshold;
        int ackSuccess;
        int ackFailures;
        boolean isDone;

        ReadIndexHeartbeatResponseClosure(final RpcResponseClosure<ReadIndexResponse> closure,
            final ReadIndexResponseBuilder rb, final int quorum,
            final int peersCount) {
            super();
            this.closure = closure;
            this.respBuilder = rb;
            this.quorum = quorum;
            this.failPeersThreshold = peersCount % 2 == 0 ? (quorum - 1) : quorum;
            this.ackSuccess = 0;
            this.ackFailures = 0;
            this.isDone = false;
        }

        @Override
        public synchronized void run(final Status status) {
            if (this.isDone) {
                return;
            }
            if (status.isOk() && getResponse().success()) {
                this.ackSuccess++;
            }
            else {
                this.ackFailures++;
            }
            // Include leader self vote yes.
            if (this.ackSuccess + 1 >= this.quorum) {
                this.respBuilder.success(true);
                this.closure.setResponse(this.respBuilder.build());
                this.closure.run(Status.OK());
                this.isDone = true;
            }
            else if (this.ackFailures >= this.failPeersThreshold) {
                this.respBuilder.success(false);
                this.closure.setResponse(this.respBuilder.build());
                this.closure.run(Status.OK());
                this.isDone = true;
            }
        }
    }

    /**
     * Handle read index request.
     */
    @Override
    public void handleReadIndexRequest(final ReadIndexRequest request,
        final RpcResponseClosure<ReadIndexResponse> done) {
        final long startMs = Utils.monotonicMs();
        this.readLock.lock();
        try {
            switch (this.state) {
                case STATE_LEADER:
                    readLeader(request, done);
                    break;
                case STATE_FOLLOWER:
                    readFollower(request, done);
                    break;
                case STATE_TRANSFERRING:
                    done.run(new Status(RaftError.EBUSY, "Is transferring leadership."));
                    break;
                default:
                    done.run(new Status(RaftError.EPERM, "Invalid state for readIndex: %s.", this.state));
                    break;
            }
        }
        finally {
            this.readLock.unlock();
            this.metrics.recordLatency("handle-read-index", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-read-index-entries", Utils.size(request.entriesList()));
        }
    }

    private int getQuorum() {
        final Configuration c = this.conf.getConf();
        if (c.isEmpty()) {
            return 0;
        }
        return c.getPeers().size() / 2 + 1;
    }

    private void readFollower(final ReadIndexRequest request, final RpcResponseClosure<ReadIndexResponse> closure) {
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            closure.run(new Status(RaftError.EPERM, "No leader at term %d.", this.currTerm));
            return;
        }
        // send request to leader.
        final ReadIndexRequest newRequest = raftOptions.getRaftMessagesFactory()
            .readIndexRequest()
            .groupId(request.groupId())
            .serverId(request.serverId())
            .peerId(request.peerId())
            .entriesList(request.entriesList())
            .peerId(this.leaderId.toString())
            .build();
        this.rpcClientService.readIndex(this.leaderId, newRequest, -1, closure);
    }

    private void readLeader(ReadIndexRequest request, RpcResponseClosure<ReadIndexResponse> closure) {
        ReadIndexResponseBuilder respBuilder = raftOptions.getRaftMessagesFactory().readIndexResponse();

        final int quorum = getQuorum();
        if (quorum <= 1) {
            // Only one peer, fast path.
            respBuilder
                .success(true)
                .index(this.ballotBox.getLastCommittedIndex());
            closure.setResponse(respBuilder.build());
            closure.run(Status.OK());
            return;
        }

        final long lastCommittedIndex = this.ballotBox.getLastCommittedIndex();
        if (this.logManager.getTerm(lastCommittedIndex) != this.currTerm) {
            // Reject read only request when this leader has not committed any log entry at its term
            closure.run(new Status(
                RaftError.EAGAIN,
                "ReadIndex request rejected because leader has not committed any log entry at its term, logIndex=%d, currTerm=%d.",
                lastCommittedIndex, this.currTerm));
            return;
        }
        respBuilder.index(lastCommittedIndex);

        if (request.peerId() != null) {
            // request from follower or learner, check if the follower/learner is in current conf.
            final PeerId peer = new PeerId();
            peer.parse(request.serverId());
            if (!this.conf.contains(peer) && !this.conf.containsLearner(peer)) {
                closure
                    .run(new Status(RaftError.EPERM, "Peer %s is not in current configuration: %s.", peer, this.conf));
                return;
            }
        }

        ReadOnlyOption readOnlyOpt = this.raftOptions.getReadOnlyOptions();
        if (readOnlyOpt == ReadOnlyOption.ReadOnlyLeaseBased && !isLeaderLeaseValid()) {
            // If leader lease timeout, we must change option to ReadOnlySafe
            readOnlyOpt = ReadOnlyOption.ReadOnlySafe;
        }

        switch (readOnlyOpt) {
            case ReadOnlySafe:
                final List<PeerId> peers = this.conf.getConf().getPeers();
                Requires.requireTrue(peers != null && !peers.isEmpty(), "Empty peers");
                final ReadIndexHeartbeatResponseClosure heartbeatDone = new ReadIndexHeartbeatResponseClosure(closure,
                    respBuilder, quorum, peers.size());
                // Send heartbeat requests to followers
                for (final PeerId peer : peers) {
                    if (peer.equals(this.serverId)) {
                        continue;
                    }
                    this.replicatorGroup.sendHeartbeat(peer, heartbeatDone);
                }
                break;
            case ReadOnlyLeaseBased:
                // Responses to followers and local node.
                respBuilder.success(true);
                closure.setResponse(respBuilder.build());
                closure.run(Status.OK());
                break;
        }
    }

    @Override
    public void apply(final Task task) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(this.getOptions().getCommonExecutor(), task.getDone(), new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(task, "Null task");

        final LogEntry entry = new LogEntry();
        entry.setData(task.getData());

        final EventTranslator<LogEntryAndClosure> translator = (event, sequence) -> {
            event.reset();

            event.nodeId = getNodeId();
            event.done = task.getDone();
            event.entry = entry;
            event.expectedTerm = task.getExpectedTerm();
        };
        switch (this.options.getApplyTaskMode()) {
            case Blocking:
                this.applyQueue.publishEvent(translator);
                break;
            case NonBlocking:
            default:
                if (!this.applyQueue.tryPublishEvent(translator)) {
                    String errorMsg = "Node is busy, has too many tasks, queue is full and bufferSize="+ this.applyQueue.getBufferSize();
                    Utils.runClosureInThread(this.getOptions().getCommonExecutor(), task.getDone(), new Status(RaftError.EBUSY, errorMsg));
                    LOG.warn("Node {} applyQueue is overload.", getNodeId());
                    this.metrics.recordTimes("apply-task-overload-times", 1);
                    if (task.getDone() == null) {
                        throw new OverloadException(errorMsg);
                    }
            }
            break;
        }
    }

    @Override
    public Message handlePreVoteRequest(final RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RaftRpcFactory.DEFAULT //
                    .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL,
                        "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }
            final PeerId candidateId = new PeerId();
            if (!candidateId.parse(request.serverId())) {
                LOG.warn("Node {} received PreVoteRequest from {} serverId bad format.", getNodeId(),
                    request.serverId());
                return RaftRpcFactory.DEFAULT //
                    .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL,
                        "Parse candidateId failed: %s.", request.serverId());
            }
            boolean granted = false;
            //noinspection ConstantConditions
            do {
                if (!this.conf.contains(candidateId)) {
                    LOG.warn("Node {} ignore PreVoteRequest from {} as it is not in conf <{}>.", getNodeId(),
                        request.serverId(), this.conf);
                    break;
                }
                if (this.leaderId != null && !this.leaderId.isEmpty() && isCurrentLeaderValid()) {
                    LOG.info(
                        "Node {} ignore PreVoteRequest from {}, term={}, currTerm={}, because the leader {}'s lease is still valid.",
                        getNodeId(), request.serverId(), request.term(), this.currTerm, this.leaderId);
                    break;
                }
                if (request.term() < this.currTerm) {
                    LOG.info("Node {} ignore PreVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.serverId(), request.term(), this.currTerm);
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    checkReplicator(candidateId);
                    break;
                }
                // A follower replicator may not be started when this node become leader, so we must check it.
                // check replicator state
                checkReplicator(candidateId);

                doUnlock = false;
                this.writeLock.unlock();

                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                final LogId requestLastLogId = new LogId(request.lastLogIndex(), request.lastLogTerm());
                granted = requestLastLogId.compareTo(lastLogId) >= 0;

                LOG.info(
                    "Node {} received PreVoteRequest from {}, term={}, currTerm={}, granted={}, requestLastLogId={}, lastLogId={}.",
                    getNodeId(), request.serverId(), request.term(), this.currTerm, granted, requestLastLogId,
                    lastLogId);
            }
            while (false);

            return raftOptions.getRaftMessagesFactory()
                .requestVoteResponse()
                .term(this.currTerm)
                .granted(granted)
                .build();
        }
        finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    // in read_lock
    private boolean isLeaderLeaseValid() {
        final long monotonicNowMs = Utils.monotonicMs();
        // Test with a current start lease timestamp.
        if (checkLeaderLease(monotonicNowMs)) {
            return true;
        }
        // Refresh start lease timestamp and try again.
        checkDeadNodes0(this.conf.getConf().getPeers(), monotonicNowMs, false, null);
        return checkLeaderLease(monotonicNowMs);
    }

    private boolean checkLeaderLease(final long monotonicNowMs) {
        return monotonicNowMs - this.lastLeaderTimestamp < this.options.getLeaderLeaseTimeoutMs();
    }

    private boolean isCurrentLeaderValid() {
        return checkLeaderLease(Utils.monotonicMs());
    }

    private void updateLastLeaderTimestamp(final long lastLeaderTimestamp) {
        this.lastLeaderTimestamp = lastLeaderTimestamp;
    }

    private void checkReplicator(final PeerId candidateId) {
        if (this.state == State.STATE_LEADER) {
            this.replicatorGroup.checkReplicator(candidateId, false);
        }
    }

    @Override
    public Message handleRequestVoteRequest(final RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RaftRpcFactory.DEFAULT //
                    .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL,
                        "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }
            final PeerId candidateId = new PeerId();
            if (!candidateId.parse(request.serverId())) {
                LOG.warn("Node {} received RequestVoteRequest from {} serverId bad format.", getNodeId(),
                    request.serverId());
                return RaftRpcFactory.DEFAULT //
                    .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL,
                        "Parse candidateId failed: %s.", request.serverId());
            }

            //noinspection ConstantConditions
            do {
                // check term
                if (request.term() >= this.currTerm) {
                    LOG.info("Node {} received RequestVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.serverId(), request.term(), this.currTerm);
                    // increase current term, change state to follower
                    if (request.term() > this.currTerm) {
                        stepDown(request.term(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                            "Raft node receives higher term RequestVoteRequest."));
                    }
                    else if (candidateId.equals(leaderId)) { // Already follows a leader in this term.
                        LOG.info("Node {} ignores RequestVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                            request.serverId(), request.term(), this.currTerm);

                        break;
                    }
                }
                else {
                    // ignore older term
                    LOG.info("Node {} ignores RequestVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.serverId(), request.term(), this.currTerm);

                    break;
                }

                doUnlock = false;
                this.writeLock.unlock();

                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();

                // vote need ABA check after unlock&writeLock
                if (request.term() != this.currTerm) {
                    LOG.warn("Node {} raise term {} when get lastLogId.", getNodeId(), this.currTerm);
                    break;
                }

                final boolean logIsOk = new LogId(request.lastLogIndex(), request.lastLogTerm())
                    .compareTo(lastLogId) >= 0;

                if (logIsOk && (this.votedId == null || this.votedId.isEmpty())) {
                    stepDown(request.term(), false, new Status(RaftError.EVOTEFORCANDIDATE,
                        "Raft node votes for some candidate, step down to restart election_timer."));
                    this.votedId = candidateId.copy();
                    this.metaStorage.setVotedFor(candidateId);
                }
            }
            while (false);

            return raftOptions.getRaftMessagesFactory()
                .requestVoteResponse()
                .term(this.currTerm)
                .granted(request.term() == this.currTerm && candidateId.equals(this.votedId))
                .build();
        }
        finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private static class FollowerStableClosure extends LogManager.StableClosure {
        final long committedIndex;
        final AppendEntriesResponseBuilder responseBuilder;
        final NodeImpl node;
        final RpcRequestClosure done;
        final long term;

        FollowerStableClosure(final AppendEntriesRequest request,
            final AppendEntriesResponseBuilder responseBuilder, final NodeImpl node,
            final RpcRequestClosure done, final long term) {
            super(null);
            this.committedIndex = Math.min(
                // committed index is likely less than the lastLogIndex
                request.committedIndex(),
                // The logs after the appended entries can not be trust, so we can't commit them even if their indexes are less than request's committed index.
                request.prevLogIndex() + Utils.size(request.entriesList()));
            this.responseBuilder = responseBuilder;
            this.node = node;
            this.done = done;
            this.term = term;
        }

        @Override
        public void run(final Status status) {
            if (!status.isOk()) {
                this.done.run(status);
                return;
            }

            this.node.readLock.lock();
            try {
                if (this.term != this.node.currTerm) {
                    // The change of term indicates that leader has been changed during
                    // appending entries, so we can't respond ok to the old leader
                    // because we are not sure if the appended logs would be truncated
                    // by the new leader:
                    //  - If they won't be truncated and we respond failure to the old
                    //    leader, the new leader would know that they are stored in this
                    //    peer and they will be eventually committed when the new leader
                    //    found that quorum of the cluster have stored.
                    //  - If they will be truncated and we responded success to the old
                    //    leader, the old leader would possibly regard those entries as
                    //    committed (very likely in a 3-nodes cluster) and respond
                    //    success to the clients, which would break the rule that
                    //    committed entries would never be truncated.
                    // So we have to respond failure to the old leader and set the new
                    // term to make it stepped down if it didn't.
                    // TODO asch make test scenario https://issues.apache.org/jira/browse/IGNITE-14832
                    this.responseBuilder.success(false).term(this.node.currTerm);
                    this.done.sendResponse(this.responseBuilder.build());
                    return;
                }
            }
            finally {
                // It's safe to release lock as we know everything is ok at this point.
                this.node.readLock.unlock();
            }

            // Don't touch node any more.
            this.responseBuilder.success(true).term(this.term);

            // Ballot box is thread safe and tolerates disorder.
            this.node.ballotBox.setLastCommittedIndex(this.committedIndex);

            this.done.sendResponse(this.responseBuilder.build());
        }
    }

    @Override
    public Message handleAppendEntriesRequest(final AppendEntriesRequest request, final RpcRequestClosure done) {
        boolean doUnlock = true;
        final long startMs = Utils.monotonicMs();
        this.writeLock.lock();
        final int entriesCount = Utils.size(request.entriesList());
        boolean success = false;
        try {
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RaftRpcFactory.DEFAULT //
                    .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL,
                        "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }

            final PeerId serverId = new PeerId();
            if (!serverId.parse(request.serverId())) {
                LOG.warn("Node {} received AppendEntriesRequest from {} serverId bad format.", getNodeId(),
                    request.serverId());
                return RaftRpcFactory.DEFAULT //
                    .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL,
                        "Parse serverId failed: %s.", request.serverId());
            }

            // Check stale term
            if (request.term() < this.currTerm) {
                LOG.warn("Node {} ignore stale AppendEntriesRequest from {}, term={}, currTerm={}.", getNodeId(),
                    request.serverId(), request.term(), this.currTerm);
                AppendEntriesResponseBuilder rb = raftOptions.getRaftMessagesFactory()
                        .appendEntriesResponse()
                        .success(false)
                        .term(this.currTerm);

                if (request.timestamp() != null) {
                    rb.timestamp(clock.update(request.timestamp()));
                }

                return rb.build();
            }

            // Check term and state to step down
            checkStepDown(request.term(), serverId);
            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                    serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                stepDown(request.term() + 1, false, new Status(RaftError.ELEADERCONFLICT,
                    "More than one leader in the same term."));
                AppendEntriesResponseBuilder rb = raftOptions.getRaftMessagesFactory()
                        .appendEntriesResponse()
                        .success(false) //
                        .term(request.term() + 1);

                if (request.timestamp() != null) {
                    rb.timestamp(clock.update(request.timestamp()));
                }

                return rb.build();
            }

            updateLastLeaderTimestamp(Utils.monotonicMs());

            if (entriesCount > 0 && this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn("Node {} received AppendEntriesRequest while installing snapshot.", getNodeId());
                return RaftRpcFactory.DEFAULT //
                    .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EBUSY,
                        "Node %s:%s is installing snapshot.", this.groupId, this.serverId);
            }

            final long prevLogIndex = request.prevLogIndex();
            final long prevLogTerm = request.prevLogTerm();
            final long localPrevLogTerm = this.logManager.getTerm(prevLogIndex);
            if (localPrevLogTerm != prevLogTerm) {
                final long lastLogIndex = this.logManager.getLastLogIndex();

                LOG.warn("Node {} reject term_unmatched AppendEntriesRequest from {}, term={}, prevLogIndex={}, " +
                        "prevLogTerm={}, localPrevLogTerm={}, lastLogIndex={}, entriesSize={}.",
                    getNodeId(), request.serverId(), request.term(), prevLogIndex, prevLogTerm, localPrevLogTerm,
                    lastLogIndex, entriesCount);

                AppendEntriesResponseBuilder rb = raftOptions.getRaftMessagesFactory()
                        .appendEntriesResponse()
                        .success(false)
                        .term(this.currTerm)
                        .lastLogIndex(lastLogIndex);

                if (request.timestamp() != null) {
                    rb.timestamp(clock.update(request.timestamp()));
                }

                return rb.build();
            }

            if (entriesCount == 0) {
                // heartbeat or probe request
                final AppendEntriesResponseBuilder respBuilder = raftOptions.getRaftMessagesFactory()
                    .appendEntriesResponse()
                    .success(true)
                    .term(this.currTerm)
                    .lastLogIndex(this.logManager.getLastLogIndex());
                if (request.timestamp() != null) {
                    respBuilder.timestamp(clock.update(request.timestamp()));
                }
                doUnlock = false;
                this.writeLock.unlock();
                // see the comments at FollowerStableClosure#run()
                this.ballotBox.setLastCommittedIndex(Math.min(request.committedIndex(), prevLogIndex));
                return respBuilder.build();
            }

            // fast checking if log manager is overloaded
            if (!this.logManager.hasAvailableCapacityToAppendEntries(1)) {
                LOG.warn("Node {} received AppendEntriesRequest but log manager is busy.", getNodeId());

                AppendEntriesResponseBuilder rb = raftOptions.getRaftMessagesFactory()
                        .appendEntriesResponse()
                        .success(false)
                        .errorCode(RaftError.EBUSY.getNumber())
                        .errorMsg(String.format("Node %s:%s log manager is busy.", this.groupId, this.serverId))
                        .term(this.currTerm);

                if (request.timestamp() != null) {
                    rb.timestamp(clock.update(request.timestamp()));
                }

                return rb.build();
            }

            // Parse request
            long index = prevLogIndex;
            final List<LogEntry> entries = new ArrayList<>(entriesCount);
            ByteBuffer allData = request.data() != null ? request.data().asReadOnlyBuffer() : EMPTY_BYTE_BUFFER.asReadOnlyBuffer();

            final Collection<RaftOutter.EntryMeta> entriesList = request.entriesList();
            for (RaftOutter.EntryMeta entry : entriesList) {
                index++;

                final LogEntry logEntry = logEntryFromMeta(index, allData, entry);

                if (logEntry != null) {
                    // Validate checksum
                    if (this.raftOptions.isEnableLogEntryChecksum() && logEntry.isCorrupted()) {
                        long realChecksum = logEntry.checksum();
                        LOG.error(
                            "Corrupted log entry received from leader, index={}, term={}, expectedChecksum={}, realChecksum={}",
                            logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(),
                            realChecksum);
                        return RaftRpcFactory.DEFAULT //
                            .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL,
                                "The log entry is corrupted, index=%d, term=%d, expectedChecksum=%d, realChecksum=%d",
                                logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(),
                                realChecksum);
                    }
                    entries.add(logEntry);
                }
            }

            final FollowerStableClosure closure = new FollowerStableClosure(
                request,
                raftOptions.getRaftMessagesFactory().appendEntriesResponse().term(this.currTerm),
                this,
                done,
                this.currTerm
            );
            this.logManager.appendEntries(entries, closure);
            // update configuration after _log_manager updated its memory status
            checkAndSetConfiguration(true);
            success = true;
            return null;
        }
        finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
            final long processLatency = Utils.monotonicMs() - startMs;
            if (entriesCount == 0) {
                this.metrics.recordLatency("handle-heartbeat-requests", processLatency);
            } else {
                this.metrics.recordLatency("handle-append-entries", processLatency);
            }
            if (success) {
                // Don't stats heartbeat requests.
                this.metrics.recordSize("handle-append-entries-count", entriesCount);
            }
        }
    }

    private LogEntry logEntryFromMeta(final long index, final ByteBuffer allData, final RaftOutter.EntryMeta entry) {
        if (entry.type() != EnumOutter.EntryType.ENTRY_TYPE_UNKNOWN) {
            final LogEntry logEntry = new LogEntry();
            logEntry.setId(new LogId(index, entry.term()));
            logEntry.setType(entry.type());

            if (entry.hasChecksum())
                logEntry.setChecksum(entry.checksum()); // since 1.2.6

            final long dataLen = entry.dataLen();
            if (dataLen > 0) {
                final byte[] bs = new byte[(int) dataLen];
                assert allData != null;
                allData.get(bs, 0, bs.length);
                logEntry.setData(ByteBuffer.wrap(bs));
            }

            if (entry.peersList() != null) {
                if (entry.type() != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                    throw new IllegalStateException(
                        "Invalid log entry that contains peers but is not ENTRY_TYPE_CONFIGURATION type: "
                            + entry.type());
                }

                fillLogEntryPeers(entry, logEntry);
            }
            else if (entry.type() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                throw new IllegalStateException(
                    "Invalid log entry that contains zero peers but is ENTRY_TYPE_CONFIGURATION type");
            }
            return logEntry;
        }
        return null;
    }

    private void fillLogEntryPeers(final RaftOutter.EntryMeta entry, final LogEntry logEntry) {
        // TODO refactor https://issues.apache.org/jira/browse/IGNITE-14832
        if (entry.peersList() != null) {
            final List<PeerId> peers = new ArrayList<>();
            for (final String peerStr : entry.peersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                peers.add(peer);
            }
            logEntry.setPeers(peers);
        }

        if (entry.oldPeersList() != null) {
            final List<PeerId> oldPeers = new ArrayList<>();
            for (final String peerStr : entry.oldPeersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                oldPeers.add(peer);
            }
            logEntry.setOldPeers(oldPeers);
        }

        if (entry.learnersList() != null) {
            final List<PeerId> peers = new ArrayList<>();
            for (final String peerStr : entry.learnersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                peers.add(peer);
            }
            logEntry.setLearners(peers);
        }

        if (entry.oldLearnersList() != null) {
            final List<PeerId> peers = new ArrayList<>();
            for (final String peerStr : entry.oldLearnersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                peers.add(peer);
            }
            logEntry.setOldLearners(peers);
        }
    }

    // called when leader receive greater term in AppendEntriesResponse
    void increaseTermTo(final long newTerm, final Status status) {
        this.writeLock.lock();
        try {
            if (newTerm < this.currTerm) {
                return;
            }
            stepDown(newTerm, false, status);
        }
        finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Peer catch up callback
     */
    private static class OnCaughtUp extends CatchUpClosure {
        private final NodeImpl node;
        private final long term;
        private final PeerId peer;
        private final long version;

        OnCaughtUp(final NodeImpl node, final long term, final PeerId peer, final long version) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
            this.version = version;
        }

        @Override
        public void run(final Status status) {
            this.node.onCaughtUp(this.peer, this.term, this.version, status);
        }
    }

    private void onCaughtUp(final PeerId peer, final long term, final long version, final Status st) {
        this.writeLock.lock();
        try {
            // check current_term and state to avoid ABA problem
            if (term != this.currTerm && this.state != State.STATE_LEADER) {
                // term has changed and nothing should be done, otherwise there will be
                // an ABA problem.
                return;
            }
            if (st.isOk()) {
                // Caught up successfully
                this.confCtx.onCaughtUp(version, peer, true);
                return;
            }
            // Retry if this peer is still alive
            if (st.getCode() == RaftError.ETIMEDOUT.getNumber()
                && Utils.monotonicMs() - this.replicatorGroup.getLastRpcSendTimestamp(peer) <= this.options
                .getElectionTimeoutMs()) {
                LOG.debug("Node {} waits peer {} to catch up.", getNodeId(), peer);
                final OnCaughtUp caughtUp = new OnCaughtUp(this, term, peer, version);
                final long dueTime = Utils.nowMs() + this.options.getElectionTimeoutMs();
                if (this.replicatorGroup.waitCaughtUp(peer, this.options.getCatchupMargin(), dueTime, caughtUp)) {
                    return;
                }
                LOG.warn("Node {} waitCaughtUp failed, peer={}.", getNodeId(), peer);
            }
            LOG.warn("Node {} caughtUp failed, status={}, peer={}.", getNodeId(), st, peer);
            this.confCtx.onCaughtUp(version, peer, false);
        }
        finally {
            this.writeLock.unlock();
        }
    }

    /**
     * @param conf The configuration.
     * @param monotonicNowMs The timestamp.
     * @param stepDownOnCheckFail {@code True} to step down on check fail.
     * @return {@code True} if a majority of peers are alive.
     */
    private boolean checkDeadNodes(final Configuration conf, final long monotonicNowMs,
        final boolean stepDownOnCheckFail) {
        // Check learner replicators at first.
        for (final PeerId peer : conf.getLearners()) {
            checkReplicator(peer);
        }
        // Ensure quorum nodes alive.
        final List<PeerId> peers = conf.listPeers();
        final Configuration deadNodes = new Configuration();
        if (checkDeadNodes0(peers, monotonicNowMs, true, deadNodes)) {
            return true;
        }
        if (stepDownOnCheckFail) {
            LOG.warn("Node {} steps down when alive nodes don't satisfy quorum, term={}, deadNodes={}, conf={}.",
                getNodeId(), this.currTerm, deadNodes, conf);
            final Status status = new Status();
            status.setError(RaftError.ERAFTTIMEDOUT, "Majority of the group dies: %d/%d", deadNodes.size(),
                peers.size());
            stepDown(this.currTerm, false, status);
        }
        return false;
    }

    /**
     * @param peers Peers list.
     * @param monotonicNowMs The timestamp.
     * @param checkReplicator {@code True} to check replicator.
     * @param deadNodes The configuration.
     * @return {@code True} if a majority of nodes are alive.
     */
    private boolean checkDeadNodes0(final List<PeerId> peers, final long monotonicNowMs, final boolean checkReplicator,
        final Configuration deadNodes) {
        final int leaderLeaseTimeoutMs = this.options.getLeaderLeaseTimeoutMs();
        int aliveCount = 0;
        long startLease = Long.MAX_VALUE;
        for (final PeerId peer : peers) {
            if (peer.equals(this.serverId)) {
                aliveCount++;
                continue;
            }
            if (checkReplicator) {
                checkReplicator(peer);
            }
            final long lastRpcSendTimestamp = this.replicatorGroup.getLastRpcSendTimestamp(peer);
            if (monotonicNowMs - lastRpcSendTimestamp <= leaderLeaseTimeoutMs) {
                aliveCount++;
                if (startLease > lastRpcSendTimestamp) {
                    startLease = lastRpcSendTimestamp;
                }
                continue;
            }
            if (deadNodes != null) {
                deadNodes.addPeer(peer);
            }
        }
        if (aliveCount >= peers.size() / 2 + 1) {
            updateLastLeaderTimestamp(startLease);
            return true;
        }
        return false;
    }

    // in read_lock
    private List<PeerId> getAliveNodes(final Collection<PeerId> peers, final long monotonicNowMs) {
        final int leaderLeaseTimeoutMs = this.options.getLeaderLeaseTimeoutMs();
        final List<PeerId> alivePeers = new ArrayList<>();
        for (final PeerId peer : peers) {
            if (peer.equals(this.serverId)) {
                alivePeers.add(peer.copy());
                continue;
            }
            if (monotonicNowMs - this.replicatorGroup.getLastRpcSendTimestamp(peer) <= leaderLeaseTimeoutMs) {
                alivePeers.add(peer.copy());
            }
        }
        return alivePeers;
    }

    @SuppressWarnings({"LoopStatementThatDoesntLoop", "ConstantConditions"})
    private void handleStepDownTimeout() {
        do {
            this.readLock.lock();
            try {
                if (this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                    LOG.debug("Node {} stop step-down timer, term={}, state={}.", getNodeId(), this.currTerm,
                        this.state);
                    return;
                }
                final long monotonicNowMs = Utils.monotonicMs();
                if (!checkDeadNodes(this.conf.getConf(), monotonicNowMs, false)) {
                    break;
                }
                if (!this.conf.getOldConf().isEmpty()) {
                    if (!checkDeadNodes(this.conf.getOldConf(), monotonicNowMs, false)) {
                        break;
                    }
                }
                return;
            }
            finally {
                this.readLock.unlock();
            }
        }
        while (false);

        this.writeLock.lock();
        try {
            if (this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.debug("Node {} stop step-down timer, term={}, state={}.", getNodeId(), this.currTerm, this.state);
                return;
            }
            final long monotonicNowMs = Utils.monotonicMs();
            checkDeadNodes(this.conf.getConf(), monotonicNowMs, true);
            if (!this.conf.getOldConf().isEmpty()) {
                checkDeadNodes(this.conf.getOldConf(), monotonicNowMs, true);
            }
        }
        finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Configuration changed callback.
     */
    private class ConfigurationChangeDone implements Closure {
        private final long term;
        private final boolean leaderStart;

        ConfigurationChangeDone(final long term, final boolean leaderStart) {
            super();
            this.term = term;
            this.leaderStart = leaderStart;
        }

        @Override
        public void run(final Status status) {
            if (status.isOk()) {
                onConfigurationChangeDone(this.term);
                if (this.leaderStart) {
                    if (getOptions().getRaftGrpEvtsLsnr() != null) {
                        options.getRaftGrpEvtsLsnr().onLeaderElected(term);
                    }
                    getOptions().getFsm().onLeaderStart(this.term);
                }
            }
            else {
                LOG.error("Fail to run ConfigurationChangeDone, status: {}.", status);
            }
        }
    }

    private void unsafeApplyConfiguration(final Configuration newConf, final Configuration oldConf,
        final boolean leaderStart) {
        Requires.requireTrue(this.confCtx.isBusy(), "ConfigurationContext is not busy");
        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.setId(new LogId(0, this.currTerm));
        entry.setPeers(newConf.listPeers());
        entry.setLearners(newConf.listLearners());
        if (oldConf != null) {
            entry.setOldPeers(oldConf.listPeers());
            entry.setOldLearners(oldConf.listLearners());
        }
        final ConfigurationChangeDone configurationChangeDone = new ConfigurationChangeDone(this.currTerm, leaderStart);
        // Use the new_conf to deal the quorum of this very log
        if (!this.ballotBox.appendPendingTask(newConf, oldConf, configurationChangeDone)) {
            Utils.runClosureInThread(this.getOptions().getCommonExecutor(), configurationChangeDone, new Status(RaftError.EINTERNAL, "Fail to append task."));
            return;
        }
        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);
        this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
        checkAndSetConfiguration(false);
    }

    private void unsafeRegisterConfChange(final Configuration oldConf, final Configuration newConf, final Closure done) {
        unsafeRegisterConfChange(oldConf, newConf, done, false);
    }

    private void unsafeRegisterConfChange(final Configuration oldConf, final Configuration newConf,
        final Closure done, boolean async) {

        Requires.requireTrue(newConf.isValid(), "Invalid new conf: %s", newConf);
        // The new conf entry(will be stored in log manager) should be valid
        Requires.requireTrue(new ConfigurationEntry(null, newConf, oldConf).isValid(), "Invalid conf entry: %s",
            newConf);

        if (this.state != State.STATE_LEADER) {
            LOG.warn("Node {} refused configuration changing as the state={}.", getNodeId(), this.state);
            if (done != null) {
                final Status status = new Status();
                if (this.state == State.STATE_TRANSFERRING) {
                    status.setError(RaftError.EBUSY, "Is transferring leadership.");
                }
                else {
                    status.setError(RaftError.EPERM, "Not leader");
                }
                Utils.runClosureInThread(this.getOptions().getCommonExecutor(), done, status);
            }
            return;
        }
        // check concurrent conf change
        if (this.confCtx.isBusy()) {
            LOG.warn("Node {} refused configuration concurrent changing.", getNodeId());
            if (done != null) {
                Utils.runClosureInThread(this.getOptions().getCommonExecutor(), done, new Status(RaftError.EBUSY, "Doing another configuration change."));
            }
            return;
        }
        // Return immediately when the new peers equals to the current configuration
        if (this.conf.getConf().equals(newConf)) {
            Closure newDone = (Status status) -> {
                // doOnNewPeersConfigurationApplied should be called, otherwise we could lose the callback invocation.
                // For example, old leader failed just before an invocation of doOnNewPeersConfigurationApplied
                JraftGroupEventsListener listener = this.getOptions().getRaftGrpEvtsLsnr();

                if (listener != null) {
                    listener.onNewPeersConfigurationApplied(newConf.getPeers(), newConf.getLearners());
                }

                done.run(status);
            };
            Utils.runClosureInThread(this.getOptions().getCommonExecutor(), newDone);
            return;
        }
        this.confCtx.start(oldConf, newConf, done, async);
    }

    private void afterShutdown() {
        this.writeLock.lock();
        try {
            if (this.logStorage != null) {
                this.logStorage.shutdown();
            }
            this.state = State.STATE_SHUTDOWN;
        }
        finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public NodeOptions getOptions() {
        return this.options;
    }

    @Override
    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    @Override
    public long getCurrentTerm() {
        this.readLock.lock();
        try {
            return this.currTerm;
        }
        finally {
            this.readLock.unlock();
        }
    }

    @Override
    public boolean isInstallingSnapshot() {
        this.readLock.lock();
        try {
            return snapshotExecutor.isInstallingSnapshot();
        }
        finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long lastLogIndex() {
        return lastLogIndexAndTerm().getIndex();
    }

    @Override
    public LogId lastLogIndexAndTerm() {
        this.readLock.lock();
        try {
            return logManager.getLastLogId(false).copy();
        }
        finally {
            this.readLock.unlock();
        }
    }

    @OnlyForTest
    ConfigurationEntry getConf() {
        this.readLock.lock();
        try {
            return this.conf;
        }
        finally {
            this.readLock.unlock();
        }
    }

    public void onConfigurationChangeDone(final long term) {
        this.writeLock.lock();
        try {
            if (term != this.currTerm || this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.warn("Node {} process onConfigurationChangeDone at term {} while state={}, currTerm={}.",
                    getNodeId(), term, this.state, this.currTerm);
                return;
            }
            this.confCtx.nextStage();
        }
        finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public PeerId getLeaderId() {
        this.readLock.lock();
        try {
            return this.leaderId.isEmpty() ? null : this.leaderId;
        }
        finally {
            this.readLock.unlock();
        }
    }

    @Override
    public String getGroupId() {
        return this.groupId;
    }

    public PeerId getServerId() {
        return this.serverId;
    }

    @Override
    public NodeId getNodeId() {
        if (this.nodeId == null) {
            this.nodeId = new NodeId(this.groupId, this.serverId);
        }
        return this.nodeId;
    }

    public RaftClientService getRpcClientService() {
        return this.rpcClientService;
    }

    public void onError(final RaftException error) {
        LOG.warn("Node {} got error: {}.", getNodeId(), (Object)error);
        if (this.fsmCaller != null) {
            // onError of fsmCaller is guaranteed to be executed once.
            this.fsmCaller.onError(error);
        }
        if (this.readOnlyService != null) {
            this.readOnlyService.setError(error);
        }
        this.writeLock.lock();
        try {
            // If it is leader, need to wake up a new one;
            // If it is follower, also step down to call on_stop_following.
            if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                stepDown(this.currTerm, this.state == State.STATE_LEADER, new Status(RaftError.EBADNODE,
                    "Raft node(leader or candidate) is in error."));
            }
            if (this.state.compareTo(State.STATE_ERROR) < 0) {
                this.state = State.STATE_ERROR;
            }
        }
        finally {
            this.writeLock.unlock();
        }
    }

    public void handleRequestVoteResponse(final PeerId peerId, final long term, final RequestVoteResponse response) {
        this.writeLock.lock();
        try {
            if (this.state != State.STATE_CANDIDATE) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}, state not in STATE_CANDIDATE but {}.",
                    getNodeId(), peerId, this.state);
                return;
            }
            // check stale term
            if (term != this.currTerm) {
                LOG.warn("Node {} received stale RequestVoteResponse from {}, term={}, currTerm={}.", getNodeId(),
                    peerId, term, this.currTerm);
                return;
            }
            // check response term
            if (response.term() > this.currTerm) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}, term={}, expect={}.", getNodeId(),
                    peerId, response.term(), this.currTerm);
                stepDown(response.term(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Raft node receives higher term request_vote_response."));
                return;
            }
            // check granted quorum?
            if (response.granted()) {
                this.voteCtx.grant(peerId);
                if (this.voteCtx.isGranted()) {
                    becomeLeader();
                }
            }
        }
        finally {
            this.writeLock.unlock();
        }
    }

    private class OnRequestVoteRpcDone extends RpcResponseClosureAdapter<RequestVoteResponse> {

        final long startMs;
        final PeerId peer;
        final long term;
        final NodeImpl node;
        RequestVoteRequest request;

        OnRequestVoteRpcDone(final PeerId peer, final long term, final NodeImpl node) {
            super();
            this.startMs = Utils.monotonicMs();
            this.peer = peer;
            this.term = term;
            this.node = node;
        }

        @Override
        public void run(final Status status) {
            NodeImpl.this.metrics.recordLatency("request-vote", Utils.monotonicMs() - this.startMs);
            if (!status.isOk()) {
                LOG.warn("Node {} RequestVote to {} error: {}.", this.node.getNodeId(), this.peer, status);
            }
            else {
                this.node.handleRequestVoteResponse(this.peer, this.term, getResponse());
            }
        }
    }

    public void handlePreVoteResponse(final PeerId peerId, final long term, final RequestVoteResponse response) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (this.state != State.STATE_FOLLOWER) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, state not in STATE_FOLLOWER but {}.",
                    getNodeId(), peerId, this.state);
                return;
            }
            if (term != this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term={}, currTerm={}.", getNodeId(),
                    peerId, term, this.currTerm);
                return;
            }
            if (response.term() > this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term {}, expect={}.", getNodeId(), peerId,
                    response.term(), this.currTerm);
                stepDown(response.term(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Raft node receives higher term pre_vote_response."));
                return;
            }
            LOG.info("Node {} received PreVoteResponse from {}, term={}, granted={}.", getNodeId(), peerId,
                response.term(), response.granted());
            // check granted quorum?
            if (response.granted()) {
                this.prevVoteCtx.grant(peerId);
                if (this.prevVoteCtx.isGranted()) {
                    doUnlock = false;
                    electSelf();
                }
            }
        }
        finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private class OnPreVoteRpcDone extends RpcResponseClosureAdapter<RequestVoteResponse> {

        final long startMs;
        final PeerId peer;
        final long term;
        RequestVoteRequest request;

        OnPreVoteRpcDone(final PeerId peer, final long term) {
            super();
            this.startMs = Utils.monotonicMs();
            this.peer = peer;
            this.term = term;
        }

        @Override
        public void run(final Status status) {
            long latency = Utils.monotonicMs() - this.startMs;
            NodeImpl.this.metrics.recordLatency("pre-vote", latency);
            if (!status.isOk()) {
                LOG.warn("Node {} PreVote to {} latency={} error: {}.", getNodeId(), this.peer, status, latency);
            }
            else {
                handlePreVoteResponse(this.peer, this.term, getResponse());
            }
        }
    }

    // in writeLock
    private void preVote() {
        long preVoteTerm;
        try {
            LOG.info("Node {} term {} start preVote.", getNodeId(), this.currTerm);
            if (this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn(
                    "Node {} term {} doesn't do preVote when installing snapshot as the configuration may be out of date.",
                    getNodeId(), this.currTerm);
                return;
            }
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do preVote as it is not in conf <{}>.", getNodeId(), this.conf);
                return;
            }
            preVoteTerm = this.currTerm;
        }
        finally {
            this.writeLock.unlock();
        }

        final LogId lastLogId = this.logManager.getLastLogId(true);

        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // pre_vote need defense ABA after unlock&writeLock
            if (preVoteTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when get lastLogId.", getNodeId(), this.currTerm);
                return;
            }
            this.prevVoteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            for (final PeerId peer : this.conf.listPeers()) {
                if (peer.equals(this.serverId)) {
                    continue;
                }

                rpcClientService.connectAsync(peer).thenAccept(ok -> {
                    if (!ok) {
                        LOG.warn("Node {} failed to init channel, address={}.", getNodeId(), peer);
                        return;
                    }
                    final OnPreVoteRpcDone done = new OnPreVoteRpcDone(peer, preVoteTerm);
                    done.request = raftOptions.getRaftMessagesFactory()
                            .requestVoteRequest()
                            .preVote(true) // it's a pre-vote request.
                            .groupId(this.groupId)
                            .serverId(this.serverId.toString())
                            .peerId(peer.toString())
                            .term(preVoteTerm + 1) // next term
                            .lastLogIndex(lastLogId.getIndex())
                            .lastLogTerm(lastLogId.getTerm())
                            .build();
                    this.rpcClientService.preVote(peer, done.request, done);
                });
            }
            this.prevVoteCtx.grant(this.serverId);
            if (this.prevVoteCtx.isGranted()) {
                doUnlock = false;
                electSelf();
            }
        }
        finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private void handleVoteTimeout() {
        this.writeLock.lock();
        if (this.state != State.STATE_CANDIDATE) {
            this.writeLock.unlock();
            return;
        }

        // This is needed for the node, which won preVote in a previous iteration, but leader wasn't elected.
        if (this.prevVoteCtx.isGranted())
            adjustElectionTimeout();

        if (this.raftOptions.isStepDownWhenVoteTimedout()) {
            LOG.warn(
                "Candidate node {} term {} steps down when election reaching vote timeout: fail to get quorum vote-granted.",
                this.nodeId, this.currTerm);
            stepDown(this.currTerm, false, new Status(RaftError.ETIMEDOUT,
                "Vote timeout: fail to get quorum vote-granted."));
            // unlock in preVote
            preVote();
        }
        else {
            LOG.debug("Node {} term {} retry to vote self.", getNodeId(), this.currTerm);
            // unlock in electSelf
            electSelf();
        }
    }

    @Override
    public boolean isLeader() {
        return isLeader(true);
    }

    @Override
    public boolean isLeader(final boolean blocking) {
        if (!blocking) {
            return this.state == State.STATE_LEADER;
        }
        this.readLock.lock();
        try {
            return this.state == State.STATE_LEADER;
        }
        finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            LOG.info("Node {} shutdown, currTerm={} state={}.", getNodeId(), this.currTerm, this.state);
            if (this.state.compareTo(State.STATE_SHUTTING) < 0) {
                // If it is leader, set the wakeup_a_candidate with true;
                // If it is follower, call on_stop_following in step_down
                if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                    stepDown(this.currTerm, this.state == State.STATE_LEADER,
                        new Status(RaftError.ESHUTDOWN, "Raft node is going to quit."));
                }
                this.state = State.STATE_SHUTTING;
                // Stop all pending timer callbacks.
                stopAllTimers();
                if (this.readOnlyService != null) {
                    this.readOnlyService.shutdown();
                }
                if (this.logManager != null) {
                    this.logManager.shutdown();
                }
                if (this.metaStorage != null) {
                    this.metaStorage.shutdown();
                }
                if (this.snapshotExecutor != null) {
                    this.snapshotExecutor.shutdown();
                }
                if (this.wakingCandidate != null) {
                    Replicator.stop(this.wakingCandidate);
                }
                if (this.fsmCaller != null) {
                    this.fsmCaller.shutdown();
                }
                if (this.rpcClientService != null) {
                    this.rpcClientService.shutdown();
                }
                if (this.applyQueue != null) {
                    final CountDownLatch latch = new CountDownLatch(1);
                    this.shutdownLatch = latch;

                    Utils.runInThread(this.getOptions().getCommonExecutor(),
                        () -> this.applyQueue.publishEvent((event, sequence) -> {
                            event.nodeId = getNodeId();
                            event.handler = null;
                            event.evtType = DisruptorEventType.REGULAR;
                            event.shutdownLatch = latch;
                        }));
                }
            }
        }
        finally {
            this.writeLock.unlock();
        }
    }

    // Should in lock
    private List<RepeatedTimer> stopAllTimers() {
        final List<RepeatedTimer> timers = new ArrayList<>();
        if (this.electionTimer != null) {
            this.electionTimer.stop();
            timers.add(this.electionTimer);
        }
        if (this.voteTimer != null) {
            this.voteTimer.stop();
            timers.add(this.voteTimer);
        }
        if (this.stepDownTimer != null) {
            this.stepDownTimer.stop();
            timers.add(this.stepDownTimer);
        }
        if (this.snapshotTimer != null) {
            this.snapshotTimer.stop();
            timers.add(this.snapshotTimer);
        }
        return timers;
    }

    private void destroyAllTimers(final List<RepeatedTimer> timers) {
        for (final RepeatedTimer timer : timers) {
            timer.destroy();
        }
    }

    @Override
    public synchronized void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            if (this.readOnlyService != null) {
                this.readOnlyService.join();
            }
            if (this.logManager != null) {
                this.logManager.join();
            }
            if (this.snapshotExecutor != null) {
                this.snapshotExecutor.join();
            }
            if (this.wakingCandidate != null) {
                Replicator.join(this.wakingCandidate);
            }
            this.shutdownLatch.await();
            this.applyDisruptor.unsubscribe(getNodeId());
            this.shutdownLatch = null;
        }
        if (this.fsmCaller != null) {
            this.fsmCaller.join();
        }

        // Stop and reset non shared pools.
        NodeOptions opts = getOptions();

        if (opts.getScheduler() != null && !opts.isSharedPools()) {
            opts.getScheduler().shutdown();
        }
        if (opts.getElectionTimer() != null && !opts.isSharedPools()) {
            opts.getElectionTimer().stop();
        }
        if (opts.getVoteTimer() != null && !opts.isSharedPools()) {
            opts.getVoteTimer().stop();
        }
        if (opts.getStepDownTimer() != null && !opts.isSharedPools()) {
            opts.getStepDownTimer().stop();
        }
        if (opts.getSnapshotTimer() != null && !opts.isSharedPools()) {
            opts.getSnapshotTimer().stop();
        }
        if (opts.getCommonExecutor() != null && !opts.isSharedPools()) {
            ExecutorServiceHelper.shutdownAndAwaitTermination(opts.getCommonExecutor());
        }
        if (opts.getStripedExecutor() != null && !opts.isSharedPools()) {
            opts.getStripedExecutor().shutdownGracefully();
        }
        if (opts.getClientExecutor() != null && !opts.isSharedPools()) {
            ExecutorServiceHelper.shutdownAndAwaitTermination(opts.getClientExecutor());
        }
        if (opts.getfSMCallerExecutorDisruptor() != null && (!opts.isSharedPools() || ownFsmCallerExecutorDisruptorConfig != null)) {
            opts.getfSMCallerExecutorDisruptor().shutdown();
        }
        if (opts.getNodeApplyDisruptor() != null && !opts.isSharedPools()) {
            opts.getNodeApplyDisruptor().shutdown();
        }
        if (opts.getReadOnlyServiceDisruptor() != null && !opts.isSharedPools()) {
            opts.getReadOnlyServiceDisruptor().shutdown();
        }
        if (opts.getLogManagerDisruptor() != null && !opts.isSharedPools()) {
            opts.getLogManagerDisruptor().shutdown();
        }
    }

    private static class StopTransferArg {
        final NodeImpl node;
        final long term;
        final PeerId peer;

        StopTransferArg(final NodeImpl node, final long term, final PeerId peer) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
        }
    }

    private void handleTransferTimeout(final long term, final PeerId peer) {
        LOG.info("Node {} failed to transfer leadership to peer {}, reached timeout.", getNodeId(), peer);
        this.writeLock.lock();
        try {
            if (term == this.currTerm) {
                this.replicatorGroup.stopTransferLeadership(peer);
                if (this.state == State.STATE_TRANSFERRING) {
                    this.fsmCaller.onLeaderStart(term);
                    this.state = State.STATE_LEADER;
                    this.stopTransferArg = null;
                }
            }
        }
        finally {
            this.writeLock.unlock();
        }
    }

    private void onTransferTimeout(final StopTransferArg arg) {
        arg.node.handleTransferTimeout(arg.term, arg.peer);
    }

    /**
     * Retrieve current configuration this node seen so far. It's not a reliable way to retrieve cluster peers info, you
     * should use {@link #listPeers()} instead.
     *
     * @return current configuration.
     */
    public Configuration getCurrentConf() {
        this.readLock.lock();
        try {
            if (this.conf != null && this.conf.getConf() != null) {
                return this.conf.getConf().copy();
            }
            return null;
        }
        finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listPeers() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return this.conf.getConf().listPeers();
        }
        finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listAlivePeers() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return getAliveNodes(this.conf.getConf().getPeers(), Utils.monotonicMs());
        }
        finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listLearners() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return this.conf.getConf().listLearners();
        }
        finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listAliveLearners() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return getAliveNodes(this.conf.getConf().getLearners(), Utils.monotonicMs());
        }
        finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void addPeer(final PeerId peer, final Closure done) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            Requires.requireTrue(!this.conf.getConf().contains(peer), "Peer already exists in current configuration");

            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.addPeer(peer);
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        }
        finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void removePeer(final PeerId peer, final Closure done) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            Requires.requireTrue(this.conf.getConf().contains(peer), "Peer not found in current configuration");

            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.removePeer(peer);
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        }
        finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void changePeersAndLearners(final Configuration newPeersAndLearners, long term, final Closure done) {
        Requires.requireNonNull(newPeersAndLearners, "Null new configuration");
        Requires.requireTrue(!newPeersAndLearners.isEmpty(), "Empty new configuration");
        this.writeLock.lock();
        try {
            long currentTerm = getCurrentTerm();

            if (currentTerm != term) {
                LOG.warn("Node {} ignored the configuration because of mismatching terms. Current term is {}, but provided is {}.",
                    getNodeId(), currentTerm, term);

                    Utils.runClosureInThread(this.getOptions().getCommonExecutor(), done, Status.OK());

                    return;
            }

            LOG.info("Node {} change configuration from {} to {}.", getNodeId(), this.conf.getConf(), newPeersAndLearners);
            unsafeRegisterConfChange(this.conf.getConf(), newPeersAndLearners, done);
        }
        finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void changePeersAndLearnersAsync(final Configuration newConf, long term, Closure done) {
        Requires.requireNonNull(newConf, "Null new configuration");
        Requires.requireTrue(!newConf.isEmpty(), "Empty new configuration");
        this.writeLock.lock();
        try {
            long currentTerm = getCurrentTerm();

            if (currentTerm != term) {
                LOG.warn("Node {} ignored the configuration because of mismatching terms. Current term is {}, but provided is {}.",
                        getNodeId(), currentTerm, term);

                Utils.runClosureInThread(this.getOptions().getCommonExecutor(), done, Status.OK());

                return;
            }

            LOG.info("Node {} change configuration from {} to {}.", getNodeId(), this.conf.getConf(), newConf);

            unsafeRegisterConfChange(this.conf.getConf(), newConf, done, true);
        }
        finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public Status resetPeers(final Configuration newPeers) {
        if (options.getExternallyEnforcedConfigIndex() != null) {
            throw new IllegalStateException("Using both externallyEnforcedConfigIndex and resetPeers() is not supported "
                    + "[externallyEnforcedConfigIndex=" + options.getExternallyEnforcedConfigIndex() + "]");
        }

        Requires.requireNonNull(newPeers, "Null new peers");
        Requires.requireTrue(!newPeers.isEmpty(), "Empty new peers");
        Requires.requireTrue(newPeers.isValid(), "Invalid new peers: %s", newPeers);
        this.writeLock.lock();
        try {
            if (newPeers.isEmpty()) {
                LOG.warn("Node {} set empty peers.", getNodeId());
                return new Status(RaftError.EINVAL, "newPeers is empty");
            }
            if (!this.state.isActive()) {
                LOG.warn("Node {} is in state {}, can't set peers.", getNodeId(), this.state);
                return new Status(RaftError.EPERM, "Bad state: %s", this.state);
            }
            // bootstrap?
            if (this.conf.getConf().isEmpty()) {
                LOG.info("Node {} set peers to {} from empty.", getNodeId(), newPeers);
                this.conf.setConf(newPeers);
                stepDown(this.currTerm + 1, false, new Status(RaftError.ESETPEER, "Set peer from empty configuration"));
                return Status.OK();
            }
            if (this.state == State.STATE_LEADER && this.confCtx.isBusy()) {
                LOG.warn("Node {} set peers need wait current conf changing.", getNodeId());
                return new Status(RaftError.EBUSY, "Changing to another configuration");
            }
            // check equal, maybe retry direct return
            if (this.conf.getConf().equals(newPeers)) {
                return Status.OK();
            }
            final Configuration newConf = new Configuration(newPeers);
            LOG.info("Node {} set peers from {} to {}.", getNodeId(), this.conf.getConf(), newPeers);
            this.conf.setConf(newConf);
            this.conf.getOldConf().reset();
            stepDown(this.currTerm + 1, false, new Status(RaftError.ESETPEER, "Raft node set peer normally"));
            resetElectionTimeoutToInitial();
            return Status.OK();
        }
        finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void addLearners(final List<PeerId> learners, final Closure done) {
        checkPeers(learners);
        this.writeLock.lock();
        try {
            final Configuration newConf = new Configuration(this.conf.getConf());
            for (final PeerId peer : learners) {
                newConf.addLearner(peer);
            }
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        }
        finally {
            this.writeLock.unlock();
        }

    }

    private void checkPeers(final List<PeerId> peers) {
        Requires.requireNonNull(peers, "Null peers");
        Requires.requireTrue(!peers.isEmpty(), "Empty peers");
        for (final PeerId peer : peers) {
            Requires.requireNonNull(peer, "Null peer");
        }
    }

    @Override
    public void removeLearners(final List<PeerId> learners, final Closure done) {
        checkPeers(learners);
        this.writeLock.lock();
        try {
            final Configuration newConf = new Configuration(this.conf.getConf());
            for (final PeerId peer : learners) {
                newConf.removeLearner(peer);
            }
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        }
        finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void resetLearners(final List<PeerId> learners, final Closure done) {
        checkPeers(learners);
        this.writeLock.lock();
        try {
            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.setLearners(new LinkedHashSet<>(learners));
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        }
        finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void snapshot(final Closure done) {
        doSnapshot(done);
    }

    private void doSnapshot(final Closure done) {
        if (this.snapshotExecutor != null) {
            this.snapshotExecutor.doSnapshot(done);
        }
        else {
            if (done != null) {
                final Status status = new Status(RaftError.EINVAL, "Snapshot is not supported");
                Utils.runClosureInThread(this.getOptions().getCommonExecutor(), done, status);
            }
        }
    }

    @Override
    public void resetElectionTimeoutMs(final int electionTimeoutMs) {
        Requires.requireTrue(electionTimeoutMs > 0, "Invalid electionTimeoutMs");
        this.writeLock.lock();
        try {
            this.options.setElectionTimeoutMs(electionTimeoutMs);
            this.replicatorGroup.resetHeartbeatInterval(heartbeatTimeout(this.options.getElectionTimeoutMs()));
            this.replicatorGroup.resetElectionTimeoutInterval(electionTimeoutMs);
            LOG.info("Node {} reset electionTimeout, currTimer {} state {} new electionTimeout {}.", getNodeId(),
                this.currTerm, this.state, electionTimeoutMs);
            this.electionTimer.reset(electionTimeoutMs);
        }
        finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public Status transferLeadershipTo(final PeerId peer) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                LOG.warn("Node {} can't transfer leadership to peer {} as it is in state {}.", getNodeId(), peer,
                    this.state);
                return new Status(this.state == State.STATE_TRANSFERRING ? RaftError.EBUSY : RaftError.EPERM,
                    "Not a leader");
            }
            if (this.confCtx.isBusy()) {
                // It's very messy to deal with the case when the |peer| received
                // TimeoutNowRequest and increase the term while somehow another leader
                // which was not replicated with the newest configuration has been
                // elected. If no add_peer with this very |peer| is to be invoked ever
                // after nor this peer is to be killed, this peer will spin in the voting
                // procedure and make the each new leader stepped down when the peer
                // reached vote timeout and it starts to vote (because it will increase
                // the term of the group)
                // To make things simple, refuse the operation and force users to
                // invoke transfer_leadership_to after configuration changing is
                // completed so that the peer's configuration is up-to-date when it
                // receives the TimeOutNowRequest.
                LOG.warn(
                    "Node {} refused to transfer leadership to peer {} when the leader is changing the configuration.",
                    getNodeId(), peer);
                return new Status(RaftError.EBUSY, "Changing the configuration");
            }

            PeerId peerId = peer.copy();
            // if peer_id is ANY_PEER(0.0.0.0:0:0), the peer with the largest
            // last_log_id will be selected.
            if (peerId.isEmpty()) {
                LOG.info("Node {} starts to transfer leadership to any peer.", getNodeId());
                if ((peerId = this.replicatorGroup.findTheNextCandidate(this.conf)) == null) {
                    return new Status(-1, "Candidate not found for any peer");
                }
            }
            if (peerId.equals(this.serverId)) {
                LOG.info("Node {} transferred leadership to self.", this.serverId);
                return Status.OK();
            }
            if (!this.conf.contains(peerId)) {
                LOG.info("Node {} refused to transfer leadership to peer {} as it is not in {}.", getNodeId(), peer,
                    this.conf);
                return new Status(RaftError.EINVAL, "Not in current configuration");
            }

            final long lastLogIndex = this.logManager.getLastLogIndex();
            if (!this.replicatorGroup.transferLeadershipTo(peerId, lastLogIndex)) {
                LOG.warn("No such peer {}.", peer);
                return new Status(RaftError.EINVAL, "No such peer %s", peer);
            }
            this.state = State.STATE_TRANSFERRING;
            final Status status = new Status(RaftError.ETRANSFERLEADERSHIP,
                "Raft leader is transferring leadership to %s", peerId);
            onLeaderStop(status);
            LOG.info("Node {} starts to transfer leadership to peer {}.", getNodeId(), peer);
            final StopTransferArg stopArg = new StopTransferArg(this, this.currTerm, peerId);
            this.stopTransferArg = stopArg;
            this.transferTimer = this.getOptions().getScheduler().schedule(() -> onTransferTimeout(stopArg),
                this.options.getElectionTimeoutMs(), TimeUnit.MILLISECONDS);

        }
        finally {
            this.writeLock.unlock();
        }
        return Status.OK();
    }

    private void onLeaderStop(final Status status) {
        this.replicatorGroup.clearFailureReplicators();
        this.fsmCaller.onLeaderStop(status);
    }

    @Override
    public Message handleTimeoutNowRequest(final TimeoutNowRequest request, final RpcRequestClosure done) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (request.term() != this.currTerm) {
                final long savedCurrTerm = this.currTerm;
                if (request.term() > this.currTerm) {
                    stepDown(request.term(), false, new Status(RaftError.EHIGHERTERMREQUEST,
                        "Raft node receives higher term request"));
                }
                LOG.info("Node {} received TimeoutNowRequest from {} while currTerm={} didn't match requestTerm={}.",
                    getNodeId(), request.peerId(), savedCurrTerm, request.term());
                return raftOptions.getRaftMessagesFactory()
                    .timeoutNowResponse()
                    .term(this.currTerm)
                    .success(false)
                    .build();
            }
            if (this.state != State.STATE_FOLLOWER) {
                LOG.info("Node {} received TimeoutNowRequest from {}, while state={}, term={}.", getNodeId(),
                    request.serverId(), this.state, this.currTerm);
                return raftOptions.getRaftMessagesFactory()
                    .timeoutNowResponse()
                    .term(this.currTerm)
                    .success(false)
                    .build();
            }

            final long savedTerm = this.currTerm;
            final TimeoutNowResponse resp = raftOptions.getRaftMessagesFactory()
                .timeoutNowResponse()
                .term(this.currTerm + 1) //
                .success(true) //
                .build();
            // Parallelize response and election
            done.sendResponse(resp);
            doUnlock = false;

            LOG.info("Node {} received TimeoutNowRequest from {}, term={} and starts voting.", getNodeId(), request.serverId(),
                    savedTerm);

            electSelf();
        }
        finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
        return null;
    }

    @Override
    public Message handleInstallSnapshot(final InstallSnapshotRequest request, final RpcRequestClosure done) {
        if (this.snapshotExecutor == null) {
            return RaftRpcFactory.DEFAULT //
                .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL, "Not supported snapshot");
        }
        final PeerId serverId = new PeerId();
        if (!serverId.parse(request.serverId())) {
            LOG.warn("Node {} ignore InstallSnapshotRequest from {} bad server id.", getNodeId(), request.serverId());
            return RaftRpcFactory.DEFAULT //
                .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL,
                    "Parse serverId failed: %s", request.serverId());
        }

        // Check if a group is started.
        final PeerId dstPeerId = new PeerId();
        if (dstPeerId.parse(request.peerId())) {
            final String groupId = request.groupId();
            final Node node = done.getRpcCtx().getNodeManager().get(groupId, dstPeerId);
            if (node == null) {
                return RaftRpcFactory.DEFAULT.newResponse(raftOptions.getRaftMessagesFactory(), RaftError.ENOENT,
                        "Peer id not found: %s, group: %s", request.peerId(), groupId);
            }
        }
        else {
            return RaftRpcFactory.DEFAULT.newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL,
                "Fail to parse peerId: %s", request.peerId());
        }

        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                LOG.warn("Node {} ignore InstallSnapshotRequest as it is not in active state {}.", getNodeId(),
                    this.state);
                return RaftRpcFactory.DEFAULT //
                    .newResponse(raftOptions.getRaftMessagesFactory(), RaftError.EINVAL,
                        "Node %s:%s is not in active state, state %s.", this.groupId, this.serverId, this.state.name());
            }

            if (request.term() < this.currTerm) {
                LOG.warn("Node {} ignore stale InstallSnapshotRequest from {}, term={}, currTerm={}.", getNodeId(),
                    request.peerId(), request.term(), this.currTerm);
                return raftOptions.getRaftMessagesFactory()
                    .installSnapshotResponse()
                    .term(this.currTerm) //
                    .success(false) //
                    .build();
            }

            checkStepDown(request.term(), serverId);

            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                    serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                stepDown(request.term() + 1, false, new Status(RaftError.ELEADERCONFLICT,
                    "More than one leader in the same term."));
                return raftOptions.getRaftMessagesFactory()
                    .installSnapshotResponse()
                    .term(request.term() + 1) //
                    .success(false) //
                    .build();
            }

        }
        finally {
            this.writeLock.unlock();
        }
        final long startMs = Utils.monotonicMs();
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info(
                    "Node {} received InstallSnapshotRequest from {}, lastIncludedLogIndex={}, lastIncludedLogTerm={}, lastLogId={}.",
                    getNodeId(), request.serverId(), request.meta().lastIncludedIndex(), request.meta()
                        .lastIncludedTerm(), this.logManager.getLastLogId(false));
            }
            this.snapshotExecutor.installSnapshot(request, raftOptions.getRaftMessagesFactory().installSnapshotResponse(), done);
            return null;
        }
        finally {
            this.metrics.recordLatency("install-snapshot", Utils.monotonicMs() - startMs);
        }
    }

    public void updateConfigurationAfterInstallingSnapshot() {
        checkAndSetConfiguration(false);
    }

    private void stopReplicator(final Collection<PeerId> keep, final Collection<PeerId> drop) {
        if (drop != null) {
            for (final PeerId peer : drop) {
                if (!keep.contains(peer) && !peer.equals(this.serverId)) {
                    this.replicatorGroup.stopReplicator(peer);
                }
            }
        }
    }

    @Override
    public UserLog readCommittedUserLog(final long index) {
        if (index <= 0) {
            throw new LogIndexOutOfBoundsException("Request index is invalid: " + index);
        }

        final long savedLastAppliedIndex = this.fsmCaller.getLastAppliedIndex();

        if (index > savedLastAppliedIndex) {
            throw new LogIndexOutOfBoundsException("Request index " + index + " is greater than lastAppliedIndex: "
                + savedLastAppliedIndex);
        }

        long curIndex = index;
        LogEntry entry = this.logManager.getEntry(curIndex);
        if (entry == null) {
            throw new LogNotFoundException("User log is deleted at index: " + index);
        }

        do {
            if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA) {
                return new UserLog(curIndex, entry.getData());
            }
            else {
                curIndex++;
            }
            if (curIndex > savedLastAppliedIndex) {
                throw new IllegalStateException("No user log between index:" + index + " and last_applied_index:"
                    + savedLastAppliedIndex);
            }
            entry = this.logManager.getEntry(curIndex);
        }
        while (entry != null);

        throw new LogNotFoundException("User log is deleted at index: " + curIndex);
    }

    @Override
    public void addReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener) {
        Requires.requireNonNull(replicatorStateListener, "replicatorStateListener");
        this.replicatorStateListeners.add(replicatorStateListener);
    }

    @Override
    public void removeReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener) {
        Requires.requireNonNull(replicatorStateListener, "replicatorStateListener");
        this.replicatorStateListeners.remove(replicatorStateListener);
    }

    @Override
    public void clearReplicatorStateListeners() {
        this.replicatorStateListeners.clear();
    }

    @Override
    public List<Replicator.ReplicatorStateListener> getReplicatorStateListeners() {
        return this.replicatorStateListeners;
    }

    @Override
    public int getNodeTargetPriority() {
        return this.targetPriority;
    }

    @Override
    public State getNodeState() {
        return this.state;
    }

    @Override
    public void describe(final Printer out) {
        // node
        final String _nodeId;
        final String _state;
        final String _leaderId;
        final long _currTerm;
        final String _conf;
        final int _targetPriority;
        this.readLock.lock();
        try {
            _nodeId = String.valueOf(getNodeId());
            _state = String.valueOf(this.state);
            _leaderId = String.valueOf(this.leaderId);
            _currTerm = this.currTerm;
            _conf = String.valueOf(this.conf);
            _targetPriority = this.targetPriority;
        }
        finally {
            this.readLock.unlock();
        }
        out.print("nodeId: ") //
            .println(_nodeId);
        out.print("state: ") //
            .println(_state);
        out.print("leaderId: ") //
            .println(_leaderId);
        out.print("term: ") //
            .println(_currTerm);
        out.print("conf: ") //
            .println(_conf);
        out.print("targetPriority: ") //
            .println(_targetPriority);

        // timers
        out.println("electionTimer: ");
        this.electionTimer.describe(out);

        out.println("voteTimer: ");
        this.voteTimer.describe(out);

        out.println("stepDownTimer: ");
        this.stepDownTimer.describe(out);

        out.println("snapshotTimer: ");
        this.snapshotTimer.describe(out);

        // logManager
        out.println("logManager: ");
        this.logManager.describe(out);

        // fsmCaller
        out.println("fsmCaller: ");
        this.fsmCaller.describe(out);

        // ballotBox
        out.println("ballotBox: ");
        this.ballotBox.describe(out);

        // snapshotExecutor
        if (this.snapshotExecutor != null) {
            out.println("snapshotExecutor: ");
            this.snapshotExecutor.describe(out);
        }

        // replicators
        out.println("replicatorGroup: ");
        this.replicatorGroup.describe(out);

        // log storage
        if (this.logStorage instanceof Describer) {
            out.println("logStorage: ");
            ((Describer) this.logStorage).describe(out);
        }
    }

    /**
     * @return The state.
     */
    public State getState() {
        return state;
    }

    @Override
    public String toString() {
        return "JRaftNode [nodeId=" + getNodeId() + "]";
    }
}
