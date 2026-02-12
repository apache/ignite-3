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
package org.apache.ignite.raft.jraft.option;

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metrics.sources.RaftMetricSource;
import org.apache.ignite.internal.raft.JraftGroupEventsListener;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.storage.impl.StripeAwareLogManager.Stripe;
import org.apache.ignite.raft.jraft.JRaftServiceFactory;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.StateMachine;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.core.ElectionPriority;
import org.apache.ignite.raft.jraft.core.FSMCallerImpl;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.ReadOnlyServiceImpl;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.core.Scheduler;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.impl.LogManagerImpl;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.apache.ignite.raft.jraft.util.ByteBufferCollectorPool;
import org.apache.ignite.raft.jraft.util.Copiable;
import org.apache.ignite.raft.jraft.util.NoopTimeoutStrategy;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.apache.ignite.raft.jraft.util.TimeoutStrategy;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentLinkedLifoByteBufferCollectorPool;
import org.apache.ignite.raft.jraft.util.concurrent.FixedThreadsExecutorGroup;
import org.apache.ignite.raft.jraft.util.timer.Timer;
import org.jetbrains.annotations.Nullable;

/**
 * Node options.
 */
public class NodeOptions extends RpcOptions implements Copiable<NodeOptions> {
    /** This value is used by default to determine the count of stripes in the striped disruptors, excluding log manager disruptor. */
    private static final int DEFAULT_STRIPES = Utils.cpus();

    /** This value is used by default to determine the count of stripes for log manager. */
    private static final int DEFAULT_LOG_STRIPES_COUNT = Math.min(4, DEFAULT_STRIPES);

    // A follower would become a candidate if it doesn't receive any message
    // from the leader in |election_timeout_ms| milliseconds
    // Default: 1200 (1.2s)
    private int electionTimeoutMs = 1200; // follower to candidate timeout

    private TimeoutStrategy electionTimeoutStrategy = new NoopTimeoutStrategy();

    // One node's local priority value would be set to | electionPriority |
    // value when it starts up.If this value is set to 0,the node will never be a leader.
    // If this node doesn't support priority election,then set this value to -1.
    // Default: -1
    private int electionPriority = ElectionPriority.Disabled;

    // If next leader is not elected until next election timeout, it exponentially
    // decay its local target priority, for example target_priority = target_priority - gap
    // Default: 10
    private int decayPriorityGap = 10;

    // Leader lease time's ratio of electionTimeoutMs,
    // To minimize the effects of clock drift, we should make that:
    // clockDrift + leaderLeaseTimeoutMs < electionTimeout
    // Default: 90, Max: 100
    private int leaderLeaseTimeRatio = 90;

    // A snapshot saving would be triggered every |snapshot_interval_s| seconds
    // if this was reset as a positive number
    // If |snapshot_interval_s| <= 0, the time based snapshot would be disabled.
    //
    // Default: 3600 (1 hour)
    private int snapshotIntervalSecs = 3600;

    // A snapshot saving would be triggered every |snapshot_interval_s| seconds,
    // and at this moment when state machine's lastAppliedIndex value
    // minus lastSnapshotId value is greater than snapshotLogIndexMargin value,
    // the snapshot action will be done really.
    // If |snapshotLogIndexMargin| <= 0, the distance based snapshot would be disable.
    //
    // Default: 0
    private int snapshotLogIndexMargin = 0;

    // We will regard a adding peer as caught up if the margin between the
    // last_log_index of this peer and the last_log_index of leader is less than
    // |catchup_margin|
    //
    // Default: 1000
    private int catchupMargin = 1000;

    // If node is starting from a empty environment (both LogStorage and
    // SnapshotStorage are empty), it would use |initial_conf| as the
    // configuration of the group, otherwise it would load configuration from
    // the existing environment.
    //
    // Default: A empty group
    private Configuration initialConf = new Configuration();

    // The specific StateMachine implemented your business logic, which must be
    // a valid instance.
    private StateMachine fsm;

    // Listener for raft group reconfiguration events.
    private JraftGroupEventsListener raftGrpEvtsLsnr;

    // Describe a specific LogStorage in format ${type}://${parameters}
    private String logUri;

    // Describe a specific RaftMetaStorage in format ${type}://${parameters}
    private String raftMetaUri;

    // Describe a specific SnapshotStorage in format ${type}://${parameters}
    private String snapshotUri;

    // If enable, we will filter duplicate files before copy remote snapshot,
    // to avoid useless transmission. Two files in local and remote are duplicate,
    // only if they has the same filename and the same checksum (stored in file meta).
    // Default: false
    private boolean filterBeforeCopyRemote = false;

    // If non-null, we will pass this throughput_snapshot_throttle to SnapshotExecutor
    // Default: NULL
    //    scoped_refptr<SnapshotThrottle>* snapshot_throttle;

    // If true, RPCs through raft_cli will be denied.
    // Default: false
    private boolean disableCli = false;

    /**
     * Timer manager thread pool size
     */
    private int timerPoolSize = Math.min(Utils.cpus() * 3, 20);

    /**
     * CLI service request RPC executor pool size, use default executor if -1.
     */
    private int cliRpcThreadPoolSize = Utils.cpus();

    /**
     * RAFT request RPC executor pool size, use default executor if -1.
     */
    private int raftRpcThreadPoolSize = Utils.cpus() * 6;

    /**
     * Common executor pool size.
     */
    private int commonThreadPollSize = Utils.cpus();

    /**
     * Whether to enable metrics for node.
     */
    private boolean enableMetrics = false; // TODO asch https://issues.apache.org/jira/browse/IGNITE-14847

    /**
     * If non-null, we will pass this SnapshotThrottle to SnapshotExecutor Default: NULL
     */
    private SnapshotThrottle snapshotThrottle;

    /**
     * Custom service factory.
     */
    private JRaftServiceFactory serviceFactory;

    /**
     * Callbacks for replicator events.
     */
    private List<Replicator.ReplicatorStateListener> replicationStateListeners;

    /**
     * The common executor for short running tasks.
     */
    private ExecutorService commonExecutor;

    /**
     * Striped executor for processing AppendEntries request/reponse.
     */
    private FixedThreadsExecutorGroup stripedExecutor;

    /**
     * The scheduler to execute delayed jobs.
     */
    private Scheduler scheduler;

    /**
     * The election timer.
     */
    private Timer electionTimer;

    /**
     * The election timer.
     */
    private Timer voteTimer;

    /**
     * The election timer.
     */
    private Timer snapshotTimer;

    /**
     * The election timer.
     */
    private Timer stepDownTimer;

    /**
     * Server name.
     */
    private String serverName;

    /**
     * Striped disruptor for FSMCaller service. The queue serves of an Append entry requests in the RAFT state machine.
     */
    private StripedDisruptor<FSMCallerImpl.ApplyTask> fSMCallerExecutorDisruptor;

    /**
     * Striped disruptor for Node apply service.
     */
    private StripedDisruptor<NodeImpl.LogEntryAndClosure> nodeApplyDisruptor;

    /**
     * Striped disruptor for Read only service.
     */
    private StripedDisruptor<ReadOnlyServiceImpl.ReadIndexEvent> readOnlyServiceDisruptor;

    /**
     * Striped disruptor for Log manager service.
     */
    private StripedDisruptor<LogManagerImpl.StableClosureEvent> logManagerDisruptor;

    /** A hybrid clock */
    private HybridClock clock = new HybridClockImpl();

    /**
     * Amount of Disruptors that will handle the RAFT server.
     */
    private int stripes = DEFAULT_STRIPES;

    /**
     * Amount of log manager Disruptors stripes.
     */
    private int logStripesCount = DEFAULT_LOG_STRIPES_COUNT;

    /**
     * Set true to use the non-blocking strategy in the log manager.
     */
    private boolean logYieldStrategy;

    /** */
    private boolean sharedPools = false;

    /** */
    private List<Stripe> logStripes;

    /**
     * Apply task in blocking or non-blocking mode, ApplyTaskMode.NonBlocking by default.
     */
    private ApplyTaskMode applyTaskMode = ApplyTaskMode.NonBlocking;

    private Marshaller commandsMarshaller;

    private RaftMetricSource raftMetrics;

    /** Node manager. */
    private NodeManager nodeManager;

    /**
     * Externally enforced config index.
     *
     * <p>If it's not {@code null}, then the Raft node abstains from becoming a leader in configurations whose index precedes
     * the externally enforced index..
     *
     * <p>The idea is that, if a Raft group was forcefully repaired (because it lost majority) using resetPeers(),
     * the old majority nodes might come back online. If this happens and we do nothing, they might elect a leader from the old majority
     * that could hijack leadership and cause havoc in the repaired group.
     *
     * <p>To prevent this, on a starup or subsequent config changes, current voting set (aka peers) of the repaired group may be 'broken'
     * to make it impossible for the current node to become a leader. This is enabled by setting a non-null value to
     * {@link NodeOptions#getExternallyEnforcedConfigIndex ()}. When it's set, on each change of configuration (happening to this.conf),
     * including the one at startup, we check whether the applied config precedes the externally enforced
     * config (in which case this.conf.peers will be 'broken' to make sure current node does not become a leader) or not (in which case
     * the applied config will be used as is).
     */
    private @Nullable Long externallyEnforcedConfigIndex;

    /**
     * If the group is declared as a system group, certain threads are dedicated specifically for that one.
     */
    private boolean isSystemGroup = false;

    /**
     * Shared pool of {@link ByteBufferCollector} for sending log entries for replication.
     *
     * <p>Used to prevent a large number of {@link ByteBufferCollector} from being accumulated across all threads that are involved in
     * sending log entries, see {@link ByteBufferCollector#allocateByRecyclers}.</p>
     */
    private ByteBufferCollectorPool appendEntriesByteBufferCollectorPool = new ConcurrentLinkedLifoByteBufferCollectorPool(
            Utils.MAX_COLLECTOR_SIZE_PER_SERVER
    );

    private SafeTimeValidator safeTimeValidator = new PermissiveSafeTimeValidator();

    public NodeOptions() {
        raftOptions.setRaftMessagesFactory(getRaftMessagesFactory());
    }

    /**
    * Gets a system group flag.
    *
    * @return System group flag.
    */
    public boolean isSystemGroup() {
        return isSystemGroup;
    }

    public void setSystemGroup(boolean systemGroup) {
        isSystemGroup = systemGroup;
    }

    /**
    * Gets raft metrics.
    *
    * @return Raft metrics.
    */
    public RaftMetricSource getRaftMetrics() {
        return raftMetrics;
    }

    public void setRaftMetrics(RaftMetricSource raftMetrics) {
        this.raftMetrics = raftMetrics;
    }

    /**
     * Gets a node manager.
     *
     * @return Node manager.
     */
    public NodeManager getNodeManager() {
        return nodeManager;
    }

    /**
     * Sets a node manager.
     *
     * @param nodeManager Node manager.
     */
    public void setNodeManager(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    /**
     * @return Stripe count.
     */
    public int getStripes() {
        return stripes;
    }

    /**
     * @param stripes Stripe count.
     */
    public void setStripes(int stripes) {
        this.stripes = stripes;
    }

    /**
     * @return Log stripes count.
     */
    public int getLogStripesCount() {
        return logStripesCount;
    }

    /**
     * @param logStripesCount Log stripes.
     */
    public void setLogStripesCount(int logStripesCount) {
        this.logStripesCount = logStripesCount;
    }

    public boolean isLogYieldStrategy() {
        return logYieldStrategy;
    }

    public void setLogYieldStrategy(boolean logYieldStrategy) {
        this.logYieldStrategy = logYieldStrategy;
    }

    /**
     * Returns {@code true} if shared pools mode is in use.
     *
     * <p>In this mode thread pools are passed in the node options and node doesn't attempt to create/destroy any.
     *
     * @return {@code true} if shared pools mode is in use.
     */
    public boolean isSharedPools() {
        return this.sharedPools;
    }

    /**
     * @param sharedPools {code true} to enable shared pools mode.
     */
    public void setSharedPools(boolean sharedPools) {
        this.sharedPools = sharedPools;
    }

    public ApplyTaskMode getApplyTaskMode() {
        return this.applyTaskMode;
    }

    public void setApplyTaskMode(final ApplyTaskMode applyTaskMode) {
        this.applyTaskMode = applyTaskMode;
    }

    /**
     * Service factory.
     */
    public JRaftServiceFactory getServiceFactory() {
        return this.serviceFactory;
    }

    public void setServiceFactory(final JRaftServiceFactory serviceFactory) {
        this.serviceFactory = serviceFactory;
    }

    public SnapshotThrottle getSnapshotThrottle() {
        return this.snapshotThrottle;
    }

    public void setSnapshotThrottle(final SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    public void setEnableMetrics(final boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }

    /**
     * Raft options
     */
    private RaftOptions raftOptions = new RaftOptions();

    public int getCliRpcThreadPoolSize() {
        return this.cliRpcThreadPoolSize;
    }

    public void setCliRpcThreadPoolSize(final int cliRpcThreadPoolSize) {
        this.cliRpcThreadPoolSize = cliRpcThreadPoolSize;
    }

    public boolean isEnableMetrics() {
        return this.enableMetrics;
    }

    public int getRaftRpcThreadPoolSize() {
        return this.raftRpcThreadPoolSize;
    }

    public void setRaftRpcThreadPoolSize(final int raftRpcThreadPoolSize) {
        this.raftRpcThreadPoolSize = raftRpcThreadPoolSize;
    }

    public int getCommonThreadPollSize() {
        return commonThreadPollSize;
    }

    public void setCommonThreadPollSize(int commonThreadPollSize) {
        this.commonThreadPollSize = commonThreadPollSize;
    }

    public int getTimerPoolSize() {
        return this.timerPoolSize;
    }

    public void setTimerPoolSize(final int timerPoolSize) {
        this.timerPoolSize = timerPoolSize;
    }

    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    public void setRaftOptions(final RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

    public void validate() {
        if (StringUtils.isBlank(this.logUri)) {
            throw new IllegalArgumentException("Blank logUri");
        }
        if (StringUtils.isBlank(this.raftMetaUri)) {
            throw new IllegalArgumentException("Blank raftMetaUri");
        }
        if (this.fsm == null) {
            throw new IllegalArgumentException("Null stateMachine");
        }
    }

    public int getElectionPriority() {
        return this.electionPriority;
    }

    public void setElectionPriority(int electionPriority) {
        this.electionPriority = electionPriority;
    }

    public int getDecayPriorityGap() {
        return this.decayPriorityGap;
    }

    public void setDecayPriorityGap(int decayPriorityGap) {
        this.decayPriorityGap = decayPriorityGap;
    }

    public int getElectionTimeoutMs() {
        return this.electionTimeoutMs;
    }

    public void setElectionTimeoutMs(final int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public int getLeaderLeaseTimeRatio() {
        return this.leaderLeaseTimeRatio;
    }

    public void setLeaderLeaseTimeRatio(final int leaderLeaseTimeRatio) {
        if (leaderLeaseTimeRatio <= 0 || leaderLeaseTimeRatio > 100) {
            throw new IllegalArgumentException("leaderLeaseTimeRatio: " + leaderLeaseTimeRatio
                + " (expected: 0 < leaderLeaseTimeRatio <= 100)");
        }
        this.leaderLeaseTimeRatio = leaderLeaseTimeRatio;
    }

    public int getLeaderLeaseTimeoutMs() {
        // TODO asch precompute IGNITE-14832
        return this.electionTimeoutMs * this.leaderLeaseTimeRatio / 100;
    }

    public int getSnapshotIntervalSecs() {
        return this.snapshotIntervalSecs;
    }

    public void setSnapshotIntervalSecs(final int snapshotIntervalSecs) {
        this.snapshotIntervalSecs = snapshotIntervalSecs;
    }

    public int getSnapshotLogIndexMargin() {
        return this.snapshotLogIndexMargin;
    }

    public void setSnapshotLogIndexMargin(int snapshotLogIndexMargin) {
        this.snapshotLogIndexMargin = snapshotLogIndexMargin;
    }

    public int getCatchupMargin() {
        return this.catchupMargin;
    }

    public void setCatchupMargin(final int catchupMargin) {
        this.catchupMargin = catchupMargin;
    }

    public Configuration getInitialConf() {
        return this.initialConf;
    }

    public void setInitialConf(final Configuration initialConf) {
        this.initialConf = initialConf;
    }

    public JraftGroupEventsListener getRaftGrpEvtsLsnr() {
        return raftGrpEvtsLsnr;
    }

    public void setRaftGrpEvtsLsnr(JraftGroupEventsListener raftGrpEvtsLsnr) {
        this.raftGrpEvtsLsnr = raftGrpEvtsLsnr;
    }

    public StateMachine getFsm() {
        return this.fsm;
    }

    public void setFsm(final StateMachine fsm) {
        this.fsm = fsm;
    }

    public String getLogUri() {
        return this.logUri;
    }

    public void setLogUri(final String logUri) {
        this.logUri = logUri;
    }

    public String getRaftMetaUri() {
        return this.raftMetaUri;
    }

    public void setRaftMetaUri(final String raftMetaUri) {
        this.raftMetaUri = raftMetaUri;
    }

    public String getSnapshotUri() {
        return this.snapshotUri;
    }

    public void setSnapshotUri(final String snapshotUri) {
        this.snapshotUri = snapshotUri;
    }

    public boolean isFilterBeforeCopyRemote() {
        return this.filterBeforeCopyRemote;
    }

    public void setFilterBeforeCopyRemote(final boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
    }

    public boolean isDisableCli() {
        return this.disableCli;
    }

    public void setDisableCli(final boolean disableCli) {
        this.disableCli = disableCli;
    }

    public void setCommonExecutor(ExecutorService commonExecutor) {
        this.commonExecutor = commonExecutor;
    }

    public ExecutorService getCommonExecutor() {
        return this.commonExecutor;
    }

    public FixedThreadsExecutorGroup getStripedExecutor() {
        return this.stripedExecutor;
    }

    public void setStripedExecutor(FixedThreadsExecutorGroup stripedExecutor) {
        this.stripedExecutor = stripedExecutor;
    }

    public Scheduler getScheduler() {
        return this.scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public Timer getElectionTimer() {
        return this.electionTimer;
    }

    public void setElectionTimer(Timer electionTimer) {
        this.electionTimer = electionTimer;
    }

    public Timer getVoteTimer() {
        return this.voteTimer;
    }

    public void setVoteTimer(Timer voteTimer) {
        this.voteTimer = voteTimer;
    }

    public Timer getSnapshotTimer() {
        return this.snapshotTimer;
    }

    public void setSnapshotTimer(Timer snapshotTimer) {
        this.snapshotTimer = snapshotTimer;
    }

    public Timer getStepDownTimer() {
        return this.stepDownTimer;
    }

    public void setStepDownTimer(Timer stepDownTimer) {
        this.stepDownTimer = stepDownTimer;
    }

    public String getServerName() {
        return this.serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public StripedDisruptor<FSMCallerImpl.ApplyTask> getfSMCallerExecutorDisruptor() {
        return this.fSMCallerExecutorDisruptor;
    }

    public void setfSMCallerExecutorDisruptor(StripedDisruptor<FSMCallerImpl.ApplyTask> fSMCallerExecutorDisruptor) {
        this.fSMCallerExecutorDisruptor = fSMCallerExecutorDisruptor;
    }

    public StripedDisruptor<NodeImpl.LogEntryAndClosure> getNodeApplyDisruptor() {
        return this.nodeApplyDisruptor;
    }

    public void setNodeApplyDisruptor(StripedDisruptor<NodeImpl.LogEntryAndClosure> nodeApplyDisruptor) {
        this.nodeApplyDisruptor = nodeApplyDisruptor;
    }

    public StripedDisruptor<ReadOnlyServiceImpl.ReadIndexEvent> getReadOnlyServiceDisruptor() {
        return this.readOnlyServiceDisruptor;
    }

    public void setReadOnlyServiceDisruptor(StripedDisruptor<ReadOnlyServiceImpl.ReadIndexEvent> readOnlyServiceDisruptor) {
        this.readOnlyServiceDisruptor = readOnlyServiceDisruptor;
    }

    public StripedDisruptor<LogManagerImpl.StableClosureEvent> getLogManagerDisruptor() {
        return this.logManagerDisruptor;
    }

    public void setLogManagerDisruptor(StripedDisruptor<LogManagerImpl.StableClosureEvent> logManagerDisruptor) {
        this.logManagerDisruptor = logManagerDisruptor;
    }

    public void setLogStripes(List<Stripe> logStripes) {
        this.logStripes = logStripes;
    }

    public List<Stripe> getLogStripes() {
        return this.logStripes;
    }

    public HybridClock getClock() {
        return this.clock;
    }

    public void setClock(HybridClock clock) {
        this.clock = clock;
    }

    @Override
    public NodeOptions copy() {
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setElectionTimeoutMs(this.electionTimeoutMs);
        nodeOptions.setElectionPriority(this.electionPriority);
        nodeOptions.setDecayPriorityGap(this.decayPriorityGap);
        nodeOptions.setSnapshotIntervalSecs(this.snapshotIntervalSecs);
        nodeOptions.setSnapshotLogIndexMargin(this.snapshotLogIndexMargin);
        nodeOptions.setCatchupMargin(this.catchupMargin);
        nodeOptions.setFilterBeforeCopyRemote(this.filterBeforeCopyRemote);
        nodeOptions.setDisableCli(this.disableCli);
        nodeOptions.setTimerPoolSize(this.timerPoolSize);
        nodeOptions.setCliRpcThreadPoolSize(this.cliRpcThreadPoolSize);
        nodeOptions.setRaftRpcThreadPoolSize(this.raftRpcThreadPoolSize);
        nodeOptions.setCommonThreadPollSize(this.commonThreadPollSize);
        nodeOptions.setEnableMetrics(this.enableMetrics);
        nodeOptions.setRaftOptions(this.raftOptions.copy());
        nodeOptions.setReplicationStateListeners(this.replicationStateListeners);
        nodeOptions.setCommonExecutor(this.getCommonExecutor());
        nodeOptions.setStripedExecutor(this.getStripedExecutor());
        nodeOptions.setServerName(this.getServerName());
        nodeOptions.setScheduler(this.getScheduler());
        nodeOptions.setClientExecutor(this.getClientExecutor());
        nodeOptions.setNodeApplyDisruptor(this.getNodeApplyDisruptor());
        nodeOptions.setfSMCallerExecutorDisruptor(this.getfSMCallerExecutorDisruptor());
        nodeOptions.setReadOnlyServiceDisruptor(this.getReadOnlyServiceDisruptor());
        nodeOptions.setLogManagerDisruptor(this.getLogManagerDisruptor());
        nodeOptions.setLogStripes(this.getLogStripes());
        nodeOptions.setElectionTimer(this.getElectionTimer());
        nodeOptions.setVoteTimer(this.getVoteTimer());
        nodeOptions.setSnapshotTimer(this.getSnapshotTimer());
        nodeOptions.setStepDownTimer(this.getStepDownTimer());
        nodeOptions.setSharedPools(this.isSharedPools());
        nodeOptions.setRpcDefaultTimeout(this.getRpcDefaultTimeout());
        nodeOptions.setRpcConnectTimeoutMs(this.getRpcConnectTimeoutMs());
        nodeOptions.setRpcInstallSnapshotTimeout(this.getRpcInstallSnapshotTimeout());
        nodeOptions.setElectionTimeoutStrategy(this.getElectionTimeoutStrategy());
        nodeOptions.setClock(this.getClock());
        nodeOptions.setCommandsMarshaller(this.getCommandsMarshaller());
        nodeOptions.setStripes(this.getStripes());
        nodeOptions.setLogStripesCount(this.getLogStripesCount());
        nodeOptions.setLogYieldStrategy(this.isLogYieldStrategy());
        nodeOptions.setNodeManager(this.getNodeManager());
        nodeOptions.setAppendEntriesByteBufferCollectorPool(appendEntriesByteBufferCollectorPool);
        nodeOptions.setRaftMetrics(raftMetrics);
        nodeOptions.setSafeTimeValidator(this.getSafeTimeValidator());

        return nodeOptions;
    }

    public String toString() {
        return "NodeOptions{" + "electionTimeoutMs=" + this.electionTimeoutMs + ", electionPriority="
               + this.electionPriority + ", decayPriorityGap=" + this.decayPriorityGap + ", leaderLeaseTimeRatio="
               + this.leaderLeaseTimeRatio + ", snapshotIntervalSecs=" + this.snapshotIntervalSecs
               + ", snapshotLogIndexMargin=" + this.snapshotLogIndexMargin + ", catchupMargin=" + this.catchupMargin
               + ", initialConf=" + this.initialConf + ", fsm=" + this.fsm + ", logUri='" + this.logUri + '\''
               + ", raftMetaUri='" + this.raftMetaUri + '\'' + ", snapshotUri='" + this.snapshotUri + '\''
               + ", filterBeforeCopyRemote=" + this.filterBeforeCopyRemote + ", disableCli=" + this.disableCli + ", timerPoolSize="
               + this.timerPoolSize + ", cliRpcThreadPoolSize=" + this.cliRpcThreadPoolSize + ", raftRpcThreadPoolSize="
               + this.raftRpcThreadPoolSize + ", enableMetrics=" + this.enableMetrics + ", snapshotThrottle="
               + this.snapshotThrottle + ", serviceFactory=" + this.serviceFactory + ", applyTaskMode="
               + this.applyTaskMode + ", raftOptions=" + this.raftOptions + "} " + super.toString();
    }

    /**
     * @param replicationStateListeners Listeners.
     */
    public void setReplicationStateListeners(List<Replicator.ReplicatorStateListener> replicationStateListeners) {
        this.replicationStateListeners = replicationStateListeners;
    }

    /**
     * @return Listeners.
     */
    public List<Replicator.ReplicatorStateListener> getReplicationStateListeners() {
        return replicationStateListeners;
    }

    public TimeoutStrategy getElectionTimeoutStrategy() {
        return electionTimeoutStrategy;
    }

    public void setElectionTimeoutStrategy(TimeoutStrategy electionTimeoutStrategy) {
        this.electionTimeoutStrategy = electionTimeoutStrategy;
    }

    public Marshaller getCommandsMarshaller() {
        return commandsMarshaller;
    }

    public void setCommandsMarshaller(Marshaller commandsMarshaller) {
        this.commandsMarshaller = commandsMarshaller;
    }

    @Nullable
    public Long getExternallyEnforcedConfigIndex() {
        return externallyEnforcedConfigIndex;
    }

    public void setExternallyEnforcedConfigIndex(@Nullable Long index) {
        this.externallyEnforcedConfigIndex = index;
    }

    /** Returns shared pool of {@link ByteBufferCollector} for sending log entries for replication. */
    public ByteBufferCollectorPool getAppendEntriesByteBufferCollectorPool() {
        return appendEntriesByteBufferCollectorPool;
    }

    /** Sets shared pool of {@link ByteBufferCollector} for sending log entries for replication. */
    public void setAppendEntriesByteBufferCollectorPool(ByteBufferCollectorPool appendEntriesByteBufferCollectorPool) {
        this.appendEntriesByteBufferCollectorPool = appendEntriesByteBufferCollectorPool;
    }

    public SafeTimeValidator getSafeTimeValidator() {
        return safeTimeValidator;
    }

    public void setSafeTimeValidator(SafeTimeValidator safeTimeValidator) {
        this.safeTimeValidator = safeTimeValidator;
    }
}
