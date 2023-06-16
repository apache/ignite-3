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

package org.apache.ignite.internal.raft.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SubscriptionLeaderChangeRequest;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.jetbrains.annotations.Nullable;

/**
 * RAFT client aware of a logical topology to handle distributed events.
 */
public class TopologyAwareRaftGroupService implements RaftGroupService {

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TopologyAwareRaftGroupService.class);

    /** Raft message factory. */
    private final RaftMessagesFactory factory;

    /** Cluster service. */
    private final ClusterService clusterService;

    /** RPC RAFT client. */
    private final RaftGroupService raftClient;

    /** Logical topology service. */
    private final LogicalTopologyService logicalTopologyService;

    /** Leader election handler. */
    private final ServerEventHandler serverEventHandler;

    private final RaftGroupEventsClientListener eventsClientListener;

    /** Executor to invoke RPC requests. */
    private final ScheduledExecutorService executor;

    /** RAFT configuration. */
    private final RaftConfiguration raftConfiguration;

    /**
     * Whether to notify callback after subscription to pass the current leader and term into it, even if the leader
     * did not change in that moment (see {@link #subscribeLeader(BiConsumer)}).
     */
    private final boolean notifyOnSubscription;

    /**
     * The constructor.
     *
     * @param cluster Cluster service.
     * @param factory Message factory.
     * @param executor RPC executor.
     * @param raftClient RPC RAFT client.
     * @param logicalTopologyService Logical topology.
     * @param notifyOnSubscription Whether to notify callback after subscription to pass the current leader and term into it,
     *        even if the leader did not change in that moment (see {@link #subscribeLeader(BiConsumer)}).
     */
    private TopologyAwareRaftGroupService(
            ClusterService cluster,
            RaftMessagesFactory factory,
            ScheduledExecutorService executor,
            RaftConfiguration raftConfiguration,
            RaftGroupService raftClient,
            LogicalTopologyService logicalTopologyService,
            RaftGroupEventsClientListener eventsClientListener,
            boolean notifyOnSubscription
    ) {
        this.clusterService = cluster;
        this.factory = factory;
        this.executor = executor;
        this.raftConfiguration = raftConfiguration;
        this.raftClient = raftClient;
        this.logicalTopologyService = logicalTopologyService;
        this.serverEventHandler = new ServerEventHandler();
        this.eventsClientListener = eventsClientListener;
        this.notifyOnSubscription = notifyOnSubscription;

        this.eventsClientListener.addLeaderElectionListener(groupId(), serverEventHandler);

        logicalTopologyService.addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onNodeJoined(LogicalNode appearedNode, LogicalTopologySnapshot newTopology) {
                for (Peer peer : peers()) {
                    if (serverEventHandler.isSubscribed() && appearedNode.name().equals(peer.consistentId())) {
                        LOG.info("New peer will be sending a leader elected notification [grpId={}, consistentId={}]", groupId(),
                                peer.consistentId());

                        sendSubscribeMessage(appearedNode, TopologyAwareRaftGroupService.this.factory.subscriptionLeaderChangeRequest()
                                .groupId(groupId())
                                .subscribe(true)
                                .build())
                                .thenComposeAsync(subscribed -> {
                                    if (subscribed) {
                                        return refreshAndGetLeaderWithTerm()
                                                .thenAcceptAsync(leaderWithTerm -> {
                                                    if (leaderWithTerm.leader() != null
                                                            && appearedNode.name().equals(leaderWithTerm.leader().consistentId())) {
                                                        serverEventHandler.onLeaderElected(appearedNode, leaderWithTerm.term());
                                                    }
                                                }, executor);
                                    }

                                    return CompletableFuture.completedFuture(null);
                                }, executor);
                    }
                }
            }
        });
    }

    /**
     * Starts an instance of topology aware RAFT client.
     *
     * @param groupId Replication group id.
     * @param cluster Cluster service.
     * @param factory Message factory.
     * @param raftConfiguration RAFT configuration.
     * @param configuration Group configuration.
     * @param getLeader True to get the group's leader upon service creation.
     * @param executor RPC executor.
     * @param logicalTopologyService Logical topology service.
     * @param notifyOnSubscription Whether to notify callback after subscription to pass the current leader and term into it,
     *        even if the leader did not change in that moment (see {@link #subscribeLeader(BiConsumer)}).
     * @return Future to create a raft client.
     */
    public static CompletableFuture<RaftGroupService> start(
            ReplicationGroupId groupId,
            ClusterService cluster,
            RaftMessagesFactory factory,
            RaftConfiguration raftConfiguration,
            PeersAndLearners configuration,
            boolean getLeader,
            ScheduledExecutorService executor,
            LogicalTopologyService logicalTopologyService,
            RaftGroupEventsClientListener eventsClientListener,
            boolean notifyOnSubscription
    ) {
        return RaftGroupServiceImpl.start(groupId, cluster, factory, raftConfiguration, configuration, getLeader, executor)
                .thenApply(raftGroupService -> new TopologyAwareRaftGroupService(cluster, factory, executor, raftConfiguration,
                        raftGroupService, logicalTopologyService, eventsClientListener, notifyOnSubscription));
    }

    /**
     * Sends a subscribe message to a specific node of the cluster.
     *
     * @param node Node.
     * @param msg  Subscribe message.
     * @return A future that completes with true when the message sent and false value when the node left the cluster.
     */
    private CompletableFuture<Boolean> sendSubscribeMessage(ClusterNode node, SubscriptionLeaderChangeRequest msg) {
        var msgSendFut = new CompletableFuture<Boolean>();

        sendWithRetry(node, msg, msgSendFut);

        return msgSendFut;
    }

    /**
     * Tries to send a subscribe message until the node leaves of the cluster.
     *
     * @param node Node.
     * @param msg Subscribe message to send.
     * @param msgSendFut Future that completes with true when the message sent and with false when the node left topology and cannot get a
     *     cluster.
     */
    private void sendWithRetry(ClusterNode node, SubscriptionLeaderChangeRequest msg, CompletableFuture<Boolean> msgSendFut) {
        clusterService.messagingService().invoke(node, msg, raftConfiguration.responseTimeout().value()).whenCompleteAsync((unused, th) -> {
            if (th != null) {
                if (recoverable(th)) {
                    logicalTopologyService.logicalTopologyOnLeader().whenCompleteAsync((logicalTopologySnapshot, topologyGetIssue) -> {
                        if (topologyGetIssue != null) {
                            LOG.error("Actual logical topology snapshot was not got.", topologyGetIssue);

                            msgSendFut.completeExceptionally(topologyGetIssue);

                            return;
                        }

                        if (logicalTopologySnapshot.nodes().contains(node)) {
                            sendWithRetry(node, msg, msgSendFut);
                        } else {
                            LOG.info("Could not subscribe to leader update from a specific node, because the node had left from the"
                                    + " cluster [node={}]", node);

                            msgSendFut.complete(false);
                        }
                    }, executor);
                } else {
                    if (!(th instanceof NodeStoppingException)) {
                        LOG.error("Could not send the subscribe message to the node [node={}, msg={}]", th, node, msg);
                    }

                    msgSendFut.completeExceptionally(th);
                }

                return;
            }

            msgSendFut.complete(true);
        }, executor);
    }

    /**
     * Whether an exception is recoverable or not.
     *
     * @param t The exception.
     * @return True if the exception is recoverable, false otherwise.
     */
    private static boolean recoverable(Throwable t) {
        if (t instanceof ExecutionException || t instanceof CompletionException) {
            t = t.getCause();
        }

        return t instanceof TimeoutException || t instanceof IOException;
    }

    /**
     * Assigns a closure to call when a leader will is elected.
     *
     * @param callback Callback closure.
     * @return Future that is completed when all subscription messages to peers are sent.
     */
    public CompletableFuture<Void> subscribeLeader(BiConsumer<ClusterNode, Long> callback) {
        assert !serverEventHandler.isSubscribed() : "The node already subscribed";

        int peers = peers().size();

        var futs = new CompletableFuture[peers];

        serverEventHandler.setOnLeaderElectedCallback(callback);

        for (int i = 0; i < peers; i++) {
            Peer peer = peers().get(i);

            ClusterNode node = clusterService.topologyService().getByConsistentId(peer.consistentId());

            if (node != null) {
                futs[i] = sendSubscribeMessage(node, factory.subscriptionLeaderChangeRequest()
                        .groupId(groupId())
                        .subscribe(true)
                        .build());
            } else {
                futs[i] = CompletableFuture.completedFuture(null);
            }
        }

        if (notifyOnSubscription) {
            return CompletableFuture.allOf(futs).whenCompleteAsync((unused, throwable) -> {
                if (throwable != null) {
                    throw new IgniteException(Common.INTERNAL_ERR, throwable);
                }

                refreshAndGetLeaderWithTerm().thenAcceptAsync(leaderWithTerm -> {
                    if (leaderWithTerm.leader() != null) {
                        serverEventHandler.onLeaderElected(
                                clusterService.topologyService().getByConsistentId(leaderWithTerm.leader().consistentId()),
                                leaderWithTerm.term()
                        );
                    }
                }, executor);
            }, executor);
        } else {
            return CompletableFuture.allOf(futs);
        }
    }

    /**
     * Unsubscribe of notification about a leader elected.
     *
     * @return Future that is completed when all messages about cancelling subscription to peers are sent.
     */
    public CompletableFuture<Void> unsubscribeLeader() {
        serverEventHandler.setOnLeaderElectedCallback(null);
        serverEventHandler.resetLeader();

        var peers = peers();
        List<CompletableFuture<Boolean>> futs = new ArrayList<>();

        for (int i = 0; i < peers.size(); i++) {
            Peer peer = peers.get(i);

            ClusterNode node = clusterService.topologyService().getByConsistentId(peer.consistentId());

            if (node != null) {
                futs.add(sendSubscribeMessage(node, factory.subscriptionLeaderChangeRequest()
                        .groupId(groupId())
                        .subscribe(false)
                        .build()));
            }
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[futs.size()]));
    }

    @Override
    public ReplicationGroupId groupId() {
        return raftClient.groupId();
    }

    @Override
    public @Nullable Peer leader() {
        Peer leader = serverEventHandler.leader();

        return leader == null ? raftClient.leader() : leader;
    }

    @Override
    public @Nullable List<Peer> peers() {
        return raftClient.peers();
    }

    @Override
    public @Nullable List<Peer> learners() {
        return raftClient.learners();
    }

    @Override
    public CompletableFuture<Void> refreshLeader() {
        return raftClient.refreshLeader();
    }

    @Override
    public CompletableFuture<LeaderWithTerm> refreshAndGetLeaderWithTerm() {
        return raftClient.refreshAndGetLeaderWithTerm();
    }

    @Override
    public CompletableFuture<Void> refreshMembers(boolean onlyAlive) {
        return raftClient.refreshMembers(onlyAlive);
    }

    @Override
    public CompletableFuture<Void> addPeer(Peer peer) {
        return raftClient.addPeer(peer);
    }

    @Override
    public CompletableFuture<Void> removePeer(Peer peer) {
        return raftClient.removePeer(peer);
    }

    @Override
    public CompletableFuture<Void> changePeers(Collection<Peer> peers) {
        return raftClient.changePeers(peers);
    }

    @Override
    public CompletableFuture<Void> changePeersAsync(PeersAndLearners peersAndLearners, long term) {
        return raftClient.changePeersAsync(peersAndLearners, term);
    }

    @Override
    public CompletableFuture<Void> addLearners(Collection<Peer> learners) {
        return raftClient.addLearners(learners);
    }

    @Override
    public CompletableFuture<Void> removeLearners(Collection<Peer> learners) {
        return raftClient.removeLearners(learners);
    }

    @Override
    public CompletableFuture<Void> resetLearners(Collection<Peer> learners) {
        return raftClient.resetLearners(learners);
    }

    @Override
    public CompletableFuture<Void> snapshot(Peer peer) {
        return raftClient.snapshot(peer);
    }

    @Override
    public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        return raftClient.transferLeadership(newLeader);
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd) {
        return raftClient.run(cmd);
    }

    @Override
    public void shutdown() {
        eventsClientListener.removeLeaderElectionListener(groupId(), serverEventHandler);

        raftClient.shutdown();
    }

    @Override
    public CompletableFuture<Long> readIndex() {
        return raftClient.readIndex();
    }

    @Override
    public ClusterService clusterService() {
        return raftClient.clusterService();
    }

    /**
     * Leader election handler.
     */
    private static class ServerEventHandler implements BiConsumer<ClusterNode, Long> {
        /** A term of last elected leader. */
        private long term = 0;

        /** Last elected leader. */
        private Peer leaderPeer;

        /** A leader elected callback. */
        private BiConsumer<ClusterNode, Long> onLeaderElectedCallback;

        /**
         * Notifies about a new leader elected, if it did not make before.
         *
         * @param node Node.
         * @param term Term.
         */
        private synchronized void onLeaderElected(ClusterNode node, long term) {
            if (onLeaderElectedCallback != null && term > this.term) {
                this.term = term;
                this.leaderPeer = new Peer(node.name());

                onLeaderElectedCallback.accept(node, term);
            }
        }

        /**
         * Assigns a leader election callback.
         *
         * @param onLeaderElectedCallback A callback closure.
         */
        public synchronized void setOnLeaderElectedCallback(BiConsumer<ClusterNode, Long> onLeaderElectedCallback) {
            this.onLeaderElectedCallback = onLeaderElectedCallback;
        }

        /**
         * Whether notification is required.
         *
         * @return True if notification required, false otherwise.
         */
        public synchronized boolean isSubscribed() {
            return onLeaderElectedCallback != null;
        }

        @Override
        public void accept(ClusterNode clusterNode, Long term) {
            onLeaderElected(clusterNode, term);
        }

        Peer leader() {
            return leaderPeer;
        }

        void resetLeader() {
            leaderPeer = null;
        }
    }
}
