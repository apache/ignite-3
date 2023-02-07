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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
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
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.RaftMessageGroup;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SubscriptionLeaderChangeRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SubscriptionLeaderChangeResponse;
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

    /** Executor to invoke RPC requests. */
    private final ScheduledExecutorService executor;

    /** RAFT configuration. */
    private final RaftConfiguration raftConfiguration;


    /**
     * The constructor.
     *
     * @param cluster Cluster service.
     * @param factory Message factory.
     * @param executor RPC executor.
     * @param raftClient RPC RAFT client.
     * @param logicalTopologyService Logical topology.
     */
    private TopologyAwareRaftGroupService(
            ClusterService cluster,
            RaftMessagesFactory factory,
            ScheduledExecutorService executor,
            RaftConfiguration raftConfiguration,
            RaftGroupService raftClient,
            LogicalTopologyService logicalTopologyService
    ) {
        this.clusterService = cluster;
        this.factory = factory;
        this.executor = executor;
        this.raftConfiguration = raftConfiguration;
        this.raftClient = raftClient;
        this.logicalTopologyService = logicalTopologyService;
        this.serverEventHandler = new ServerEventHandler();

        cluster.messagingService().addMessageHandler(RaftMessageGroup.class, (message, senderConsistentId, correlationId) -> {
            if (message instanceof SubscriptionLeaderChangeResponse) {
                var msg = (SubscriptionLeaderChangeResponse) message;

                serverEventHandler.onLeaderElected(clusterService.topologyService().getByConsistentId(senderConsistentId), msg.term());
            }
        });

        logicalTopologyService.addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onAppeared(ClusterNode appearedNode, LogicalTopologySnapshot newTopology) {
                for (Peer peer : peers()) {
                    if (serverEventHandler.isSubscribed() && appearedNode.name().equals(peer.consistentId())) {
                        LOG.info("New peer will be sending a leader elected notification [grpId={}, consistentId={}]", groupId(),
                                peer.consistentId());

                        sendSubscribeMessage(appearedNode, TopologyAwareRaftGroupService.this.factory.subscriptionLeaderChangeRequest()
                                .groupId(groupId())
                                .subscribe(true)
                                .build())
                                .thenComposeAsync(couldLeaderChange -> {
                                    if (couldLeaderChange) {
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
            LogicalTopologyService logicalTopologyService
    ) {
        return RaftGroupServiceImpl.start(groupId, cluster, factory, raftConfiguration, configuration, getLeader, executor)
                .thenApply(raftGroupService -> new TopologyAwareRaftGroupService(cluster, factory, executor, raftConfiguration,
                        raftGroupService, logicalTopologyService));
    }

    /**
     * Sends a subscribe message to a specific node of the cluster.
     *
     * @param node Node.
     * @param msg  Subscribe message.
     * @return A future that will complete when the message is sent.
     */
    private CompletableFuture<Boolean> sendSubscribeMessage(ClusterNode node, SubscriptionLeaderChangeRequest msg) {
        var msgSendFut = new CompletableFuture<Boolean>();

        sendWithRetry(node, msg, msgSendFut);

        return msgSendFut;
    }

    /**
     * Tries to send a subscribe message until the node leaves of the cluster.
     *
     * @param node       Node.
     * @param msg        Subscribe message to send.
     * @param msgSendFut Future that will completed when the message is send or an issue prevent that.
     */
    private void sendWithRetry(ClusterNode node, SubscriptionLeaderChangeRequest msg, CompletableFuture<Boolean> msgSendFut) {
        clusterService.messagingService().invoke(node, msg, raftConfiguration.responseTimeout().value()).whenCompleteAsync((unused, th) -> {
            if (th != null) {
                if (recoverable(th)) {
                    logicalTopologyService.logicalTopologyOnLeader().whenCompleteAsync((logicalTopologySnapshot, topologyGetIssue) -> {
                        if (topologyGetIssue != null) {
                            LOG.error("Actual logical topology snapshot was not get.", topologyGetIssue);

                            msgSendFut.completeExceptionally(topologyGetIssue);

                            return;
                        }

                        if (logicalTopologySnapshot.nodes().contains(node)) {
                            sendWithRetry(node, msg, msgSendFut);
                        } else {
                            LOG.info("Could not subscribe to leader update from a specific node, because the node had left of the cluster"
                                    + " [node={}]", node);

                            msgSendFut.complete(false);
                        }
                    }, executor);
                } else {
                    LOG.error("Could not send the subscribe message to the node [node={}, msg={}]", th, node, msg);

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
     */
    public void subscribeLeader(Consumer<ClusterNode> callback) {
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

        CompletableFuture.allOf(futs).whenCompleteAsync((unused, throwable) -> {
            if (throwable != null) {
                throw new IgniteException(Common.UNEXPECTED_ERR, throwable);
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
    }

    /**
     * Unsubscribe of notification about a leader elected.
     */
    public void unsubscribeLeader() {
        serverEventHandler.setOnLeaderElectedCallback(null);

        for (Peer peer : peers()) {
            ClusterNode node = clusterService.topologyService().getByConsistentId(peer.consistentId());

            if (node != null) {
                sendSubscribeMessage(node, factory.subscriptionLeaderChangeRequest()
                        .groupId(groupId())
                        .subscribe(false)
                        .build());
            }
        }
    }

    @Override
    public ReplicationGroupId groupId() {
        return raftClient.groupId();
    }

    @Override
    public @Nullable Peer leader() {
        return raftClient.leader();
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
    public CompletableFuture<Void> removeLearners(List<Peer> learners) {
        return raftClient.removeLearners(learners);
    }

    @Override
    public CompletableFuture<Void> resetLearners(List<Peer> learners) {
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
        raftClient.shutdown();
    }

    @Override
    public ClusterService clusterService() {
        return raftClient.clusterService();
    }

    /**
     * Leader election handler.
     */
    private static class ServerEventHandler {
        /** A term of last elected leader. */
        private long term = 0;

        /** A leader elected callback. */
        private Consumer<ClusterNode> onLeaderElectedCallback;

        /**
         * Notifies about a new leader elected, if it did not make before.
         *
         * @param node Node.
         * @param term Term.
         */
        private synchronized void onLeaderElected(ClusterNode node, long term) {
            if (onLeaderElectedCallback != null && term > this.term) {
                this.term = term;

                onLeaderElectedCallback.accept(node);
            }
        }

        /**
         * Assigns a leader election callback.
         *
         * @param onLeaderElectedCallback A callback closure.
         */
        public synchronized void setOnLeaderElectedCallback(Consumer<ClusterNode> onLeaderElectedCallback) {
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
    }
}
