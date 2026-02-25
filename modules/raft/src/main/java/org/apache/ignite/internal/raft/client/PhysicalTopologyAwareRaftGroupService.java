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

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.raft.client.RaftPeerUtils.parsePeer;
import static org.apache.ignite.internal.raft.client.RaftPeerUtils.parsePeerList;
import static org.apache.ignite.internal.raft.client.RaftPeerUtils.peerId;
import static org.apache.ignite.internal.raft.client.RaftPeerUtils.peerIds;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ExceptionFactory;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.TimeAwareRaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersAsyncResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.LearnersOpResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SubscriptionLeaderChangeRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexResponse;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.jetbrains.annotations.Nullable;

/**
 * The RAFT group service is based on the cluster physical topology. This service has ability to subscribe of a RAFT group leader update.
 *
 * <p>Command execution with leader-aware retry semantics is delegated to {@link RaftCommandExecutor}.
 */
public class PhysicalTopologyAwareRaftGroupService implements TimeAwareRaftGroupService {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PhysicalTopologyAwareRaftGroupService.class);

    /** Raft message factory. */
    private static final RaftMessagesFactory MESSAGES_FACTORY = Loza.FACTORY;

    /** Replication group ID. */
    private final ReplicationGroupId groupId;

    /** General leader election listener. */
    private final ServerEventHandler generalLeaderElectionListener;

    /** Failure manager. */
    private final FailureManager failureManager;

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Factory for creating stopping exceptions. */
    private final ExceptionFactory stoppingExceptionFactory;

    /** Command executor with retry semantics. */
    private final RaftCommandExecutor commandExecutor;

    /** Sender for subscription messages with retry logic. */
    private final SubscriptionMessageSender subscriptionMessageSender;

    /** Current peers. */
    private volatile List<Peer> peers;

    /** Current learners. */
    private volatile List<Peer> learners;

    /** Topology event handler for cleanup during shutdown. */
    private final TopologyEventHandler topologyEventHandler;

    /** Events client listener for cleanup during shutdown. */
    private final RaftGroupEventsClientListener eventsClientListener;

    /** Timeout in milliseconds to wait for subscription cleanup during shutdown. */
    private static final long SUBSCRIPTION_CLEANUP_TIMEOUT_MS = 30_000;

    /**
     * Constructor.
     *
     * @param failureManager Failure manager.
     * @param clusterService Cluster service.
     * @param executor Executor to invoke RPC requests and notify listeners.
     * @param raftConfiguration RAFT configuration.
     * @param eventsClientListener Events client listener.
     */
    private PhysicalTopologyAwareRaftGroupService(
            ReplicationGroupId groupId,
            FailureManager failureManager,
            ClusterService clusterService,
            ScheduledExecutorService executor,
            RaftConfiguration raftConfiguration,
            PeersAndLearners configuration,
            Marshaller cmdMarshaller,
            ExceptionFactory stoppingExceptionFactory,
            RaftGroupEventsClientListener eventsClientListener
    ) {
        this.groupId = groupId;
        this.failureManager = failureManager;
        this.clusterService = clusterService;
        this.stoppingExceptionFactory = stoppingExceptionFactory;
        this.peers = List.copyOf(configuration.peers());
        this.learners = List.copyOf(configuration.learners());

        this.commandExecutor = new RaftCommandExecutor(
                groupId,
                this::peers,
                clusterService,
                executor,
                raftConfiguration,
                cmdMarshaller,
                stoppingExceptionFactory
        );

        this.subscriptionMessageSender = new SubscriptionMessageSender(clusterService.messagingService(), executor, raftConfiguration);

        this.generalLeaderElectionListener = new ServerEventHandler(executor);

        this.eventsClientListener = eventsClientListener;
        eventsClientListener.addLeaderElectionListener(groupId, generalLeaderElectionListener);

        // Subscribe the command executor's leader availability state to leader election notifications.
        subscribeLeader(commandExecutor.leaderElectionListener());

        TopologyService topologyService = clusterService.topologyService();

        this.topologyEventHandler = new TopologyEventHandler() {
            @Override
            public void onAppeared(InternalClusterNode member) {
                CompletableFuture<Boolean> fut = changeNodeSubscriptionIfNeed(member, true);

                requestLeaderManually(topologyService, executor, fut);
            }
        };
        topologyService.addEventHandler(topologyEventHandler);

        ArrayList<CompletableFuture<?>> futures = new ArrayList<>();

        for (InternalClusterNode member : topologyService.allMembers()) {
            futures.add(changeNodeSubscriptionIfNeed(member, true));
        }

        requestLeaderManually(topologyService, executor, CompletableFutures.allOf(futures));
    }

    /**
     * Requests the leader information for the RAFT group.
     *
     * @param topologyService Topology service.
     * @param executor Executor to run asynchronous tasks.
     * @param subscriptionsFut Future representing the completion of subscription updates.
     */
    // TODO: IGNITE-27256 Remove the method after implementing a notification after subscription.
    private void requestLeaderManually(
            TopologyService topologyService,
            Executor executor,
            CompletableFuture<?> subscriptionsFut
    ) {
        subscriptionsFut.thenRunAsync(
                () -> refreshAndGetLeaderWithTerm(Long.MAX_VALUE).whenCompleteAsync(
                        (leaderWithTerm, throwable) -> {
                            if (throwable != null) {
                                LOG.warn("Could not refresh and get leader with term [grp={}].", groupId(), throwable);
                                return;
                            }

                            if (leaderWithTerm == null || leaderWithTerm.leader() == null) {
                                LOG.debug("No leader information available [grp={}].", groupId());
                                return;
                            }

                            InternalClusterNode leaderHost = topologyService
                                    .getByConsistentId(leaderWithTerm.leader().consistentId());

                            if (leaderHost != null) {
                                generalLeaderElectionListener.onLeaderElected(
                                        leaderHost,
                                        leaderWithTerm.term()
                                );
                            } else {
                                LOG.warn("Leader host occurred to leave the topology [nodeId = {}].",
                                        leaderWithTerm.leader().consistentId());
                            }
                        }, executor),
                executor);
    }

    private CompletableFuture<Boolean> changeNodeSubscriptionIfNeed(InternalClusterNode member, boolean subscribe) {
        Peer peer = new Peer(member.name());

        if (peers().contains(peer)) {
            SubscriptionLeaderChangeRequest msg = MESSAGES_FACTORY.subscriptionLeaderChangeRequest()
                    .groupId(groupId)
                    .subscribe(subscribe)
                    .build();

            return subscriptionMessageSender.send(member, msg).whenComplete((isSent, err) -> {
                if (err != null) {
                    failureManager.process(new FailureContext(err, "Could not change subscription to leader updates [grp="
                            + groupId() + "]."));
                }

                LOG.info("Subscription status changed for the peer [grp={}, consistentId={}, subscribe={}, isSent={}].",
                        groupId(), member.name(), subscribe, isSent);
            });
        }

        return CompletableFutures.booleanCompletedFuture(false);
    }

    /**
     * Starts a new instance of the PhysicalTopologyAwareRaftGroupService.
     *
     * @param groupId The ID of the RAFT group.
     * @param cluster Cluster service for communication.
     * @param raftConfiguration Configuration for the RAFT group.
     * @param configuration Peers and learners configuration.
     * @param executor Executor for asynchronous tasks.
     * @param eventsClientListener Listener for RAFT group events.
     * @param cmdMarshaller Marshaller for RAFT commands.
     * @param stoppingExceptionFactory Factory for creating stopping exceptions.
     * @param failureManager Manager for handling failures.
     * @return A new instance of PhysicalTopologyAwareRaftGroupService.
     */
    public static PhysicalTopologyAwareRaftGroupService start(
            ReplicationGroupId groupId,
            ClusterService cluster,
            RaftConfiguration raftConfiguration,
            PeersAndLearners configuration,
            ScheduledExecutorService executor,
            RaftGroupEventsClientListener eventsClientListener,
            Marshaller cmdMarshaller,
            ExceptionFactory stoppingExceptionFactory,
            FailureManager failureManager
    ) {
        return new PhysicalTopologyAwareRaftGroupService(
                groupId,
                failureManager,
                cluster,
                executor,
                raftConfiguration,
                configuration,
                cmdMarshaller,
                stoppingExceptionFactory,
                eventsClientListener
        );
    }

    private void finishSubscriptions() {
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();

        for (InternalClusterNode member : clusterService.topologyService().allMembers()) {
            futures.add(changeNodeSubscriptionIfNeed(member, false));
        }

        // Wait for unsubscription to complete with a timeout, but don't fail if it times out.
        try {
            CompletableFutures.allOf(futures).get(SUBSCRIPTION_CLEANUP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debug("Interrupted while waiting for subscription cleanup [grp={}].", groupId);
        } catch (ExecutionException e) {
            LOG.debug("Error during subscription cleanup [grp={}].", groupId, e.getCause());
        } catch (TimeoutException e) {
            LOG.debug("Timeout waiting for subscription cleanup [grp={}].", groupId);
        }
    }

    public void subscribeLeader(LeaderElectionListener callback) {
        generalLeaderElectionListener.addCallbackAndNotify(callback);
    }

    public void unsubscribeLeader(LeaderElectionListener callback) {
        generalLeaderElectionListener.removeCallbackAndNotify(callback);
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd, long timeoutMillis) {
        return commandExecutor.run(cmd, timeoutMillis);
    }

    @Override
    public ReplicationGroupId groupId() {
        return groupId;
    }

    @Override
    public @Nullable Peer leader() {
        return commandExecutor.leader();
    }

    @Override
    public List<Peer> peers() {
        return peers;
    }

    @Override
    public @Nullable List<Peer> learners() {
        return learners;
    }

    @Override
    public CompletableFuture<Void> refreshLeader(long timeoutMillis) {
        return commandExecutor.<GetLeaderResponse>send(
                peer -> MESSAGES_FACTORY.getLeaderRequest()
                        .peerId(peerId(peer))
                        .groupId(groupId.toString())
                        .build(),
                TargetPeerStrategy.RANDOM,
                timeoutMillis
        ).thenAccept(resp -> {
            // Only update cached leader if the term is newer to avoid overwriting
            // fresher info from leader election notifications with stale responses.
            commandExecutor.setLeaderIfTermNewer(parsePeer(resp.leaderId()), resp.currentTerm());
        });
    }

    @Override
    public CompletableFuture<LeaderWithTerm> refreshAndGetLeaderWithTerm(long timeoutMillis) {
        return commandExecutor.<GetLeaderResponse>send(
                peer -> MESSAGES_FACTORY.getLeaderRequest()
                        .peerId(peerId(peer))
                        .groupId(groupId.toString())
                        .build(),
                TargetPeerStrategy.RANDOM,
                timeoutMillis
        ).thenApply(resp -> {
            if (resp.leaderId() == null) {
                return LeaderWithTerm.NO_LEADER;
            }

            Peer respLeader = parsePeer(resp.leaderId());
            // Only update cached leader if the term is newer to avoid overwriting
            // fresher info from leader election notifications with stale responses.
            commandExecutor.setLeaderIfTermNewer(respLeader, resp.currentTerm());
            return new LeaderWithTerm(respLeader, resp.currentTerm());
        });
    }

    @Override
    public CompletableFuture<Void> refreshMembers(boolean onlyAlive, long timeoutMillis) {
        return commandExecutor.<GetPeersResponse>send(
                peer -> MESSAGES_FACTORY.getPeersRequest()
                        .leaderId(peerId(peer))
                        .onlyAlive(onlyAlive)
                        .groupId(groupId.toString())
                        .build(),
                TargetPeerStrategy.LEADER,
                timeoutMillis
        ).thenAccept(resp -> {
            this.peers = parsePeerList(resp.peersList());
            this.learners = parsePeerList(resp.learnersList());
        });
    }

    @Override
    public CompletableFuture<Void> addPeer(Peer peer, long sequenceToken, long timeoutMillis) {
        return commandExecutor.<AddPeerResponse>send(
                targetPeer -> MESSAGES_FACTORY.addPeerRequest()
                        .leaderId(peerId(targetPeer))
                        .groupId(groupId.toString())
                        .peerId(peerId(peer))
                        .sequenceToken(sequenceToken)
                        .build(),
                TargetPeerStrategy.LEADER,
                timeoutMillis
        ).thenAccept(resp -> this.peers = parsePeerList(resp.newPeersList()));
    }

    @Override
    public CompletableFuture<Void> removePeer(Peer peer, long sequenceToken, long timeoutMillis) {
        return commandExecutor.<RemovePeerResponse>send(
                targetPeer -> MESSAGES_FACTORY.removePeerRequest()
                        .leaderId(peerId(targetPeer))
                        .groupId(groupId.toString())
                        .peerId(peerId(peer))
                        .sequenceToken(sequenceToken)
                        .build(),
                TargetPeerStrategy.LEADER,
                timeoutMillis
        ).thenAccept(resp -> this.peers = parsePeerList(resp.newPeersList()));
    }

    @Override
    public CompletableFuture<Void> changePeersAndLearners(
            PeersAndLearners peersAndLearners, long term, long sequenceToken, long timeoutMillis) {
        LOG.info("Sending changePeersAndLearners request for group={} to peers={} and learners={} with leader term={}",
                groupId, peersAndLearners.peers(), peersAndLearners.learners(), term);

        return commandExecutor.<ChangePeersAndLearnersResponse>send(
                targetPeer -> MESSAGES_FACTORY.changePeersAndLearnersRequest()
                        .leaderId(peerId(targetPeer))
                        .groupId(groupId.toString())
                        .term(term)
                        .newPeersList(peerIds(peersAndLearners.peers()))
                        .newLearnersList(peerIds(peersAndLearners.learners()))
                        .sequenceToken(sequenceToken)
                        .build(),
                TargetPeerStrategy.LEADER,
                timeoutMillis
        ).thenAccept(resp -> {
            this.peers = parsePeerList(resp.newPeersList());
            this.learners = parsePeerList(resp.newLearnersList());
        });
    }

    @Override
    public CompletableFuture<Void> changePeersAndLearnersAsync(
            PeersAndLearners peersAndLearners, long term, long sequenceToken, long timeoutMillis) {
        LOG.info("Sending changePeersAndLearnersAsync request for group={} to peers={} and learners={} with leader term={}",
                groupId, peersAndLearners.peers(), peersAndLearners.learners(), term);

        return commandExecutor.<ChangePeersAndLearnersAsyncResponse>send(
                targetPeer -> MESSAGES_FACTORY.changePeersAndLearnersAsyncRequest()
                        .leaderId(peerId(targetPeer))
                        .groupId(groupId.toString())
                        .term(term)
                        .newPeersList(peerIds(peersAndLearners.peers()))
                        .newLearnersList(peerIds(peersAndLearners.learners()))
                        .sequenceToken(sequenceToken)
                        .build(),
                TargetPeerStrategy.LEADER,
                timeoutMillis
        ).thenAccept(resp -> {
            this.peers = parsePeerList(resp.newPeersList());
            this.learners = parsePeerList(resp.newLearnersList());
        });
    }

    @Override
    public CompletableFuture<Void> addLearners(Collection<Peer> learners, long sequenceToken, long timeoutMillis) {
        return commandExecutor.<LearnersOpResponse>send(
                targetPeer -> MESSAGES_FACTORY.addLearnersRequest()
                        .leaderId(peerId(targetPeer))
                        .groupId(groupId.toString())
                        .learnersList(peerIds(learners))
                        .sequenceToken(sequenceToken)
                        .build(),
                TargetPeerStrategy.LEADER,
                timeoutMillis
        ).thenAccept(resp -> this.learners = parsePeerList(resp.newLearnersList()));
    }

    @Override
    public CompletableFuture<Void> removeLearners(Collection<Peer> learners, long sequenceToken, long timeoutMillis) {
        return commandExecutor.<LearnersOpResponse>send(
                targetPeer -> MESSAGES_FACTORY.removeLearnersRequest()
                        .leaderId(peerId(targetPeer))
                        .groupId(groupId.toString())
                        .learnersList(peerIds(learners))
                        .sequenceToken(sequenceToken)
                        .build(),
                TargetPeerStrategy.LEADER,
                timeoutMillis
        ).thenAccept(resp -> this.learners = parsePeerList(resp.newLearnersList()));
    }

    @Override
    public CompletableFuture<Void> resetLearners(Collection<Peer> learners, long sequenceToken, long timeoutMillis) {
        return commandExecutor.<LearnersOpResponse>send(
                targetPeer -> MESSAGES_FACTORY.resetLearnersRequest()
                        .leaderId(peerId(targetPeer))
                        .groupId(groupId.toString())
                        .learnersList(peerIds(learners))
                        .sequenceToken(sequenceToken)
                        .build(),
                TargetPeerStrategy.LEADER,
                timeoutMillis
        ).thenAccept(resp -> this.learners = parsePeerList(resp.newLearnersList()));
    }

    @Override
    public CompletableFuture<Void> snapshot(Peer peer, boolean forced, long timeoutMillis) {
        return commandExecutor.send(
                targetPeer -> MESSAGES_FACTORY.snapshotRequest()
                        .peerId(peerId(peer))
                        .groupId(groupId.toString())
                        .forced(forced)
                        .build(),
                TargetPeerStrategy.SPECIFIC,
                peer,
                timeoutMillis
        ).thenApply(unused -> null);
    }

    @Override
    public CompletableFuture<Void> transferLeadership(Peer newLeader, long timeoutMillis) {
        return commandExecutor.send(
                targetPeer -> MESSAGES_FACTORY.transferLeaderRequest()
                        .groupId(groupId.toString())
                        .leaderId(peerId(targetPeer))
                        .peerId(peerId(newLeader))
                        .build(),
                TargetPeerStrategy.LEADER,
                timeoutMillis
        ).thenAccept(resp -> commandExecutor.setLeader(newLeader));
    }

    @Override
    public void shutdown() {
        // Remove topology event handler to prevent new topology events from triggering activity.
        clusterService.topologyService().removeEventHandler(topologyEventHandler);

        // Remove leader election listener to prevent memory leaks and stop receiving events.
        eventsClientListener.removeLeaderElectionListener(groupId, generalLeaderElectionListener);

        // Stop the command executor - blocks new run() calls, cancels leader waiters.
        commandExecutor.shutdown(stoppingExceptionFactory.create("Raft client is stopping [groupId=" + groupId() + "]."));

        // Unsubscribe from leader updates with timeout to ensure clean shutdown.
        finishSubscriptions();
    }

    @Override
    public CompletableFuture<Long> readIndex(long timeoutMillis) {
        return commandExecutor.<ReadIndexResponse>send(
                peer -> MESSAGES_FACTORY.readIndexRequest()
                        .groupId(groupId.toString())
                        .peerId(peer.consistentId())
                        .serverId(peer.consistentId())
                        .build(),
                TargetPeerStrategy.LEADER,
                timeoutMillis
        ).thenApply(ReadIndexResponse::index);
    }

    @Override
    public ClusterService clusterService() {
        return clusterService;
    }

    @Override
    public void updateConfiguration(PeersAndLearners configuration) {
        this.peers = List.copyOf(configuration.peers());
        this.learners = List.copyOf(configuration.learners());
        commandExecutor.setLeader(null);
    }

    @Override
    public void markAsStopping() {
        commandExecutor.markAsStopping();
    }

    /**
     * Leader election handler.
     */
    private static class ServerEventHandler implements LeaderElectionListener {
        private final Executor executor;

        /** Future chain for async notifications. */
        private CompletableFuture<Void> fut = nullCompletedFuture();

        /** A leader elected callback. */
        private final ArrayList<LeaderElectionListener> callbacks = new ArrayList<>();

        /** Last elected leader. */
        private InternalClusterNode leaderNode = null;

        /** Term of the last elected leader. */
        private long leaderTerm = -1;

        /**
         * Constructor.
         *
         * @param executor Executor.
         */
        ServerEventHandler(Executor executor) {
            this.executor = executor;
        }

        /**
         * Chains an async action to the future, starting a new chain if the current one is done.
         */
        private void chainAsync(Runnable action) {
            if (fut.isDone()) {
                fut = runAsync(action, executor);
            } else {
                fut = fut.thenRunAsync(action, executor);
            }
        }

        /**
         * Notifies about a new leader elected, if it did not make before.
         *
         * @param node Node.
         * @param term Term.
         */
        @Override
        public synchronized void onLeaderElected(InternalClusterNode node, long term) {
            if (term > leaderTerm) {
                leaderTerm = term;
                leaderNode = node;

                if (callbacks.isEmpty()) {
                    return;
                }

                ArrayList<LeaderElectionListener> listeners = new ArrayList<>(callbacks);

                // Avoid notifying in the synchronized block.
                chainAsync(() -> {
                    for (LeaderElectionListener listener : listeners) {
                        listener.onLeaderElected(node, term);
                    }
                });
            }
        }

        synchronized void addCallbackAndNotify(LeaderElectionListener callback) {
            callbacks.add(callback);

            if (leaderTerm != -1) {
                long finalLeaderTerm = this.leaderTerm;
                InternalClusterNode finalLeaderNode = this.leaderNode;

                // Notify about the current leader outside the synchronized block.
                chainAsync(() -> callback.onLeaderElected(finalLeaderNode, finalLeaderTerm));
            }
        }

        synchronized void removeCallbackAndNotify(LeaderElectionListener callback) {
            callbacks.remove(callback);
        }
    }
}
