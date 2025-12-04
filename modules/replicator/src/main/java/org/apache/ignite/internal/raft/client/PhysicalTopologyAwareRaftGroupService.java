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
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ExceptionFactory;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeerUnavailableException;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.ThrottlingContextHolder;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SubscriptionLeaderChangeRequest;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.jetbrains.annotations.Nullable;

/**
 * The RAFT group service is based on the cluster physical topology. This service has ability to subscribe of a RAFT group leader update.
 */
public class PhysicalTopologyAwareRaftGroupService implements RaftGroupService {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TopologyAwareRaftGroupService.class);

    /** Raft message factory. */
    private static final RaftMessagesFactory MESSAGES_FACTORY = Loza.FACTORY;

    /** General leader election listener. */
    private final ServerEventHandler generalLeaderElectionListener;

    /** Failure manager. */
    private final FailureManager failureManager;

    /** Cluster service. */
    private final ClusterService clusterService;

    /** RPC RAFT client. */
    private final RaftGroupService raftClient;

    /** Executor to invoke RPC requests. */
    private final Executor executor;

    /** RAFT configuration. */
    private final RaftConfiguration raftConfiguration;

    /**
     * Constructor.
     *
     * @param failureManager Failure manager.
     * @param clusterService Cluster service.
     * @param executor Executor to invoke RPC requests and notify listeners.
     * @param raftConfiguration RAFT configuration.
     * @param raftClient RPC RAFT client.
     * @param eventsClientListener Events client listener.
     */
    private PhysicalTopologyAwareRaftGroupService(
            FailureManager failureManager,
            ClusterService clusterService,
            Executor executor,
            RaftConfiguration raftConfiguration,
            RaftGroupService raftClient,
            RaftGroupEventsClientListener eventsClientListener
    ) {
        this.failureManager = failureManager;
        this.clusterService = clusterService;
        this.executor = executor;
        this.raftConfiguration = raftConfiguration;
        this.raftClient = raftClient;

        this.generalLeaderElectionListener = new ServerEventHandler(executor);

        eventsClientListener.addLeaderElectionListener(raftClient.groupId(), generalLeaderElectionListener);

        clusterService.topologyService().addEventHandler(new TopologyEventHandler() {
            @Override
            public void onAppeared(InternalClusterNode member) {
                CompletableFuture<Boolean> fut = changeNodeSubscriptionIfNeed(member, true);

                requestLeaderManually(clusterService, executor, raftClient, fut);
            }
        });

        ArrayList<CompletableFuture<?>> futures = new ArrayList<>();

        for (InternalClusterNode member : clusterService.topologyService().allMembers()) {
            futures.add(changeNodeSubscriptionIfNeed(member, true));
        }

        requestLeaderManually(clusterService, executor, raftClient, CompletableFutures.allOf(futures));
    }

    /**
     * Requests the leader information for the RAFT group.
     * TODO: IGNITE-27256 Remove the method after implementing a notification after subscription.
     *
     * @param clusterService Cluster service to retrieve topology information.
     * @param executor Executor to run asynchronous tasks.
     * @param raftClient RAFT client to interact with the RAFT group.
     * @param subscriptionsFut Future representing the completion of subscription updates.
     */
    private void requestLeaderManually(
            ClusterService clusterService,
            Executor executor,
            RaftGroupService raftClient,
            CompletableFuture<?> subscriptionsFut
    ) {
        subscriptionsFut.thenRunAsync(
                () -> raftClient.refreshAndGetLeaderWithTerm().whenCompleteAsync(
                        (leaderWithTerm, throwable) -> {
                            if (throwable != null) {
                                LOG.warn("Could not refresh and get leader with term [grp={}].", groupId(), throwable);
                            }

                            InternalClusterNode leaderHost = clusterService.topologyService()
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
            SubscriptionLeaderChangeRequest msg = subscriptionLeaderChangeRequest(subscribe);

            return sendMessage(member, msg).whenComplete((isSent, err) -> {
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
     * @param factory Factory for creating RAFT messages.
     * @param raftConfiguration Configuration for the RAFT group.
     * @param configuration Peers and learners configuration.
     * @param executor Executor for asynchronous tasks.
     * @param eventsClientListener Listener for RAFT group events.
     * @param cmdMarshaller Marshaller for RAFT commands.
     * @param stoppingExceptionFactory Factory for creating stopping exceptions.
     * @param throttlingContextHolder Context holder for throttling.
     * @param failureManager Manager for handling failures.
     * @return A new instance of PhysicalTopologyAwareRaftGroupService.
     */
    public static PhysicalTopologyAwareRaftGroupService start(
            ReplicationGroupId groupId,
            ClusterService cluster,
            RaftMessagesFactory factory,
            RaftConfiguration raftConfiguration,
            PeersAndLearners configuration,
            ScheduledExecutorService executor,
            RaftGroupEventsClientListener eventsClientListener,
            Marshaller cmdMarshaller,
            ExceptionFactory stoppingExceptionFactory,
            ThrottlingContextHolder throttlingContextHolder,
            FailureManager failureManager
    ) {
        return new PhysicalTopologyAwareRaftGroupService(
                failureManager,
                cluster,
                executor,
                raftConfiguration,
                RaftGroupServiceImpl.start(
                        groupId,
                        cluster,
                        factory,
                        raftConfiguration,
                        configuration,
                        executor,
                        cmdMarshaller,
                        stoppingExceptionFactory,
                        throttlingContextHolder
                ),
                eventsClientListener
        );
    }

    private void finishSubscriptions() {
        for (InternalClusterNode member : clusterService.topologyService().allMembers()) {
            changeNodeSubscriptionIfNeed(member, false);
        }
    }

    public void subscribeLeader(LeaderElectionListener callback) {
        generalLeaderElectionListener.addCallbackAndNotify(callback);
    }

    public void unsubscribeLeader(LeaderElectionListener callback) {
        generalLeaderElectionListener.removeCallbackAndNotify(callback);
    }

    private SubscriptionLeaderChangeRequest subscriptionLeaderChangeRequest(boolean subscribe) {
        return MESSAGES_FACTORY.subscriptionLeaderChangeRequest()
                .groupId(groupId())
                .subscribe(subscribe)
                .build();
    }

    private CompletableFuture<Boolean> sendMessage(InternalClusterNode node, SubscriptionLeaderChangeRequest msg) {
        var msgSendFut = new CompletableFuture<Boolean>();

        sendWithRetry(node, msg, msgSendFut);

        return msgSendFut;
    }

    private void sendWithRetry(InternalClusterNode node, SubscriptionLeaderChangeRequest msg, CompletableFuture<Boolean> msgSendFut) {
        Long responseTimeout = raftConfiguration.responseTimeoutMillis().value();

        clusterService.messagingService().invoke(node, msg, responseTimeout).whenCompleteAsync((unused, invokeThrowable) -> {
            if (invokeThrowable == null) {
                msgSendFut.complete(true);

                return;
            }

            Throwable invokeCause = unwrapCause(invokeThrowable);
            if (!msg.subscribe()) {
                // We don't want to propagate exceptions when unsubscribing (if it's not an Error!).
                if (invokeCause instanceof Error) {
                    msgSendFut.completeExceptionally(invokeThrowable);
                } else {
                    LOG.debug("An exception while trying to unsubscribe.", invokeThrowable);

                    msgSendFut.complete(false);
                }
            } else if (recoverable(invokeCause)) {
                sendWithRetry(node, msg, msgSendFut);
            } else if (invokeCause instanceof RecipientLeftException) {
                LOG.info(
                        "Could not subscribe to leader update from a specific node, because the node had left the cluster: [node={}].",
                        node
                );

                msgSendFut.complete(false);
            } else {
                if (!(invokeCause instanceof NodeStoppingException)) {
                    LOG.error("Could not send the subscribe message to the node: [node={}, msg={}].", invokeThrowable, node, msg);
                }

                msgSendFut.completeExceptionally(invokeThrowable);
            }
        }, executor);
    }

    private static boolean recoverable(Throwable t) {
        t = unwrapCause(t);

        return t instanceof TimeoutException
                || t instanceof IOException
                || t instanceof PeerUnavailableException
                || t instanceof RecipientLeftException;
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
    public List<Peer> peers() {
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
    public CompletableFuture<Void> addPeer(Peer peer, long sequenceToken) {
        return raftClient.addPeer(peer, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> removePeer(Peer peer, long sequenceToken) {
        return raftClient.removePeer(peer, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> changePeersAndLearners(PeersAndLearners peersAndLearners, long term, long sequenceToken) {
        return raftClient.changePeersAndLearners(peersAndLearners, term, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> changePeersAndLearnersAsync(PeersAndLearners peersAndLearners, long term, long sequenceToken) {
        return raftClient.changePeersAndLearnersAsync(peersAndLearners, term, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> addLearners(Collection<Peer> learners, long sequenceToken) {
        return raftClient.addLearners(learners, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> removeLearners(Collection<Peer> learners, long sequenceToken) {
        return raftClient.removeLearners(learners, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> resetLearners(Collection<Peer> learners, long sequenceToken) {
        return raftClient.resetLearners(learners, sequenceToken);
    }

    @Override
    public CompletableFuture<Void> snapshot(Peer peer, boolean forced) {
        return raftClient.snapshot(peer, forced);
    }

    @Override
    public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        return raftClient.transferLeadership(newLeader);
    }

    @Override
    public void shutdown() {
        finishSubscriptions();

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

    @Override
    public void updateConfiguration(PeersAndLearners configuration) {
        raftClient.updateConfiguration(configuration);
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd) {
        return raftClient.run(cmd);
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd, long timeoutMillis) {
        return raftClient.run(cmd, timeoutMillis);
    }

    /**
     * Leader election handler.
     */
    private static class ServerEventHandler implements LeaderElectionListener {
        private final Executor executor;

        CompletableFuture<Void> fut = CompletableFutures.nullCompletedFuture();

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
                if (fut.isDone()) {
                    fut = runAsync(() -> {
                        for (LeaderElectionListener listener : listeners) {
                            listener.onLeaderElected(node, term);
                        }
                    }, executor);
                } else {
                    fut = fut.thenRunAsync(() -> {
                        for (LeaderElectionListener listener : listeners) {
                            listener.onLeaderElected(node, term);
                        }
                    }, executor);
                }
            }
        }

        synchronized void addCallbackAndNotify(LeaderElectionListener callback) {
            callbacks.add(callback);

            if (leaderTerm != -1) {

                // Notify about the current leader outside of the synchronized block.
                if (fut.isDone()) {
                    fut = runAsync(() -> callback.onLeaderElected(leaderNode, leaderTerm), executor);
                } else {
                    fut = fut.thenRunAsync(() -> callback.onLeaderElected(leaderNode, leaderTerm), executor);
                }
            }
        }

        synchronized void removeCallbackAndNotify(LeaderElectionListener callback) {
            callbacks.remove(callback);
        }
    }
}
