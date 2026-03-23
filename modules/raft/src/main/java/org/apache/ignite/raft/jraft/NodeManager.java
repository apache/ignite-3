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

package org.apache.ignite.raft.jraft;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.raft.jraft.core.Scheduler;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.CoalescedHeartbeatRequestBuilder;
import org.apache.ignite.raft.jraft.rpc.InvokeCallback;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcClient;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.CoalescedHeartbeatResponse;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.util.OnlyForTest;

/**
 * Raft nodes manager.
 */
public class NodeManager implements Lifecycle<NodeOptions> {
    private static final IgniteLogger LOG = Loggers.forClass(NodeManager.class);

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final ConcurrentMap<NodeId, Node> nodeMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<Node>> groupMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<PeerId, Queue<Object[]>> coalesced = new ConcurrentHashMap<>();

    /** Node options. */
    private NodeOptions options;
    /** Task scheduler. */
    private Scheduler scheduler;
    /** Rpc client. */
    private final RpcClient rpcClient;
    /** Message factory. */
    private RaftMessagesFactory messagesFactory;
    /** Predicate to block a heartbeat messages. */
    private BiPredicate<Message, PeerId> blockPred;

    public NodeManager(ClusterService service) {
        rpcClient = new IgniteRpcClient(service);
    }

    @Override
    public boolean init(NodeOptions opts) {
        options = opts;
        scheduler = opts.getScheduler();
        messagesFactory = opts.getRaftMessagesFactory();

        // TODO: IGNITE-24789 Single trigger for all RAFT heartbeat in node.
        scheduler.schedule(this::onSentHeartbeat , opts.getElectionTimeoutMs(), TimeUnit.MILLISECONDS);

        return true;
    }

    @Override
    public void shutdown() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        rpcClient.shutdown();
    }

    public void blockMessages(BiPredicate<Message, PeerId> predicate) {
        this.blockPred = predicate;
    }

    public void stopBlock() {
        this.blockPred = null;
    }

    /**
     * Sends a heartbeat request.
     */
    private void onSentHeartbeat() {
        for (PeerId remote : coalesced.keySet()) {
            coalesced.computeIfPresent(remote, (peer, queue) -> {
                if (!queue.isEmpty()) {
                    CoalescedHeartbeatRequestBuilder builder = messagesFactory.coalescedHeartbeatRequest();
                    ArrayList<AppendEntriesRequest> list = new ArrayList<>();
                    builder.messages(list);

                    Object[] req;

                    List<CompletableFuture<Message>> futs = new ArrayList<>();
                    ArrayList<Object[]> blocked = new ArrayList<>();

                    while ((req = queue.poll()) != null) {
                        var msg = (AppendEntriesRequest) req[0];

                        if (blockPred != null && blockPred.test(msg, peer)) {
                            blocked.add(req);

                            continue;
                        }

                        builder.messages().add(msg);

                        futs.add((CompletableFuture<Message>) req[1]);
                    }

                    queue.addAll(blocked);

                    try {
                        rpcClient.invokeAsync(peer, builder.build(), null, new InvokeCallback() {
                            @Override
                            public void complete(Object result, Throwable err) {
                                if (err != null) {
                                    for (CompletableFuture<Message> fut : futs) {
                                        fut.completeExceptionally(err);
                                    }

                                    return;
                                }

                                CoalescedHeartbeatResponse resp = (CoalescedHeartbeatResponse) result;

                                assert resp.messages().size() == futs.size();

                                int i = 0;
                                for (Message message : resp.messages()) {
                                    futs.get(i++).complete(message); // Future completion will trigger callbacks.
                                }

                            }

                            @Override
                            public Executor executor() {
                                return options.getStripedExecutor().next();
                            }
                        }, options.getElectionTimeoutMs() / 2);
                    } catch (Exception e) {
                        LOG.error("Failed to send heartbeat message to remote node [remote={}].", e, peer);

                        for (CompletableFuture<Message> fut : futs) {
                            fut.completeExceptionally(e);
                        }
                    }
                }

                return queue;
            });
        }

        if (!stopGuard.get()) {
            scheduler.schedule(this::onSentHeartbeat, options.getElectionTimeoutMs() / 4, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Adds a node.
     */
    public boolean add(final Node node) {
        final NodeId nodeId = node.getNodeId();
        if (this.nodeMap.putIfAbsent(nodeId, node) == null) {
            final String groupId = node.getGroupId();
            List<Node> nodes = this.groupMap.get(groupId);
            if (nodes == null) {
                nodes = new CopyOnWriteArrayList<>();
                List<Node> existsNode = this.groupMap.putIfAbsent(groupId, nodes);
                if (existsNode != null) {
                    nodes = existsNode;
                }
            }
            nodes.add(node);
            return true;
        }
        return false;
    }

    /**
     * Clear the states, for test
     */
    @OnlyForTest
    public void clear() {
        this.groupMap.clear();
        this.nodeMap.clear();
    }

    /**
     * Remove a node.
     */
    public boolean remove(final Node node) {
        if (this.nodeMap.remove(node.getNodeId(), node)) {
            PeerId peerId = node.getNodeId().getPeerId();

            for (PeerId remote : coalesced.keySet()) {
                if (remote.equals(peerId.getConsistentId())) {
                    coalesced.remove(remote);
                }
            }

            List<Node> nodes = this.groupMap.get(node.getGroupId());

            if (nodes != null) {
                return nodes.remove(node);
            }
        }

        return false;
    }

    /**
     * Get node by groupId and peer.
     */
    public Node get(final String groupId, final PeerId peerId) {
        return this.nodeMap.get(new NodeId(groupId, peerId));
    }

    /**
     * Get all nodes in a raft group.
     */
    public List<Node> getNodesByGroupId(final String groupId) {
        return this.groupMap.get(groupId);
    }

    /**
     * Get all nodes
     */
    public List<Node> getAllNodes() {
        return this.groupMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    public CompletableFuture<Message> enqueue(PeerId to, Message request) {
        CompletableFuture<Message> fut = new CompletableFuture<>();

        coalesced.computeIfAbsent(to, k -> new ConcurrentLinkedQueue<>())
                .add(new Object[]{request, fut});

        return fut;
    }

    public ConcurrentMap<PeerId, Queue<Object[]>> getCoalesced() {
        return coalesced;
    }
}
