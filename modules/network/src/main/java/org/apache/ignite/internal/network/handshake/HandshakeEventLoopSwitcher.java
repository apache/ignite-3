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

package org.apache.ignite.internal.network.handshake;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.EventLoop;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.netty.ChannelKey;
import reactor.util.annotation.Nullable;

/**
 * A class responsible for managing the assignment of Netty channels to event loops
 * and switching channels between event loops when necessary.
 */
public class HandshakeEventLoopSwitcher {
    private static final IgniteLogger LOG = Loggers.forClass(HandshakeEventLoopSwitcher.class);

    /** List of available event loops. */
    private final List<EventLoop> executors;

    /** Map to track the number of channels assigned to each event loop. */
    private final Map<Integer, Set<ChannelId>> activeChannelMap;

    /**
     * Map to track channel reservations for specific communication connections.
     * The map prevents applying different event loops for the same chаnell  key.
     */
    private final Map<ChannelKey, Integer> channelReservationMap = new HashMap<>();

    /**
     * Constructs a new instance of HandshakeEventLoopSwitcher.
     *
     * @param eventLoops The list of event loops to manage.
     */
    public HandshakeEventLoopSwitcher(List<EventLoop> eventLoops) {
        this.executors = eventLoops;
        this.activeChannelMap = new HashMap<>(eventLoops.size());
    }

    /**
     * Switches the event loop of a given channel if needed.
     *
     * @param channel The channel to potentially switch to a different event loop.
     * @return A CompletableFuture that completes when the event loop is switched. The future completes in the target event loop.
     */
    public CompletableFuture<Void> switchEventLoopIfNeeded(Channel channel) {
        return switchEventLoopIfNeeded(channel, null);
    }

    /**
     * Switches the event loop of a given channel if needed.
     *
     * @param channel The channel to potentially switch to a different event loop.
     * @param channelKey The unique key identifying the channel. That is a logical identifier of the connection between two nodes
     *         (it would be {@code null} for a client-server connection). The purpose is to have strict message ordering in a reliable
     *         connection.
     * @return A CompletableFuture that completes when the event loop is switched. The future completes in the target event loop.
     */
    public CompletableFuture<Void> switchEventLoopIfNeeded(Channel channel, @Nullable ChannelKey channelKey) {
        ChannelId channelId = channel.id();

        EventLoop targetEventLoop = eventLoopForKey(channelId, channelKey);

        if (targetEventLoop != channel.eventLoop()) {
            CompletableFuture<Void> fut = new CompletableFuture<>();

            channel.deregister().addListener(deregistrationFuture -> {
                if (!deregistrationFuture.isSuccess()) {
                    LOG.error("Cannot deregister a channel from an event loop", deregistrationFuture.cause());

                    fut.completeExceptionally(deregistrationFuture.cause());

                    channel.close();

                    return;
                }

                targetEventLoop.register(channel).addListener(registrationFuture -> {
                    if (!registrationFuture.isSuccess()) {
                        LOG.error("Cannot register a channel with an event loop", registrationFuture.cause());

                        fut.completeExceptionally(registrationFuture.cause());

                        channel.close();

                        return;
                    }

                    channel.closeFuture().addListener(future -> {
                        channelUnregistered(channelId);
                    });

                    fut.complete(null);
                });
            });

            return fut;
        }

        return nullCompletedFuture();
    }

    /**
     * Determines the appropriate event loop for a given channel key.
     *
     * @param channelId The ID of the channel.
     * @param channelKey The unique key identifying the channel.
     * @return The selected event loop for the channel.
     */
    private synchronized EventLoop eventLoopForKey(ChannelId channelId, @Nullable ChannelKey channelKey) {
        if (channelKey != null) {
            Integer idx = channelReservationMap.get(channelKey);

            if (idx != null) {
                return executors.get(idx);
            }
        }

        int minCnt = Integer.MAX_VALUE;
        int index = 0;

        for (int i = 0; i < executors.size(); i++) {
            Set<ChannelId> channelIds = activeChannelMap.getOrDefault(i, Set.of());

            if (channelIds.contains(channelId)) {
                assert channelKey == null : "Channel key should be null if the channel is already registered in the event loop";

                return executors.get(index);
            }

            int cnt = channelIds.size();

            if (cnt < minCnt) {
                minCnt = cnt;
                index = i;

                if (cnt == 0) {
                    // If we found an event loop with no channels, we can stop searching.
                    break;
                }
            }
        }

        activeChannelMap.computeIfAbsent(index, key -> new HashSet<>()).add(channelId);

        if (channelKey != null) {
            channelReservationMap.put(channelKey, index);
        }

        EventLoop eventLoop = executors.get(index);

        LOG.debug("Channel mapped to the loop [channelId={}, channelKey={}, eventLoopIndex={}]", channelId, channelKey, index);

        return eventLoop;
    }

    /**
     * Removes a channel from the event loop tracking map when it is unregistered.
     *
     * @param channelId The unique ID identifying the channel to unregister.
     */
    private synchronized void channelUnregistered(ChannelId channelId) {
        for (Set<ChannelId> channelKeys : activeChannelMap.values()) {
            if (channelKeys.remove(channelId)) {
                break;
            }
        }
    }

    /**
     * Removes all channels associated with a node from the event loop tracking map
     * when the node leaves the topology.
     *
     * @param node The node that left the topology.
     */
    public synchronized void nodeLeftTopology(InternalClusterNode node) {
        channelReservationMap.entrySet().removeIf(entry -> entry.getKey().launchId().equals(node.id()));
    }
}
