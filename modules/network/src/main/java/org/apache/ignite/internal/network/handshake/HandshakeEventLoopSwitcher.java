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

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.EventLoop;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * A class responsible for managing the assignment of Netty channels to event loops
 * and switching channels between event loops when necessary.
 */
public class HandshakeEventLoopSwitcher {
    private static final IgniteLogger LOG = Loggers.forClass(HandshakeEventLoopSwitcher.class);

    /** List of available event loops. */
    private final List<EventLoop> executors;
    /** Map to track the number of channels assigned to each event loop. */
    private final HashMap<Integer, Set<ChannelId>> channelCountMap;

    /**
     * Constructs a new instance of HandshakeEventLoopSwitcher.
     *
     * @param eventLoops The list of event loops to manage.
     */
    public HandshakeEventLoopSwitcher(List<EventLoop> eventLoops) {
        this.executors = eventLoops;
        this.channelCountMap = new HashMap<>(eventLoops.size());
    }

    /**
     * Switches the event loop of a given channel if needed.
     *
     * @param channel The channel to potentially switch to a different event loop.
     * @param afterSwitching A callback to execute after the switching operation.
     */
    public void switchEventLoopIfNeeded(Channel channel, Runnable afterSwitching) {
        EventLoop targetEventLoop = eventLoopForKey(channel.id());

        if (targetEventLoop != channel.eventLoop()) {
            channel.deregister().addListener(deregistrationFuture -> {
                if (!deregistrationFuture.isSuccess()) {
                    LOG.error("Cannot deregister a channel from an event loop", deregistrationFuture.cause());

                    channel.close();

                    return;
                }

                targetEventLoop.register(channel).addListener(registrationFuture -> {
                    if (!registrationFuture.isSuccess()) {
                        LOG.error("Cannot register a channel with an event loop", registrationFuture.cause());

                        channel.close();

                        return;
                    }

                    channel.closeFuture().addListener(future -> {
                        channelUnregistered(channel);
                    });

                    afterSwitching.run();
                });
            });
        } else {
            afterSwitching.run();
        }
    }

    /**
     * Determines the appropriate event loop for a given channel ID.
     *
     * @param channelId The ID of the channel.
     * @return The selected event loop for the channel.
     */
    private synchronized EventLoop eventLoopForKey(ChannelId channelId) {
        int minCnt = channelCountMap.getOrDefault(0, Set.of()).size();
        int index = 0;

        for (int i = 1; i < executors.size(); i++) {
            Set<ChannelId> channelIds = channelCountMap.getOrDefault(i, Set.of());

            if (channelIds.contains(channelId)) {
                return executors.get(index);
            }

            int cnt = channelIds.size();

            if (cnt < minCnt) {
                minCnt = cnt;
                index = i;
            }
        }


        channelCountMap.computeIfAbsent(index, key -> new HashSet<>()).add(channelId);

        EventLoop eventLoop = executors.get(index);

        LOG.debug("Channel mapped to the loop [channelId={}, eventLoopIndex={}]", channelId, index);

        return eventLoop;
    }

    /**
     * Removes a channel from the event loop tracking map when it is unregistered.
     *
     * @param channel The channel to unregister.
     */
    private synchronized void channelUnregistered(Channel channel) {
        for (Set<ChannelId> channelIds : channelCountMap.values()) {
            if (channelIds.remove(channel.id())) {
                break;
            }
        }
    }
}
