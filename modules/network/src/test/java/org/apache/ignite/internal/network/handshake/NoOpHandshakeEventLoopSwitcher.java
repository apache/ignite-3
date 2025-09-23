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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.netty.ChannelKey;
import org.apache.ignite.internal.util.CompletableFutures;

/**
 * A no-operation implementation of the HandshakeEventLoopSwitcher.
 * This class provides empty or default implementations for all methods,
 * effectively disabling any event loop switching behavior.
 */
public class NoOpHandshakeEventLoopSwitcher extends HandshakeEventLoopSwitcher {
    /**
     * Constructs a new instance of NoOpHandshakeEventLoopSwitcher.
     * This implementation does not require any event loops because it never switches an event loop,
     * so an empty list is passed to the superclass.
     */
    public NoOpHandshakeEventLoopSwitcher() {
        super(List.of());
    }

    @Override
    public CompletableFuture<Void> switchEventLoopIfNeeded(Channel channel) {
        return CompletableFutures.nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> switchEventLoopIfNeeded(Channel channel, ChannelKey channelKey) {
        return CompletableFutures.nullCompletedFuture();
    }

    @Override
    public synchronized void nodeLeftTopology(InternalClusterNode node) {
        // No operation for NoOpHandshakeEventLoopSwitcher
    }
}
