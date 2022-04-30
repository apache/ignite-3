/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.network.NetworkMessage;

/**
 * Handshake operation manager.
 */
public interface HandshakeManager {
    /**
     * Initializes the handshake manager.
     *
     * @param handlerContext Channel handler context.
     */
    void onInit(ChannelHandlerContext handlerContext);

    /**
     * Handles an event of the connection opening.
     */
    default void onConnectionOpen() {
        // No-op.
    }

    /**
     * Handles an incoming message.
     *
     * @param message Message to handle.
     */
    void onMessage(NetworkMessage message);

    /**
     * Returns future that represents the handshake operation.
     *
     * @return Future that represents the handshake operation.
     */
    CompletableFuture<NettySender> handshakeFuture();
}
