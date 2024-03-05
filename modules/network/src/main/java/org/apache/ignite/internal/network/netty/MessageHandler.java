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

package org.apache.ignite.internal.network.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.function.Consumer;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.recovery.message.AcknowledgementMessage;
import org.apache.ignite.internal.network.recovery.message.ProbeMessage;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.network.ClusterNode;

/**
 * Network message handler that delegates handling to {@link #messageListener}.
 */
public class MessageHandler extends ChannelInboundHandlerAdapter {
    /** Handler name. */
    public static final String NAME = "message-handler";

    /** Message listener. */
    private final Consumer<InNetworkObject> messageListener;

    private final ClusterNode remoteNode;

    private final short connectionIndex;

    private final PerSessionSerializationService serializationService;

    /**
     * Constructor.
     *
     * @param messageListener Message listener.
     * @param remoteNode Remote node (from which the messages are coming).
     * @param connectionIndex Connection index (aka channel ID).
     * @param serializationService Serialization service.
     */
    public MessageHandler(
            Consumer<InNetworkObject> messageListener,
            ClusterNode remoteNode,
            short connectionIndex,
            PerSessionSerializationService serializationService
    ) {
        this.messageListener = messageListener;
        this.remoteNode = remoteNode;
        this.connectionIndex = connectionIndex;
        this.serializationService = serializationService;
    }

    /** {@inheritDoc} */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        NetworkMessage message = (NetworkMessage) msg;

        if (notPayloadMessage(message)) {
            return;
        }

        messageListener.accept(
                new InNetworkObject(message, remoteNode, connectionIndex, serializationService.compositeDescriptorRegistry())
        );
    }

    private static boolean notPayloadMessage(NetworkMessage message) {
        return message instanceof AcknowledgementMessage || message instanceof ProbeMessage;
    }
}
