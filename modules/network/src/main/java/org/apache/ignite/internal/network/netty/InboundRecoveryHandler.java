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
import java.util.Collections;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptor;
import org.apache.ignite.internal.network.recovery.message.AcknowledgementMessage;

/**
 * Inbound handler that handles incoming acknowledgement messages and sends acknowledgement messages for other messages.
 */
public class InboundRecoveryHandler extends ChannelInboundHandlerAdapter {
    /** Handler name. */
    public static final String NAME = "inbound-recovery-handler";

    /** Recovery descriptor. */
    private final RecoveryDescriptor descriptor;

    /** Messages factory. */
    private final NetworkMessagesFactory factory;

    /**
     * Constructor.
     *
     * @param descriptor Recovery descriptor.
     * @param factory Message factory.
     */
    public InboundRecoveryHandler(RecoveryDescriptor descriptor, NetworkMessagesFactory factory) {
        this.descriptor = descriptor;
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        NetworkMessage message = (NetworkMessage) msg;

        if (message instanceof AcknowledgementMessage) {
            AcknowledgementMessage ackMessage = (AcknowledgementMessage) msg;
            long receivedMessages = ackMessage.receivedMessages();

            descriptor.acknowledge(receivedMessages);
        } else if (message.needAck()) {
            AcknowledgementMessage ackMsg = factory.acknowledgementMessage()
                    .receivedMessages(descriptor.onReceive()).build();

            ctx.channel().writeAndFlush(new OutNetworkObject(ackMsg, Collections.emptyList(), false));
        }

        super.channelRead(ctx, message);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        descriptor.release(ctx);

        super.channelInactive(ctx);
    }
}
