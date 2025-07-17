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
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptor;
import org.apache.ignite.internal.network.recovery.message.AcknowledgementMessage;
import org.apache.ignite.internal.util.FastTimestamps;

/**
 * Inbound handler that handles incoming acknowledgement messages and sends acknowledgement messages for other messages.
 */
public class InboundRecoveryHandler extends ChannelInboundHandlerAdapter {
    private static final IgniteLogger LOG = Loggers.forClass(InboundRecoveryHandler.class);

    /** Handler name. */
    public static final String NAME = "inbound-recovery-handler";

    /** Recovery descriptor. */
    private final RecoveryDescriptor descriptor;

    /** Messages factory. */
    private final NetworkMessagesFactory factory;

    ThreadLocal<HIstMsgInfo> locHisto = new ThreadLocal<>() {;
        @Override
        protected HIstMsgInfo initialValue() {
            return new HIstMsgInfo();
        }
    };

    private static class HIstMsgInfo {
        /** Date format for thread dumps. */
        final DateTimeFormatter THREAD_DUMP_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

        HashMap<String, Integer> map = new HashMap();

        long timestamp;

        void add(String msg) {
            map.compute(msg, (k, v) -> v == null ? 1 : v + 1);
        }

        void println() {
            if (FastTimestamps.coarseCurrentTimeMillis() - timestamp > 10_000) {
                LOG.info("PVD:: Message histogram: {} messages, last timestamp: {}", map,
                        THREAD_DUMP_FMT.format(Instant.ofEpochMilli(timestamp)));

                timestamp = System.currentTimeMillis();
                map.clear();
            }
        }
    }

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

        var hist = locHisto.get();
        hist.add(msg.getClass().getName());
        hist.println();

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
