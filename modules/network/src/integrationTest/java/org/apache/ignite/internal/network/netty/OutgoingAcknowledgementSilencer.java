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

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.ignite.internal.network.recovery.message.AcknowledgementMessage;
import org.apache.ignite.network.OutNetworkObject;

/**
 * {@link io.netty.channel.ChannelOutboundHandler} that drops outgoing {@link AcknowledgementMessage}s.
 */
@Sharable
public class OutgoingAcknowledgementSilencer extends ChannelOutboundHandlerAdapter {
    /** Name of this handler. */
    public static final String NAME = "acknowledgement-silencer";

    private volatile boolean silenceAcks = true;

    /**
     * Stops dropping outgoing {@link AcknowledgementMessage}s.
     */
    public void stopSilencing() {
        silenceAcks = false;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        OutNetworkObject out = (OutNetworkObject) msg;

        if (silenceAcks && out.networkMessage() instanceof AcknowledgementMessage) {
            promise.setSuccess();
            return;
        }

        super.write(ctx, msg, promise);
    }
}
