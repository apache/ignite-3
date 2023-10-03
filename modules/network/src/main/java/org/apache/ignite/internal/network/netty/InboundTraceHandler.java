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
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.ignite.internal.network.message.TraceableMessage;

/**
 * Inbound handler that handles incoming acknowledgement messages and sends acknowledgement messages for other messages.
 */
public class InboundTraceHandler extends ChannelInboundHandlerAdapter {
    /** Handler name. */
    public static final String NAME = "inbound-trace-handler";

    private static final TextMapGetter<TraceableMessage> GETTER = new TraceableMessageGetter();

    /** {@inheritDoc} */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof TraceableMessage)) {
            super.channelRead(ctx, msg);
            return;
        }

        TraceableMessage message = (TraceableMessage) msg;

        Context context = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), message, GETTER);

        try (Scope ignored = context.makeCurrent()) {
            super.channelRead(ctx, message.message());
        }
    }

    private static class TraceableMessageGetter implements TextMapGetter<TraceableMessage> {
        @Override
        public Iterable<String> keys(TraceableMessage carrier) {
            return carrier.headers().keySet();
        }

        @Override
        public String get(TraceableMessage carrier, String key) {
            return carrier.headers().get(key);
        }
    }
}
