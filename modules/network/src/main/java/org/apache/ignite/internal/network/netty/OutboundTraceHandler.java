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

import static io.opentelemetry.api.GlobalOpenTelemetry.getPropagators;
import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.network.message.TraceableMessageImpl;
import org.apache.ignite.network.OutNetworkObject;

/** Outbound handler that adds tracing headers to outgoing message. */
public class OutboundTraceHandler extends ChannelOutboundHandlerAdapter {
    /** Handler name. */
    public static final String NAME = "outbound-trace-handler";

    /** {@inheritDoc} */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (Span.current().getSpanContext().isValid()) {
            OutNetworkObject outNetworkObject = (OutNetworkObject) msg;

            var propagator = getPropagators().getTextMapPropagator();
            Map<String, String> headers = new HashMap<>(capacity(propagator.fields().size()));

            propagator.inject(Context.current(), headers, (carrier, key, val) -> carrier.put(key, val));

            msg = new OutNetworkObject(
                    TraceableMessageImpl.builder().headers(headers).message(outNetworkObject.networkMessage()).build(),
                    outNetworkObject.descriptors(),
                    outNetworkObject.shouldBeSavedForRecovery()
            );
        }

        super.write(ctx, msg, promise);
    }
}
