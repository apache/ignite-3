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

package org.apache.ignite.client.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Channel handler that closes the channel on {@link IdleStateEvent}.
 */
class IdleChannelHandler extends ChannelDuplexHandler {
    private static final IgniteLogger LOG = Loggers.forClass(IdleChannelHandler.class);

    private final long idleTimeout;

    private final ClientHandlerMetricSource metrics;

    IdleChannelHandler(long idleTimeout, ClientHandlerMetricSource metrics, long connectionId) {
        this.idleTimeout = idleTimeout;
        this.metrics = metrics;
    }

    /** {@inheritDoc} */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
            metrics.sessionsRejectedTimeoutIncrement();
            LOG.warn("Closing idle channel [idleTimeout=" + idleTimeout + ", remoteAddress=" + ctx.channel().remoteAddress() + ']');

            ctx.close();
        }
    }
}
