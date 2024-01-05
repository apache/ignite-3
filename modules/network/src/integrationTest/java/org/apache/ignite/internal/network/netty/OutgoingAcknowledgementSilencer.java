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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.network.recovery.message.AcknowledgementMessage;
import org.apache.ignite.network.OutNetworkObject;

/**
 * {@link io.netty.channel.ChannelOutboundHandler} that drops outgoing {@link AcknowledgementMessage}s.
 */
@Sharable
public class OutgoingAcknowledgementSilencer extends ChannelOutboundHandlerAdapter {
    /** Name of this handler. */
    static final String NAME = "acknowledgement-silencer";

    private volatile boolean silenceAcks = true;

    private final AtomicInteger addedCount = new AtomicInteger();

    /**
     * Install this silencer on the given channels; it will drop {@link AcknowledgementMessage}s sent by them.
     *
     * @param senders Senders to install on.
     * @return The installed silencer.
     * @throws InterruptedException If interrupted while waiting for the installation to be completed.
     */
    public static OutgoingAcknowledgementSilencer installOn(Collection<NettySender> senders)
            throws InterruptedException {
        OutgoingAcknowledgementSilencer ackSilencer = new OutgoingAcknowledgementSilencer();

        for (NettySender sender : senders) {
            sender.channel().pipeline().addAfter(OutboundEncoder.NAME, NAME, ackSilencer);
        }

        ackSilencer.waitForAddedTo(senders.size());

        return ackSilencer;
    }

    /**
     * Stops dropping outgoing {@link AcknowledgementMessage}s.
     */
    public void stopSilencing() {
        silenceAcks = false;
    }

    /**
     * Waits for this silencer being added to the required number of pipelines.
     *
     * @param count Number of pipelines.
     * @throws InterruptedException If interrupted while waiting.
     */
    private void waitForAddedTo(int count) throws InterruptedException {
        waitForCondition(() -> addedCount.get() >= count, TimeUnit.SECONDS.toMillis(10));
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

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);

        addedCount.incrementAndGet();
    }
}
