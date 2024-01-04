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

package org.apache.ignite.internal.network.recovery;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.OutNetworkObject;
import org.apache.ignite.network.RecipientLeftException;
import org.jetbrains.annotations.Nullable;

/**
 * Recovery protocol descriptor.
 */
public class RecoveryDescriptor {
    /** Unacknowledged messages. */
    private final Queue<OutNetworkObject> unacknowledgedMessages;

    /** Count of sent messages. */
    private long sentCount;

    /** Count of acknowledged sent messages. */
    private long acknowledgedCount;

    /** Count of received messages. */
    private long receivedCount;

    /** Some context around current owner channel of this descriptor. */
    private final AtomicReference<DescriptorAcquiry> channelHolder = new AtomicReference<>();

    /** Event loop of the channel that was (or still is) the most recent owner of this descriptor. */
    private final AtomicReference<EventLoop> lastEventLoop = new AtomicReference<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param queueLimit Maximum size of unacknowledged messages queue.
     */
    public RecoveryDescriptor(int queueLimit) {
        this.unacknowledgedMessages = new ArrayDeque<>(queueLimit);
    }

    /**
     * Returns count of received messages.
     *
     * @return Count of received messages.
     */
    public long receivedCount() {
        return receivedCount;
    }

    /**
     * Acknowledges that sent messages were received by the remote node.
     *
     * @param messagesReceivedByRemote Number of all messages received by the remote node.
     */
    public void acknowledge(long messagesReceivedByRemote) {
        while (acknowledgedCount < messagesReceivedByRemote) {
            OutNetworkObject req = unacknowledgedMessages.poll();

            assert req != null;

            req.acknowledge();

            acknowledgedCount++;
        }
    }

    /**
     * Returns the number of the messages unacknowledged by the remote node.
     *
     * @return The number of the messages unacknowledged by the remote node.
     */
    public int unacknowledgedCount() {
        long res = sentCount - acknowledgedCount;
        int size = unacknowledgedMessages.size();

        assert res >= 0;
        assert res == size;

        return size;
    }

    /**
     * Returns unacknowledged messages.
     *
     * @return Unacknowledged messages.
     */
    public List<OutNetworkObject> unacknowledgedMessages() {
        return new ArrayList<>(unacknowledgedMessages);
    }

    /**
     * Tries to add a sent message to the unacknowledged queue.
     *
     * @param msg Message.
     * @return {@code true} if the message was added, {@code false} if the descriptor is already closed.
     */
    public boolean add(OutNetworkObject msg) {
        if (!busyLock.enterBusy()) {
            return false;
        }

        try {
            msg.shouldBeSavedForRecovery(false);
            sentCount++;
            unacknowledgedMessages.add(msg);

            return true;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Handles the event of receiving a new message.
     *
     * @return Number of received messages.
     */
    public long onReceive() {
        receivedCount++;

        return receivedCount;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(RecoveryDescriptor.class, this);
    }

    /**
     * Release this descriptor.
     *
     * @param ctx Channel handler context.
     */
    public void release(ChannelHandlerContext ctx) {
        DescriptorAcquiry oldAcquiry = channelHolder.getAndUpdate(acquiry -> {
            if (acquiry != null && acquiry.channel() == ctx.channel()) {
                return null;
            }

            return acquiry;
        });

        if (oldAcquiry != null && oldAcquiry.channel() == ctx.channel()) {
            // We have successfully released the descriptor.
            // Let's mark the clinch resolved just in case.
            oldAcquiry.markClinchResolved();
        }
    }

    /**
     * Acquire this descriptor.
     *
     * @param ctx Channel handler context.
     * @param handshakeCompleteFuture Future that gets completed when the corresponding handshake completes.
     */
    public boolean acquire(ChannelHandlerContext ctx, CompletableFuture<NettySender> handshakeCompleteFuture) {
        boolean acquired = channelHolder.compareAndSet(null, new DescriptorAcquiry(ctx.channel(), handshakeCompleteFuture));

        if (acquired) {
            lastEventLoop.set(ctx.channel().eventLoop());
        }

        return acquired;
    }

    /**
     * Returns context around the channel that holds this descriptor.
     */
    @Nullable DescriptorAcquiry holder() {
        return channelHolder.get();
    }

    /**
     * Returns {@code toString()} representation of a {@link Channel}, that holds this descriptor.
     */
    String holderDescription() {
        DescriptorAcquiry acquiry = channelHolder.get();

        if (acquiry == null) {
            // This can happen if channel was already closed and it released the descriptor.
            return "No channel";
        }

        return acquiry.channel().toString();
    }

    /**
     * Makes this recovery descriptor closed for the purposes of sending messages. After it is closed, no calls
     * to {@link #add(OutNetworkObject)} will have any effect, they will return {@code false}.
     */
    public void close() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        RecipientLeftException ex = new RecipientLeftException();
        for (OutNetworkObject unackedMessageObj : unacknowledgedMessages) {
            unackedMessageObj.failAcknowledgement(ex);
        }

        unacknowledgedMessages.clear();
    }

    /**
     * Closes this descriptor in the event loop of the last channel that owned (or still owns) it.
     */
    public void closeInEventLoopOfLastOwner() {
        EventLoop eventLoop = lastEventLoop.get();

        if (eventLoop != null) {
            if (eventLoop.inEventLoop()) {
                close();
            } else {
                eventLoop.execute(this::close);
            }
        }
    }
}
