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

import static java.util.concurrent.CompletableFuture.failedFuture;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Recovery protocol descriptor.
 *
 * <p>Main state held by a descriptor (unacked messages queue and counters) is not protected by synchronization primitives
 * to make it fast to access it (which happens on each message send/receive). Here is how thread-safety of these
 * accesses is maintained:
 *
 * <ol>
 *     <li>Each descriptor belongs to at most one owner at a time (usually, owners are Channels, but the last
 *     owner is ConnectionManager when it disposes a descriptor)</li>
 *     <li>Owner starts 'owning' a descriptor by acquiring it (using {@link #tryAcquire(ChannelHandlerContext, CompletableFuture)}
 *     or {@link #tryBlockForever(Exception)}) and stops owning it by releasing it with {@link #release(ChannelHandlerContext)}</li>
 *     <li>Only the owner can access non-volatile state of the descriptor, and only in the same thread in which it
 *     acquired it and in which it will release it</li>
 *     <li>Acquiry, accesses while owning and release happen in the same thread, so there is happens-before between them even without
 *     synchronization</li>
 *     <li>Release from a previous owner happens-before an acquiry by the next owner (because same {@link AtomicReference} is used
 *     to implement acquire/release in an atomic way)</li>
 *     <li>The last two items mean that all accesses to the non-synchronized state of a descriptor form a happens-before chain,
 *     so all effects of the earlier writes are visible to the subsequent accesses</li>
 * </ol>
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
     * <p>Must only be called from the thread in which this descriptor was acquired last time.
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
     * <p>Must only be called from the thread in which this descriptor was acquired last time.
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
     * <p>Must only be called from the thread in which this descriptor was acquired last time.
     *
     * @return Unacknowledged messages.
     */
    public List<OutNetworkObject> unacknowledgedMessages() {
        return new ArrayList<>(unacknowledgedMessages);
    }

    /**
     * Tries to add a sent message to the unacknowledged queue.
     *
     * <p>Must only be called from the thread in which this descriptor was acquired last time.
     *
     * @param msg Message.
     */
    public void add(OutNetworkObject msg) {
        msg.shouldBeSavedForRecovery(false);
        sentCount++;

        boolean added = unacknowledgedMessages.add(msg);
        assert added : "Wasn't added as the queue is full: " + msg.networkMessage();
    }

    /**
     * Handles the event of receiving a new message.
     *
     * <p>Must only be called from the thread in which this descriptor was acquired last time.
     *
     * @return Number of received messages.
     */
    public long onReceive() {
        receivedCount++;

        return receivedCount;
    }

    @Override
    public String toString() {
        return S.toString(RecoveryDescriptor.class, this);
    }

    /**
     * Releases this descriptor if it's been acquired by the given channel, otherwise does nothing.
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
     * Tries to acquire this descriptor.
     *
     * @param ctx Channel handler context.
     * @param handshakeCompleteFuture Future that gets completed when the corresponding handshake completes.
     * @return Whether the descriptor was successfully acquired.
     */
    public boolean tryAcquire(ChannelHandlerContext ctx, CompletableFuture<NettySender> handshakeCompleteFuture) {
        return doTryAcquire(ctx.channel(), handshakeCompleteFuture);
    }

    private boolean doTryAcquire(@Nullable Channel channel, CompletableFuture<NettySender> handshakeCompleteFuture) {
        return channelHolder.compareAndSet(null, new DescriptorAcquiry(channel, handshakeCompleteFuture));
    }

    /**
     * Tries to acquire this descriptor to never release it (as the counterpart node has left or this node is being stopped).
     *
     * @param ex Exception with which the handshake future will be completed.
     */
    public boolean tryBlockForever(Exception ex) {
        return doTryAcquire(null, failedFuture(ex));
    }

    /**
     * Returns whether this descriptor is blocked (that is, it is acquired by ConnectionManager to dispose the descriptor
     * and it will never be released).
     */
    public boolean isBlockedForever() {
        DescriptorAcquiry acquiry = channelHolder.get();
        return acquiry != null && acquiry.channel() == null;
    }

    /**
     * Returns context around the channel that holds this descriptor.
     */
    @Nullable public DescriptorAcquiry holder() {
        return channelHolder.get();
    }

    @Nullable public Channel holderChannel() {
        DescriptorAcquiry acquiry = holder();
        return acquiry == null ? null : acquiry.channel();
    }

    /**
     * Returns {@code toString()} representation of a {@link Channel}, that holds this descriptor.
     */
    String holderDescription() {
        DescriptorAcquiry acquiry = channelHolder.get();

        if (acquiry == null) {
            // This can happen if channel was already closed and it released the descriptor.
            return "No acquiry";
        }

        Channel channel = acquiry.channel();

        if (channel == null) {
            // This can happen if the descriptor has been blocked.
            return "Blocked (no channel)";
        }

        return channel.toString();
    }

    /**
     * Fails futures of all unacknowledged messages and clears the unacknowledged messages queue.
     *
     * <p>Must only be called from the thread in which this descriptor was blocked.
     */
    public void dispose(Exception exceptionToFailSendFutures) {
        for (OutNetworkObject unackedMessageObj : unacknowledgedMessages) {
            unackedMessageObj.failAcknowledgement(exceptionToFailSendFutures);
        }

        unacknowledgedMessages.clear();
    }
}
