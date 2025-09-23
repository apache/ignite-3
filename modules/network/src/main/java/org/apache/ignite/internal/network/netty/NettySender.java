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

import static org.apache.ignite.internal.network.netty.NettyUtils.toCompletableFuture;
import static org.apache.ignite.internal.util.CompletableFutures.isCompletedSuccessfully;

import io.netty.channel.Channel;
import io.netty.handler.stream.ChunkedInput;
import java.nio.channels.ClosedChannelException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptor;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.TestOnly;

/**
 * Wrapper for a Netty {@link Channel}, that uses {@link ChunkedInput} and {@link DirectMessageWriter} to send data.
 */
public class NettySender {
    private static final IgniteLogger LOG = Loggers.forClass(NettySender.class);

    /** Netty channel. */
    private final Channel channel;

    /** Launch id of the remote node. */
    private final UUID launchId;

    /** Consistent id of the remote node. */
    private final String consistentId;

    private final short channelId;

    @IgniteToStringExclude
    private final RecoveryDescriptor recoveryDescriptor;

    /**
     * Constructor.
     *
     * @param channel Netty channel.
     * @param launchId Launch id of the remote node.
     * @param consistentId Consistent id of the remote node.
     * @param channelId channel identifier.
     * @param recoveryDescriptor Descriptor corresponding to the current logical connection.
     */
    public NettySender(Channel channel, UUID launchId, String consistentId, short channelId, RecoveryDescriptor recoveryDescriptor) {
        this.channel = channel;
        this.launchId = launchId;
        this.consistentId = consistentId;
        this.channelId = channelId;
        this.recoveryDescriptor = recoveryDescriptor;
    }

    /**
     * Sends the message.
     *
     * <p>NB: the future returned by this method might be completed by a channel different from the channel encapsulated
     * in the current sender (this might happen if the 'current' channel gets closed and another one is opened
     * in the same logical connection and resends a not-yet-acknowledged message sent via the old channel).
     *
     * @param obj Network message wrapper.
     * @return Future of the send operation (that gets completed when the message gets acknowledged by the receiver).
     */
    @TestOnly
    public CompletableFuture<Void> send(OutNetworkObject obj) {
        return send(obj, () -> {});
    }

    /**
     * Sends the message.
     *
     * <p>NB: the future returned by this method might be completed by a channel different from the channel encapsulated
     * in the current sender (this might happen if the 'current' channel gets closed and another one is opened
     * in the same logical connection and resends a not-yet-acknowledged message sent via the old channel).
     *
     * @param obj Network message wrapper.
     * @param triggerChannelRecreation Used to trigger channel recreation (when it turns out that the underlying channel is closed
     *     and the connection recovery procedure has to be performed).
     * @return Future of the send operation (that gets completed when the message gets acknowledged by the receiver).
     */
    public CompletableFuture<Void> send(OutNetworkObject obj, Runnable triggerChannelRecreation) {
        if (!obj.networkMessage().needAck()) {
            // We don't care that the caller might get an exception like ClosedChannelException or that the message
            // will be lost if the channel is closed as it does not require to be acked.
            return toCompletableFuture(channel.writeAndFlush(obj));
        }

        // Write in event loop to make sure that, if a ClosedSocketException happens, we recover from it without exiting the event loop.
        // We need this to avoid message reordering due to switching from old channel to a new one.
        // Also, we ALWAYS execute the writes on the event loop (by adding them to the event loop queue) even if we are on
        // the event loop thread, to avoid another reordering.
        return toCompletableFuture(channel.eventLoop().submit(() -> writeWithRecovery(obj, channel, triggerChannelRecreation), null));
    }

    private void chainRecoverSendAfterChannelClosure(
            CompletableFuture<Void> writeFuture,
            OutNetworkObject obj,
            Channel currentChannel,
            Runnable triggerChannelRecreation
    ) {
        if (!isCompletedSuccessfully(writeFuture)) {
            writeFuture.whenComplete((res, ex) -> {
                if (ex instanceof ClosedChannelException) {
                    try {
                        recoverSendAfterChannelClosure(obj, currentChannel, triggerChannelRecreation);
                    } catch (RuntimeException | AssertionError e) {
                        LOG.error("An error while sending a message {}", e, obj.networkMessage());
                    }
                }
            });
        }
    }

    private void recoverSendAfterChannelClosure(OutNetworkObject obj, Channel currentChannel, Runnable triggerChannelRecreation) {
        assert NettyBootstrapFactory.isInNetworkThread() : "In a non-netty thread " + Thread.currentThread();

        // As we are in the channel event loop and all channels of the same logical connection use the same event loop,
        // we can be sure that nothing related to our channel, recovery descriptor and possible new channel can change
        // concurrently.

        Channel holderChannel = recoveryDescriptor.holderChannel();

        if (holderChannel == null || holderChannel == currentChannel) {
            // Our channel is being closed or is already completely closed, its pipeline might be (or not be) destroyed.
            // But no new channel acquired the descriptor, so we are alone.

            if (obj.shouldBeSavedForRecovery()) {
                // It was not saved yet (as normally OutboundRecoveryHandler adds it to the unacknowledged messages queue, but
                // the channel has been closed and its pipeline destroyed, so the handler did not get called).
                // Let's add it manually.
                recoveryDescriptor.add(obj);
            }

            // The message is in the unacked messages queue, so it is enough to make sure that a new channel will be opened.
            // Triggering it explicitly, otherwise this would only happen when next message is sent.
            triggerChannelRecreation.run();
        } else {
            // This logical connection is already riding another Channel. We must resend this message using the new Channel.
            // Both channels share the same event loop (hence, same Netty thread), so, if we do the resend while staying
            // in the same thread, we will keep the message ordering.

            // The message is already added to unacked message queue, or it will be added by the send, or by the continuation handling
            // ClosedSocketException that we recursively chain.
            writeWithRecovery(obj, holderChannel, triggerChannelRecreation);
        }
    }

    private void writeWithRecovery(OutNetworkObject obj, Channel channel, Runnable triggerChannelRecreation) {
        CompletableFuture<Void> writeFuture = toCompletableFuture(channel.writeAndFlush(obj));

        chainRecoverSendAfterChannelClosure(writeFuture, obj, channel, triggerChannelRecreation);
    }

    /**
     * Returns launch id of the remote node.
     *
     * @return Launch id of the remote node.
     */
    public UUID launchId() {
        return launchId;
    }

    /**
     * Returns consistent id of the remote node.
     *
     * @return Consistent id of the remote node.
     */
    public String consistentId() {
        return consistentId;
    }

    /**
     * Returns channel identifier.
     *
     * @return Channel identifier.
     */
    public short channelId() {
        return channelId;
    }

    /**
     * Closes channel asynchronously.
     *
     * @return Future of the close operation.
     */
    public CompletableFuture<Void> closeAsync() {
        return toCompletableFuture(this.channel.close());
    }

    /**
     * Returns {@code true} if the channel is open, {@code false} otherwise.
     *
     * @return {@code true} if the channel is open, {@code false} otherwise.
     */
    public boolean isOpen() {
        return this.channel.isOpen();
    }

    /**
     * Returns channel.
     *
     * @return Channel.
     */
    @TestOnly
    public Channel channel() {
        return channel;
    }

    /**
     * Returns the recovery descriptor associated with this sender.
     */
    @TestOnly
    public RecoveryDescriptor recoveryDescriptor() {
        return recoveryDescriptor;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
