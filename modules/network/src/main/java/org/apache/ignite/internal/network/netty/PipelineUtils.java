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

import io.netty.channel.ChannelPipeline;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.util.function.Consumer;
import org.apache.ignite.internal.network.NaiveMessageFormat;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptor;
import org.apache.ignite.internal.network.serialization.MessageFormat;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;

/** Pipeline utils. */
public class PipelineUtils {
    /** {@link ChunkedWriteHandler}'s name. */
    private static final String CHUNKED_WRITE_HANDLER_NAME = "chunked-write-handler";

    /**
     * Sets up initial pipeline with ssl.
     *
     * @param pipeline Channel pipeline.
     * @param serializationService Serialization service.
     * @param handshakeManager Handshake manager.
     * @param messageListener Message listener.
     * @param sslContext Netty SSL context.
     */
    public static void setup(ChannelPipeline pipeline, PerSessionSerializationService serializationService,
            HandshakeManager handshakeManager, Consumer<InNetworkObject> messageListener, SslContext sslContext) {
        pipeline.addFirst("ssl", sslContext.newHandler(pipeline.channel().alloc()));

        setup(pipeline, serializationService, handshakeManager, messageListener);
    }

    /**
     * Sets up initial pipeline.
     *
     * @param pipeline Channel pipeline.
     * @param serializationService Serialization service.
     * @param handshakeManager Handshake manager.
     * @param messageListener Message listener.
     */
    public static void setup(ChannelPipeline pipeline, PerSessionSerializationService serializationService,
            HandshakeManager handshakeManager, Consumer<InNetworkObject> messageListener) {
        MessageFormat messageFormat = new NaiveMessageFormat();

        // Consolidate flushes to bigger ones (improves throughput with smaller messages at the price of the latency).
        pipeline.addLast(new FlushConsolidationHandler(FlushConsolidationHandler.DEFAULT_EXPLICIT_FLUSH_AFTER_FLUSHES, true));

        pipeline.addLast(InboundDecoder.NAME, new InboundDecoder(messageFormat, serializationService));
        pipeline.addLast(HandshakeHandler.NAME, new HandshakeHandler(handshakeManager, messageListener, serializationService));
        pipeline.addLast(CHUNKED_WRITE_HANDLER_NAME, new ChunkedWriteHandler());
        pipeline.addLast(OutboundEncoder.NAME, new OutboundEncoder(messageFormat, serializationService));
        pipeline.addLast(IoExceptionSuppressingHandler.NAME, new IoExceptionSuppressingHandler());
        pipeline.addLast(DefaultExceptionHandler.NAME, new DefaultExceptionHandler());
    }

    /**
     * Changes pipeline after the handshake.
     *
     * @param pipeline Pipeline.
     * @param descriptor Recovery descriptor.
     * @param messageHandler Message handler.
     * @param factory Message factory.
     */
    public static void afterHandshake(
            ChannelPipeline pipeline,
            RecoveryDescriptor descriptor,
            MessageHandler messageHandler,
            NetworkMessagesFactory factory
    ) {
        pipeline.addAfter(OutboundEncoder.NAME, OutboundRecoveryHandler.NAME, new OutboundRecoveryHandler(descriptor));
        pipeline.addBefore(
                HandshakeHandler.NAME, InboundRecoveryHandler.NAME, new InboundRecoveryHandler(descriptor, factory)
        );
        pipeline.addAfter(HandshakeHandler.NAME, MessageHandler.NAME, messageHandler);
    }
}
