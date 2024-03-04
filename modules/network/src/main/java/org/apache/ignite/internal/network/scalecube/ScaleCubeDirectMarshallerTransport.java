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

package org.apache.ignite.internal.network.scalecube;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageTypes;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.network.message.ScaleCubeMessageBuilder;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * ScaleCube transport over {@link ConnectionManager}.
 */
class ScaleCubeDirectMarshallerTransport implements Transport {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(Transport.class);

    private static final ChannelType SCALECUBE_CHANNEL_TYPE = ChannelType.register((short) 1, "ScaleCube");

    /** Message subject. */
    private final DirectProcessor<Message> subject = DirectProcessor.create();

    /** Message sink. */
    private final FluxSink<Message> sink = subject.sink();

    /** Close handler. */
    private final MonoProcessor<Void> stop = MonoProcessor.create();

    /** On stop. */
    private final MonoProcessor<Void> onStop = MonoProcessor.create();

    /** Connection manager. */
    private final MessagingService messagingService;

    /** Message factory. */
    private final NetworkMessagesFactory messageFactory;

    /** Topology service. */
    private final ScaleCubeTopologyService topologyService;

    /** Node address. */
    private final Address address;

    /**
     * Constructor.
     *
     * @param localAddress Local address.
     * @param messagingService Messaging service.
     * @param topologyService Topology service.
     * @param messageFactory  Message factory.
     */
    ScaleCubeDirectMarshallerTransport(
            Address localAddress,
            MessagingService messagingService,
            ScaleCubeTopologyService topologyService,
            NetworkMessagesFactory messageFactory
    ) {
        this.address = localAddress;
        this.messagingService = messagingService;
        this.topologyService = topologyService;
        this.messageFactory = messageFactory;

        this.messagingService.addMessageHandler(NetworkMessageTypes.class, (message, sender, correlationId) -> onMessage(message));
        // Setup cleanup
        stop.then(doStop())
                .doFinally(s -> onStop.onComplete())
                .subscribe(
                        null,
                        ex -> LOG.warn("Failed to stop [address={}, reason={}]", address, ex.toString())
                );
    }

    /**
     * Cleanup resources on stop.
     *
     * @return A mono, that resolves when the stop operation is finished.
     */
    private Mono<Void> doStop() {
        return Mono.defer(() -> {
            LOG.info("Stopping [address={}]", address);

            sink.complete();

            LOG.info("Stopped [address={}]", address);
            return Mono.empty();
        });
    }

    /** {@inheritDoc} */
    @Override
    public Address address() {
        return address;
    }

    /** {@inheritDoc} */
    @Override
    public Mono<Transport> start() {
        return Mono.just(this);
    }

    /** {@inheritDoc} */
    @Override
    public Mono<Void> stop() {
        return Mono.defer(() -> {
            stop.onComplete();
            return onStop;
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean isStopped() {
        return onStop.isDisposed();
    }

    /** {@inheritDoc} */
    @Override
    public Mono<Void> send(Address address, Message message) {
        var addr = new NetworkAddress(address.host(), address.port());

        return Mono.fromFuture(() -> {
            ClusterNode node = topologyService.getByAddress(addr);

            // Create a fake node for nodes that are not in the topology yet
            if (node == null) {
                node = new ClusterNodeImpl(null, null, addr);
            }

            return messagingService.send(node, SCALECUBE_CHANNEL_TYPE, fromMessage(message));
        });
    }

    /**
     * Handles new network messages.
     *
     * @param msg    Network message.
     */
    private void onMessage(NetworkMessage msg) {
        Message message = fromNetworkMessage(msg);

        if (message != null) {
            sink.next(message);
        }
    }

    /**
     * Wrap ScaleCube {@link Message} with {@link NetworkMessage}.
     *
     * @param message ScaleCube message.
     * @return Netowork message that wraps ScaleCube message.
     * @throws IgniteInternalException If failed to write message to ObjectOutputStream.
     */
    private NetworkMessage fromMessage(Message message) throws IgniteInternalException {
        Object dataObj = message.data();

        ScaleCubeMessageBuilder scaleCubeMessageBuilder = messageFactory.scaleCubeMessage();

        if (dataObj instanceof NetworkMessage) {
            // If data object is a network message, we can use direct marshaller to serialize it
            scaleCubeMessageBuilder.message((NetworkMessage) dataObj);
        } else {
            // If data object is not a network message user object marshaller will be used
            scaleCubeMessageBuilder.data(dataObj);
        }

        return scaleCubeMessageBuilder
                .headers(message.headers())
                .build();
    }

    /**
     * Unwrap ScaleCube {@link Message} from {@link NetworkMessage}.
     *
     * @param networkMessage Network message.
     * @return ScaleCube message.
     * @throws IgniteInternalException If failed to read ScaleCube message byte array.
     */
    @Nullable
    private static Message fromNetworkMessage(NetworkMessage networkMessage) throws IgniteInternalException {
        if (networkMessage instanceof ScaleCubeMessage) {
            ScaleCubeMessage msg = (ScaleCubeMessage) networkMessage;

            Map<String, String> headers = msg.headers();

            Object obj = msg.data();

            NetworkMessage message = msg.message();

            Object data = obj != null ? obj : message;

            return Message.withHeaders(headers).data(data).build();
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Mono<Message> requestResponse(Address address, Message request) {
        return Mono.create(sink -> {
            Objects.requireNonNull(request, "request must be not null");
            Objects.requireNonNull(request.correlationId(), "correlationId must be not null");

            Disposable receive =
                    listen()
                            .filter(resp -> resp.correlationId() != null)
                            .filter(resp -> resp.correlationId().equals(request.correlationId()))
                            .take(1)
                            .subscribe(sink::success, sink::error, sink::success);

            Disposable send =
                    send(address, request)
                            .subscribe(
                                    null,
                                    ex -> {
                                        receive.dispose();
                                        sink.error(ex);
                                    });

            sink.onDispose(Disposables.composite(send, receive));
        });
    }

    /** {@inheritDoc} */
    @Override
    public final Flux<Message> listen() {
        return subject.onBackpressureBuffer();
    }
}
