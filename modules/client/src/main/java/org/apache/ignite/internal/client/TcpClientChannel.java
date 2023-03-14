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

package org.apache.ignite.internal.client;

import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.internal.client.io.ClientConnection;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.io.ClientMessageHandler;
import org.apache.ignite.internal.client.proto.ClientMessageCommon;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.client.proto.ResponseFlags;
import org.apache.ignite.internal.client.proto.ServerMessageType;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Implements {@link ClientChannel} over TCP.
 */
class TcpClientChannel implements ClientChannel, ClientMessageHandler, ClientConnectionStateHandler {
    /** Protocol version used by default on first connection attempt. */
    private static final ProtocolVersion DEFAULT_VERSION = ProtocolVersion.LATEST_VER;

    /** Supported protocol versions. */
    private static final Collection<ProtocolVersion> supportedVers = Collections.singletonList(
            ProtocolVersion.V3_0_0
    );

    /** Minimum supported heartbeat interval. */
    private static final long MIN_RECOMMENDED_HEARTBEAT_INTERVAL = 500;

    /** Config. */
    private final ClientChannelConfiguration cfg;

    /** Protocol context. */
    private volatile ProtocolContext protocolCtx;

    /** Channel. */
    private volatile ClientConnection sock;

    /** Request id. */
    private final AtomicLong reqId = new AtomicLong(1);

    /** Pending requests. */
    private final Map<Long, ClientRequestFuture> pendingReqs = new ConcurrentHashMap<>();

    /** Topology change listeners. */
    private final Collection<Consumer<ClientChannel>> assignmentChangeListeners = new CopyOnWriteArrayList<>();

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Executor for async operation listeners. */
    private final Executor asyncContinuationExecutor;

    /** Connect timeout in milliseconds. */
    private final long connectTimeout;

    /** Heartbeat timeout in milliseconds. */
    private final long heartbeatTimeout;

    /** Heartbeat timer. */
    private volatile Timer heartbeatTimer;

    /** Logger. */
    private final IgniteLogger log;

    /** Last send operation timestamp. */
    private volatile long lastSendMillis;

    /**
     * Constructor.
     *
     * @param cfg     Config.
     */
    private TcpClientChannel(ClientChannelConfiguration cfg) {
        validateConfiguration(cfg);
        this.cfg = cfg;

        log = ClientUtils.logger(cfg.clientConfiguration(), TcpClientChannel.class);

        asyncContinuationExecutor = cfg.clientConfiguration().asyncContinuationExecutor() == null
                ? ForkJoinPool.commonPool()
                : cfg.clientConfiguration().asyncContinuationExecutor();

        connectTimeout = cfg.clientConfiguration().connectTimeout();
        heartbeatTimeout = cfg.clientConfiguration().heartbeatTimeout();
    }

    private CompletableFuture<ClientChannel> initAsync(ClientConnectionMultiplexer connMgr) {
        return connMgr
                .openAsync(cfg.getAddress(), this, this)
                .thenCompose(s -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Connection established [remoteAddress=" + s.remoteAddress() + ']');
                    }

                    sock = s;

                    return handshakeAsync(DEFAULT_VERSION);
                })
                .whenComplete((res, err) -> {
                    if (err != null) {
                        close();
                    }
                })
                .thenApplyAsync(unused -> {
                    // Netty has a built-in IdleStateHandler to detect idle connections (used on the server side).
                    // However, to adjust the heartbeat interval dynamically, we have to use a timer here.
                    heartbeatTimer = initHeartbeat(cfg.clientConfiguration().heartbeatInterval());

                    return this;
                }, asyncContinuationExecutor);
    }

    /**
     * Creates a new channel asynchronously.
     *
     * @param cfg Configuration.
     * @param connMgr Connection manager.
     * @return Channel.
     */
    static CompletableFuture<ClientChannel> createAsync(ClientChannelConfiguration cfg, ClientConnectionMultiplexer connMgr) {
        //noinspection resource - returned from method.
        return new TcpClientChannel(cfg).initAsync(connMgr);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        close(null);
    }

    /**
     * Close the channel with cause.
     */
    private void close(@Nullable Exception cause) {
        if (closed.compareAndSet(false, true)) {
            // Disconnect can happen before we initialize the timer.
            var timer = heartbeatTimer;

            if (timer != null) {
                timer.cancel();
            }

            sock.close();

            for (ClientRequestFuture pendingReq : pendingReqs.values()) {
                pendingReq.completeExceptionally(new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed", cause));
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onMessage(ByteBuf buf) {
        try {
            processNextMessage(buf);
        } catch (Throwable t) {
            buf.release();
            throw t;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onDisconnected(@Nullable Exception e) {
        if (log.isDebugEnabled()) {
            log.debug("Connection closed [remoteAddress=" + cfg.getAddress() + ']');
        }

        close(e);
    }

    /** {@inheritDoc} */
    @Override
    public <T> CompletableFuture<T> serviceAsync(
            int opCode,
            PayloadWriter payloadWriter,
            PayloadReader<T> payloadReader
    ) {
        try {
            if (log.isTraceEnabled()) {
                log.trace("Sending request [opCode=" + opCode + ", remoteAddress=" + cfg.getAddress() + ']');
            }

            ClientRequestFuture fut = send(opCode, payloadWriter);

            return receiveAsync(fut, payloadReader);
        } catch (Throwable t) {
            CompletableFuture<T> fut = new CompletableFuture<>();
            fut.completeExceptionally(t);

            return fut;
        }
    }

    /**
     * Constructor.
     *
     * @param opCode        Operation code.
     * @param payloadWriter Payload writer to stream or {@code null} if request has no payload.
     * @return Request future.
     */
    private ClientRequestFuture send(int opCode, PayloadWriter payloadWriter) {
        long id = reqId.getAndIncrement();

        if (closed()) {
            throw new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed");
        }

        ClientRequestFuture fut = new ClientRequestFuture();

        pendingReqs.put(id, fut);

        PayloadOutputChannel payloadCh = new PayloadOutputChannel(this, new ClientMessagePacker(sock.getBuffer()));

        try {
            var req = payloadCh.out();

            req.packInt(opCode);
            req.packLong(id);

            if (payloadWriter != null) {
                payloadWriter.accept(payloadCh);
            }

            write(req).addListener(f -> {
                if (!f.isSuccess()) {
                    fut.completeExceptionally(new IgniteClientConnectionException(CONNECTION_ERR, "Failed to send request", f.cause()));
                }
            });

            return fut;
        } catch (Throwable t) {
            log.warn("Failed to send request [id=" + id + ", op=" + opCode + ", remoteAddress=" + cfg.getAddress() + "]: " +
                    t.getMessage(), t);

            // Close buffer manually on fail. Successful write closes the buffer automatically.
            payloadCh.close();
            pendingReqs.remove(id);

            throw IgniteException.wrap(t);
        }
    }

    /**
     * Receives the response asynchronously.
     *
     * @param pendingReq    Request future.
     * @param payloadReader Payload reader from stream.
     * @return Future for the operation.
     */
    private <T> CompletableFuture<T> receiveAsync(ClientRequestFuture pendingReq, PayloadReader<T> payloadReader) {
        return pendingReq.thenApplyAsync(payload -> {
            if (payload == null) {
                return null;
            }

            if (payloadReader == null) {
                payload.close();
                return null;
            }

            try (var in = new PayloadInputChannel(this, payload)) {
                return payloadReader.apply(in);
            } catch (Exception e) {
                log.error("Failed to deserialize server response [remoteAddress=" + cfg.getAddress() + "]: " + e.getMessage(), e);

                throw new IgniteClientConnectionException(PROTOCOL_ERR, "Failed to deserialize server response: " + e.getMessage(), e);
            }
        }, asyncContinuationExecutor);
    }

    /**
     * Process next message from the input stream and complete corresponding future.
     */
    private void processNextMessage(ByteBuf buf) throws IgniteException {
        var unpacker = new ClientMessageUnpacker(buf);

        if (protocolCtx == null) {
            // Process handshake.
            pendingReqs.remove(-1L).complete(unpacker);
            return;
        }

        var type = unpacker.unpackInt();

        if (type != ServerMessageType.RESPONSE) {
            log.error("Unexpected message type [remoteAddress=" + cfg.getAddress() + "]: " + type);

            throw new IgniteClientConnectionException(PROTOCOL_ERR, "Unexpected message type: " + type);
        }

        Long resId = unpacker.unpackLong();

        ClientRequestFuture pendingReq = pendingReqs.remove(resId);

        if (pendingReq == null) {
            log.error("Unexpected response ID [remoteAddress=" + cfg.getAddress() + "]: " + resId);

            throw new IgniteClientConnectionException(PROTOCOL_ERR, String.format("Unexpected response ID [%s]", resId));
        }

        int flags = unpacker.unpackInt();

        if (ResponseFlags.getPartitionAssignmentChangedFlag(flags)) {
            if (log.isInfoEnabled()) {
                log.info("Partition assignment change notification received [remoteAddress=" + cfg.getAddress() + "]");
            }

            for (Consumer<ClientChannel> listener : assignmentChangeListeners) {
                listener.accept(this);
            }
        }

        if (unpacker.tryUnpackNil()) {
            pendingReq.complete(unpacker);
        } else {
            IgniteException err = readError(unpacker);

            unpacker.close();

            pendingReq.completeExceptionally(err);
        }
    }

    /**
     * Unpacks request error.
     *
     * @param unpacker Unpacker.
     * @return Exception.
     */
    private static IgniteException readError(ClientMessageUnpacker unpacker) {
        var traceId = unpacker.unpackUuid();
        var code = unpacker.unpackInt();
        var errClassName = unpacker.unpackString();
        var errMsg = unpacker.tryUnpackNil() ? null : unpacker.unpackString();

        IgniteException causeWithStackTrace = unpacker.tryUnpackNil() ? null : new IgniteException(traceId, code, unpacker.unpackString());

        try {
            Class<?> errCls = Class.forName(errClassName);

            if (IgniteException.class.isAssignableFrom(errCls)) {
                Constructor<?> constructor = errCls.getDeclaredConstructor(UUID.class, int.class, String.class, Throwable.class);

                return (IgniteException) constructor.newInstance(traceId, code, errMsg, causeWithStackTrace);
            }
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException ignored) {
            // Ignore: incompatible exception class. Fall back to generic exception.
        }

        return new IgniteException(traceId, code, errClassName + ": " + errMsg, causeWithStackTrace);
    }

    /** {@inheritDoc} */
    @Override
    public boolean closed() {
        return closed.get();
    }

    /** {@inheritDoc} */
    @Override
    public ProtocolContext protocolContext() {
        return protocolCtx;
    }

    /** {@inheritDoc} */
    @Override
    public void addTopologyAssignmentChangeListener(Consumer<ClientChannel> listener) {
        assignmentChangeListeners.add(listener);
    }

    private static void validateConfiguration(ClientChannelConfiguration cfg) {
        String error = null;

        InetSocketAddress addr = cfg.getAddress();

        if (addr == null) {
            error = "At least one Ignite server node must be specified in the Ignite client configuration";
        } else if (addr.getPort() < 1024 || addr.getPort() > 49151) {
            error = String.format("Ignite client port %s is out of valid ports range 1024...49151", addr.getPort());
        }

        if (error != null) {
            throw new IllegalArgumentException(error);
        }
    }

    /** Client handshake. */
    private CompletableFuture<Void> handshakeAsync(ProtocolVersion ver)
            throws IgniteClientConnectionException {
        ClientRequestFuture fut = new ClientRequestFuture();
        pendingReqs.put(-1L, fut);

        handshakeReqAsync(ver).addListener(f -> {
            if (!f.isSuccess()) {
                fut.completeExceptionally(
                        new IgniteClientConnectionException(CONNECTION_ERR, "Failed to send handshake request", f.cause()));
            }
        });

        if (connectTimeout > 0) {
            fut.orTimeout(connectTimeout, TimeUnit.MILLISECONDS);
        }

        return fut.thenCompose(res -> handshakeRes(res, ver));
    }

    /**
     * Send handshake request.
     *
     * @return Channel future.
     */
    private ChannelFuture handshakeReqAsync(ProtocolVersion proposedVer) {
        sock.send(Unpooled.wrappedBuffer(ClientMessageCommon.MAGIC_BYTES));

        var req = new ClientMessagePacker(sock.getBuffer());
        req.packInt(proposedVer.major());
        req.packInt(proposedVer.minor());
        req.packInt(proposedVer.patch());

        req.packInt(2); // Client type: general purpose.

        req.packBinaryHeader(0); // Features.
        req.packMapHeader(0); // Extensions.

        return write(req);
    }

    /** Receive and handle handshake response. */
    private CompletableFuture<Void> handshakeRes(ClientMessageUnpacker unpacker, ProtocolVersion proposedVer) {
        try (unpacker) {
            ProtocolVersion srvVer = new ProtocolVersion(unpacker.unpackShort(), unpacker.unpackShort(),
                    unpacker.unpackShort());

            if (!unpacker.tryUnpackNil()) {
                if (!proposedVer.equals(srvVer) && supportedVers.contains(srvVer)) {
                    // Retry with server version.
                    return handshakeAsync(srvVer);
                }

                throw readError(unpacker);
            }

            var serverIdleTimeout = unpacker.unpackLong();
            var clusterNodeId = unpacker.unpackString();
            var clusterNodeName = unpacker.unpackString();
            var addr = sock.remoteAddress();
            var clusterNode = new ClusterNode(clusterNodeId, clusterNodeName, new NetworkAddress(addr.getHostName(), addr.getPort()));
            var clusterId = unpacker.unpackUuid();

            var featuresLen = unpacker.unpackBinaryHeader();
            unpacker.skipValues(featuresLen);

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValues(extensionsLen);

            protocolCtx = new ProtocolContext(
                    srvVer, ProtocolBitmaskFeature.allFeaturesAsEnumSet(), serverIdleTimeout, clusterNode, clusterId);

            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            log.warn("Failed to handle handshake response [remoteAddress=" + cfg.getAddress() + "]: " + e.getMessage(), e);

            return CompletableFuture.failedFuture(e);
        }
    }

    /** Write bytes to the output stream. */
    private ChannelFuture write(ClientMessagePacker packer) throws IgniteClientConnectionException {
        lastSendMillis = System.currentTimeMillis();

        var buf = packer.getBuffer();

        return sock.send(buf);
    }

    /**
     * Initializes heartbeats.
     *
     * @param configuredInterval Configured heartbeat interval, in milliseconds.
     * @return Heartbeat timer.
     */
    private Timer initHeartbeat(long configuredInterval) {
        long heartbeatInterval = getHeartbeatInterval(configuredInterval);

        Timer timer = new Timer("tcp-client-channel-heartbeats-" + hashCode());

        timer.schedule(new HeartbeatTask(heartbeatInterval), heartbeatInterval, heartbeatInterval);

        return timer;
    }

    /**
     * Gets the heartbeat interval based on the configured value and served-side idle timeout.
     *
     * @param configuredInterval Configured interval.
     * @return Resolved interval.
     */
    private long getHeartbeatInterval(long configuredInterval) {
        long serverIdleTimeoutMs = protocolCtx.serverIdleTimeout();

        if (serverIdleTimeoutMs <= 0) {
            return configuredInterval;
        }

        long recommendedHeartbeatInterval = serverIdleTimeoutMs / 3;

        if (recommendedHeartbeatInterval < MIN_RECOMMENDED_HEARTBEAT_INTERVAL) {
            recommendedHeartbeatInterval = MIN_RECOMMENDED_HEARTBEAT_INTERVAL;
        }

        return Math.min(configuredInterval, recommendedHeartbeatInterval);
    }

    /**
     * Client request future.
     */
    private static class ClientRequestFuture extends CompletableFuture<ClientMessageUnpacker> {
    }

    /**
     * Sends heartbeat messages.
     */
    private class HeartbeatTask extends TimerTask {
        /** Heartbeat interval. */
        private final long interval;

        /** Constructor. */
        HeartbeatTask(long interval) {
            this.interval = interval;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                if (System.currentTimeMillis() - lastSendMillis > interval) {
                    var fut = serviceAsync(ClientOp.HEARTBEAT, null, null);

                    if (connectTimeout > 0) {
                        fut
                                .orTimeout(heartbeatTimeout, TimeUnit.MILLISECONDS)
                                .exceptionally(e -> {
                                    if (e instanceof TimeoutException) {
                                        log.warn("Heartbeat timeout, closing the channel [remoteAddress=" + cfg.getAddress() + ']');

                                        close((TimeoutException) e);
                                    }

                                    return null;
                                });
                    }
                }
            } catch (Throwable e) {
                log.warn("Failed to send heartbeat [remoteAddress=" + cfg.getAddress() + "]: " + e.getMessage(), e);
            }
        }
    }
}
