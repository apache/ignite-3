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

import static org.apache.ignite.internal.util.ExceptionUtils.copyExceptionWithCause;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.client.IgniteClientAuthenticator;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.internal.client.io.ClientConnection;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.io.ClientMessageHandler;
import org.apache.ignite.internal.client.proto.ClientMessageCommon;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.ErrorExtensions;
import org.apache.ignite.internal.client.proto.HandshakeExtension;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.client.proto.ResponseFlags;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ViewUtils;
import org.apache.ignite.lang.ErrorGroups.Table;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Implements {@link ClientChannel} over TCP.
 */
class TcpClientChannel implements ClientChannel, ClientMessageHandler, ClientConnectionStateHandler {
    /** Protocol version used by default on first connection attempt. */
    private static final ProtocolVersion DEFAULT_VERSION = ProtocolVersion.LATEST_VER;

    /** Minimum supported heartbeat interval. */
    private static final long MIN_RECOMMENDED_HEARTBEAT_INTERVAL = 500;

    /** Config. */
    private final ClientChannelConfiguration cfg;

    /** Metrics. */
    private final ClientMetricSource metrics;

    /** Protocol context. */
    private volatile ProtocolContext protocolCtx;

    /** Channel. */
    private volatile ClientConnection sock;

    /** Request id. */
    private final AtomicLong reqId = new AtomicLong(1);

    /** Pending requests. */
    private final Map<Long, ClientRequestFuture<?>> pendingReqs = new ConcurrentHashMap<>();

    /** Notification handlers. */
    private final Map<Long, CompletableFuture<PayloadInputChannel>> notificationHandlers = new ConcurrentHashMap<>();

    /** Topology change listeners. */
    private final Consumer<Long> assignmentChangeListener;

    /** Observable timestamp listeners. */
    private final Consumer<Long> observableTimestampListener;

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Executor for async operation listeners. */
    private final Executor asyncContinuationExecutor;

    /** Connect timeout in milliseconds. */
    private final long connectTimeout;

    /** Heartbeat timeout in milliseconds. */
    private final long heartbeatTimeout;

    /** Operation timeout in milliseconds. */
    private final long operationTimeout;

    /** Heartbeat timer. */
    private volatile Timer heartbeatTimer;

    /** Logger. */
    private final IgniteLogger log;

    /** Last send operation timestamp. */
    private volatile long lastSendMillis;

    /**
     * Constructor.
     *
     * @param cfg Config.
     * @param metrics Metrics.
     */
    private TcpClientChannel(
            ClientChannelConfiguration cfg,
            ClientMetricSource metrics,
            Consumer<Long> assignmentChangeListener,
            Consumer<Long> observableTimestampListener) {
        validateConfiguration(cfg);
        this.cfg = cfg;
        this.metrics = metrics;
        this.assignmentChangeListener = assignmentChangeListener;
        this.observableTimestampListener = observableTimestampListener;

        log = ClientUtils.logger(cfg.clientConfiguration(), TcpClientChannel.class);

        asyncContinuationExecutor = cfg.clientConfiguration().asyncContinuationExecutor() == null
                ? ForkJoinPool.commonPool()
                : cfg.clientConfiguration().asyncContinuationExecutor();

        connectTimeout = cfg.clientConfiguration().connectTimeout();
        heartbeatTimeout = cfg.clientConfiguration().heartbeatTimeout();
        operationTimeout = cfg.clientConfiguration().operationTimeout();
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
                    if (protocolCtx != null) {
                        heartbeatTimer = initHeartbeat(cfg.clientConfiguration().heartbeatInterval());
                    }

                    return this;
                }, asyncContinuationExecutor);
    }

    /**
     * Creates a new channel asynchronously.
     *
     * @param cfg Configuration.
     * @param connMgr Connection manager.
     * @param metrics Metrics.
     * @return Channel.
     */
    static CompletableFuture<ClientChannel> createAsync(
            ClientChannelConfiguration cfg,
            ClientConnectionMultiplexer connMgr,
            ClientMetricSource metrics,
            Consumer<Long> assignmentChangeListener,
            Consumer<Long> observableTimestampListener) {
        //noinspection resource - returned from method.
        return new TcpClientChannel(cfg, metrics, assignmentChangeListener, observableTimestampListener)
                .initAsync(connMgr);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        close(null, true);
    }

    /**
     * Close the channel with cause.
     */
    private void close(@Nullable Throwable cause, boolean graceful) {
        if (closed.compareAndSet(false, true)) {
            if (cause != null && (cause instanceof TimeoutException || cause.getCause() instanceof TimeoutException)) {
                metrics.connectionsLostTimeoutIncrement();
            } else if (!graceful) {
                metrics.connectionsLostIncrement();
            }

            // Disconnect can happen before we initialize the timer.
            var timer = heartbeatTimer;

            if (timer != null) {
                timer.cancel();
            }

            if (sock != null) {
                sock.close();
            }

            for (ClientRequestFuture<?> pendingReq : pendingReqs.values()) {
                pendingReq.completeExceptionally(new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed", cause));
            }

            for (CompletableFuture<PayloadInputChannel> handler : notificationHandlers.values()) {
                try {
                    handler.completeExceptionally(new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed", cause));
                } catch (Exception ignored) {
                    // Ignore.
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onMessage(ByteBuf buf) {
        asyncContinuationExecutor.execute(() -> {
            try (var unpacker = new ClientMessageUnpacker(buf)) {
                processNextMessage(unpacker);
            } catch (Throwable t) {
                close(t, false);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void onDisconnected(@Nullable Exception e) {
        if (log.isDebugEnabled()) {
            log.debug("Connection closed [remoteAddress=" + cfg.getAddress() + ']');
        }

        close(e, false);
    }

    /** {@inheritDoc} */
    @Override
    public <T> CompletableFuture<T> serviceAsync(
            int opCode,
            @Nullable PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader,
            boolean expectNotifications
    ) {
        try {
            if (log.isTraceEnabled()) {
                log.trace("Sending request [opCode=" + opCode + ", remoteAddress=" + cfg.getAddress() + ']');
            }

            long id = reqId.getAndIncrement();

            CompletableFuture<PayloadInputChannel> notificationFut = null;

            if (expectNotifications) {
                // Notification can arrive before the response to the current request.
                // This is fine, because we use the same id and register the handler before sending the request.
                notificationFut = new CompletableFuture<>();
                notificationHandlers.put(id, notificationFut);
            }

            ClientRequestFuture<T> fut = send(opCode, id, payloadWriter, payloadReader, notificationFut);

            // Client-facing future will fail with a timeout, but internal ClientRequestFuture will stay in the map - otherwise
            // we'll fail with "protocol breakdown" error when a late response arrives from the server.
            return operationTimeout <= 0
                    ? fut
                    : fut.orTimeout(operationTimeout, TimeUnit.MILLISECONDS);

        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    /**
     * Sends request.
     *
     * @param opCode Operation code.
     * @param id Request id.
     * @param payloadWriter Payload writer to stream or {@code null} if request has no payload.
     * @param notificationFut Optional notification future.
     * @return Request future.
     */
    private <T> ClientRequestFuture<T> send(
            int opCode,
            long id,
            @Nullable PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader,
            @Nullable CompletableFuture<PayloadInputChannel> notificationFut) {
        if (closed()) {
            throw new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed");
        }

        ClientRequestFuture<T> fut = new ClientRequestFuture<>(payloadReader, notificationFut);
        pendingReqs.put(id, fut);

        metrics.requestsActiveIncrement();

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
                    String msg = "Failed to send request [id=" + id + ", op=" + opCode + ", remoteAddress=" + cfg.getAddress() + "]";
                    IgniteClientConnectionException ex = new IgniteClientConnectionException(CONNECTION_ERR, msg, f.cause());
                    fut.completeExceptionally(ex);
                    log.warn(msg + "]: " + f.cause().getMessage(), f.cause());

                    pendingReqs.remove(id);
                    metrics.requestsActiveDecrement();

                    // Close immediately, do not wait for onDisconnected call from Netty.
                    onDisconnected(ex);
                } else {
                    metrics.requestsSentIncrement();
                }
            });

            return fut;
        } catch (Throwable t) {
            log.warn("Failed to send request [id=" + id + ", op=" + opCode + ", remoteAddress=" + cfg.getAddress() + "]: "
                    + t.getMessage(), t);

            // Close buffer manually on fail. Successful write closes the buffer automatically.
            payloadCh.close();
            pendingReqs.remove(id);

            metrics.requestsActiveDecrement();

            throw sneakyThrow(ViewUtils.ensurePublicException(t));
        }
    }

    /**
     * Completes the request future.
     *
     * @param pendingReq    Request future.
     */
    private <T> void complete(ClientRequestFuture<T> pendingReq, ClientMessageUnpacker unpacker) {
        if (pendingReq.payloadReader == null) {
            pendingReq.complete(null);
        } else {
            try {
                T res = pendingReq.payloadReader.apply(new PayloadInputChannel(this, unpacker, pendingReq.notificationFut));
                pendingReq.complete(res);
            } catch (Throwable e) {
                log.error("Failed to deserialize server response [remoteAddress=" + cfg.getAddress() + "]: " + e.getMessage(), e);

                pendingReq.completeExceptionally(
                        new IgniteException(PROTOCOL_ERR, "Failed to deserialize server response: " + e.getMessage(), e));
            }
        }
    }

    /**
     * Process next message from the input stream and complete corresponding future.
     */
    private void processNextMessage(ClientMessageUnpacker unpacker) throws IgniteException {
        if (protocolCtx == null) {
            // Process handshake.
            complete(pendingReqs.remove(-1L), unpacker);
            return;
        }

        Long resId = unpacker.unpackLong();
        int flags = unpacker.unpackInt();

        handlePartitionAssignmentChange(flags, unpacker);
        handleObservableTimestamp(unpacker);

        Throwable err = ResponseFlags.getErrorFlag(flags) ? readError(unpacker) : null;

        if (ResponseFlags.getNotificationFlag(flags)) {
            handleNotification(resId, unpacker, err);
            return;
        }

        ClientRequestFuture<?> pendingReq = pendingReqs.remove(resId);
        if (pendingReq == null) {
            log.error("Unexpected response ID [remoteAddress=" + cfg.getAddress() + "]: " + resId);

            throw new IgniteClientConnectionException(PROTOCOL_ERR, String.format("Unexpected response ID [%s]", resId));
        }

        metrics.requestsActiveDecrement();

        if (err == null) {
            metrics.requestsCompletedIncrement();
            complete(pendingReq, unpacker);
        } else {
            metrics.requestsFailedIncrement();
            notificationHandlers.remove(resId);
            pendingReq.completeExceptionally(err);
        }
    }

    private void handleObservableTimestamp(ClientMessageUnpacker unpacker) {
        long observableTimestamp = unpacker.unpackLong();
        observableTimestampListener.accept(observableTimestamp);
    }

    private void handlePartitionAssignmentChange(int flags, ClientMessageUnpacker unpacker) {
        if (ResponseFlags.getPartitionAssignmentChangedFlag(flags)) {
            if (log.isInfoEnabled()) {
                log.info("Partition assignment change notification received [remoteAddress=" + cfg.getAddress() + "]");
            }

            long maxStartTime = unpacker.unpackLong();
            assignmentChangeListener.accept(maxStartTime);
        }
    }

    private void handleNotification(long id, ClientMessageUnpacker unpacker, @Nullable Throwable err) {
        // One-shot notification handler - remove immediately.
        CompletableFuture<PayloadInputChannel> handler = notificationHandlers.remove(id);
        if (handler == null) {
            log.error("Unexpected notification ID [remoteAddress=" + cfg.getAddress() + "]: " + id);

            throw new IgniteClientConnectionException(PROTOCOL_ERR, String.format("Unexpected notification ID [%s]", id));
        }

        try {
            if (err != null) {
                handler.completeExceptionally(err);
            } else {
                unpacker.retain();
                handler.complete(new PayloadInputChannel(this, unpacker, null));
            }
        } catch (Exception e) {
            log.error("Failed to handle server notification [remoteAddress=" + cfg.getAddress() + "]: " + e.getMessage(), e);

            throw new IgniteException(PROTOCOL_ERR, "Failed to to server notification: " + e.getMessage(), e);
        }
    }

    /**
     * Unpacks request error.
     *
     * @param unpacker Unpacker.
     * @return Exception.
     */
    private static Throwable readError(ClientMessageUnpacker unpacker) {
        var traceId = unpacker.unpackUuid();
        var code = unpacker.unpackInt();

        var errClassName = unpacker.unpackString();
        var errMsg = unpacker.tryUnpackNil() ? null : unpacker.unpackString();

        IgniteException causeWithStackTrace = unpacker.tryUnpackNil() ? null : new IgniteException(traceId, code, unpacker.unpackString());

        if (code == Table.SCHEMA_VERSION_MISMATCH_ERR) {
            int extSize;
            extSize = unpacker.tryUnpackNil() ? 0 : unpacker.unpackInt();
            int expectedSchemaVersion = -1;

            for (int i = 0; i < extSize; i++) {
                String key = unpacker.unpackString();

                if (key.equals(ErrorExtensions.EXPECTED_SCHEMA_VERSION)) {
                    expectedSchemaVersion = unpacker.unpackInt();
                } else {
                    // Unknown extension - ignore.
                    unpacker.skipValues(1);
                }
            }

            if (expectedSchemaVersion == -1) {
                return new IgniteException(
                        traceId, PROTOCOL_ERR, "Expected schema version is not specified in error extension map.", causeWithStackTrace);
            }

            return new ClientSchemaVersionMismatchException(traceId, code, errMsg, expectedSchemaVersion, causeWithStackTrace);
        }

        try {
            Class<? extends Throwable> errCls = (Class<? extends Throwable>) Class.forName(errClassName);
            return copyExceptionWithCause(errCls, traceId, code, errMsg, causeWithStackTrace);
        } catch (ClassNotFoundException ignored) {
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

    private static void validateConfiguration(ClientChannelConfiguration cfg) {
        String error = null;

        InetSocketAddress addr = cfg.getAddress();

        if (addr == null) {
            error = "At least one Ignite server node must be specified in the Ignite client configuration";
        }

        if (error != null) {
            throw new IllegalArgumentException(error);
        }
    }

    /** Client handshake. */
    private CompletableFuture<Object> handshakeAsync(ProtocolVersion ver)
            throws IgniteClientConnectionException {
        ClientRequestFuture<Object> fut = new ClientRequestFuture<>(r -> handshakeRes(r.in()), null);
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

        return fut
                .handle((res, err) -> {
                    if (err != null) {
                        if (err instanceof TimeoutException || err.getCause() instanceof TimeoutException) {
                            metrics.handshakesFailedTimeoutIncrement();
                            throw new IgniteClientConnectionException(CONNECTION_ERR, "Handshake timeout", err);
                        } else {
                            metrics.handshakesFailedIncrement();
                        }

                        throw new IgniteClientConnectionException(CONNECTION_ERR, "Handshake error", err);
                    }

                    return res;
                });
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

        IgniteClientAuthenticator authenticator = cfg.clientConfiguration().authenticator();

        if (authenticator != null) {
            // Extensions.
            req.packInt(3);

            req.packString(HandshakeExtension.AUTHENTICATION_TYPE.key());
            req.packString(authenticator.type());

            req.packString(HandshakeExtension.AUTHENTICATION_IDENTITY.key());
            packAuthnObj(req, authenticator.identity());

            req.packString(HandshakeExtension.AUTHENTICATION_SECRET.key());
            packAuthnObj(req, authenticator.secret());
        } else {
            // Extensions.
            req.packInt(0);
        }

        return write(req);
    }

    private @Nullable Object handshakeRes(ClientMessageUnpacker unpacker) {
        try {
            ProtocolVersion srvVer = new ProtocolVersion(unpacker.unpackShort(), unpacker.unpackShort(), unpacker.unpackShort());

            if (!unpacker.tryUnpackNil()) {
                throw sneakyThrow(readError(unpacker));
            }

            var serverIdleTimeout = unpacker.unpackLong();
            var clusterNodeId = unpacker.unpackString();
            var clusterNodeName = unpacker.unpackString();
            var addr = sock.remoteAddress();
            var clusterNode = new ClientClusterNode(clusterNodeId, clusterNodeName, new NetworkAddress(addr.getHostName(), addr.getPort()));
            var clusterId = unpacker.unpackUuid();
            var clusterName = unpacker.unpackString();

            long observableTimestamp = unpacker.unpackLong();
            observableTimestampListener.accept(observableTimestamp);

            unpacker.unpackByte(); // cluster version major
            unpacker.unpackByte(); // cluster version minor
            unpacker.unpackByte(); // cluster version maintenance
            unpacker.unpackByteNullable(); // cluster version patch
            unpacker.unpackStringNullable(); // cluster version pre release

            var featuresLen = unpacker.unpackBinaryHeader();
            unpacker.skipValues(featuresLen);

            var extensionsLen = unpacker.unpackInt();
            unpacker.skipValues(extensionsLen);

            protocolCtx = new ProtocolContext(
                    srvVer, ProtocolBitmaskFeature.allFeaturesAsEnumSet(), serverIdleTimeout, clusterNode, clusterId, clusterName);

            return null;
        } catch (Exception e) {
            log.warn("Failed to handle handshake response [remoteAddress=" + cfg.getAddress() + "]: " + e.getMessage(), e);

            throw e;
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

    private static void packAuthnObj(ClientMessagePacker packer, Object obj) {
        if (obj == null) {
            packer.packNil();
        } else if (obj instanceof String) {
            packer.packString((String) obj);
        } else {
            throw new IllegalArgumentException("Unsupported authentication object type: " + obj.getClass().getName());
        }
    }

    @Override
    public String toString() {
        return S.toString(TcpClientChannel.class.getSimpleName(), "remoteAddress", sock.remoteAddress(), false);
    }

    /**
     * Client request future.
     */
    private static class ClientRequestFuture<T> extends CompletableFuture<T> {
        @Nullable
        private final PayloadReader<T> payloadReader;

        @Nullable
        private final CompletableFuture<PayloadInputChannel> notificationFut;

        private ClientRequestFuture(
                @Nullable PayloadReader<T> payloadReader,
                @Nullable CompletableFuture<PayloadInputChannel> notificationFut) {
            this.payloadReader = payloadReader;
            this.notificationFut = notificationFut;
        }
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
                    var fut = serviceAsync(ClientOp.HEARTBEAT, null, null, false);

                    if (connectTimeout > 0) {
                        fut
                                .orTimeout(heartbeatTimeout, TimeUnit.MILLISECONDS)
                                .exceptionally(e -> {
                                    if (e instanceof TimeoutException) {
                                        log.warn("Heartbeat timeout, closing the channel [remoteAddress=" + cfg.getAddress() + ']');

                                        close(new IgniteClientConnectionException(CONNECTION_ERR, "Heartbeat timeout", e), false);
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
