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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.copyExceptionWithCause;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapRootCause;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import org.apache.ignite.internal.client.proto.HandshakeUtils;
import org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.client.proto.ResponseFlags;
import org.apache.ignite.internal.future.timeout.TimeoutObject;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ViewUtils;
import org.apache.ignite.lang.ErrorGroups.Table;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Implements {@link ClientChannel} over TCP.
 */
class TcpClientChannel implements ClientChannel, ClientMessageHandler, ClientConnectionStateHandler {
    /** Protocol version used by default on first connection attempt. */
    private static final ProtocolVersion DEFAULT_VERSION = ProtocolVersion.LATEST_VER;

    /** Supported features. */
    private static final BitSet SUPPORTED_FEATURES = ProtocolBitmaskFeature.featuresAsBitSet(EnumSet.of(
            ProtocolBitmaskFeature.USER_ATTRIBUTES,
            ProtocolBitmaskFeature.TABLE_GET_REQS_USE_QUALIFIED_NAME,
            ProtocolBitmaskFeature.TX_DIRECT_MAPPING,
            ProtocolBitmaskFeature.PLATFORM_COMPUTE_JOB,
            ProtocolBitmaskFeature.COMPUTE_TASK_ID,
            ProtocolBitmaskFeature.TX_DELAYED_ACKS,
            ProtocolBitmaskFeature.TX_PIGGYBACK,
            ProtocolBitmaskFeature.TX_ALLOW_NOOP_ENLIST,
            ProtocolBitmaskFeature.SQL_PARTITION_AWARENESS,
            ProtocolBitmaskFeature.SQL_DIRECT_TX_MAPPING,
            ProtocolBitmaskFeature.TX_CLIENT_GETALL_SUPPORTS_TX_OPTIONS,
            ProtocolBitmaskFeature.SQL_MULTISTATEMENT_SUPPORT
    ));

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
    private final ConcurrentMap<Long, TimeoutObjectImpl> pendingReqs = new ConcurrentHashMap<>();

    /** Notification handlers. */
    private final Map<Long, CompletableFuture<PayloadInputChannel>> notificationHandlers = new ConcurrentHashMap<>();

    /** Topology change listeners. */
    private final Consumer<Long> assignmentChangeListener;

    /** Observable timestamp listeners. */
    private final Consumer<Long> observableTimestampListener;

    /** Inflights. */
    private final ClientTransactionInflights inflights;

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

    /** Last receive operation timestamp. */
    private volatile long lastReceiveMillis;

    /** Whether tcp connection was established. */
    private volatile boolean tcpConnectionEstablished;

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
            Consumer<Long> observableTimestampListener,
            ClientTransactionInflights inflights) {
        validateConfiguration(cfg);
        this.cfg = cfg;
        this.metrics = metrics;
        this.assignmentChangeListener = assignmentChangeListener;
        this.observableTimestampListener = observableTimestampListener;
        this.inflights = inflights;

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
                    if (log.isInfoEnabled()) {
                        log.info("Connection established [remoteAddress=" + s.remoteAddress() + ']');
                    }

                    tcpConnectionEstablished = true;

                    ClientTimeoutWorker.INSTANCE.registerClientChannel(this, cfg.clientConfiguration());

                    sock = s;

                    return handshakeAsync(DEFAULT_VERSION);
                })
                .whenComplete((res, err) -> {
                    if (err != null) {
                        close(err, false);
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
            Consumer<Long> observableTimestampListener,
            ClientTransactionInflights inflights) {
        //noinspection resource - returned from method.
        return new TcpClientChannel(cfg, metrics, assignmentChangeListener, observableTimestampListener, inflights)
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
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        if (log.isInfoEnabled()) {
            log.info("Connection closed [remoteAddress=" + cfg.getAddress() + ", graceful=" + graceful + ']');
        }

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

        for (TimeoutObjectImpl pendingReq : pendingReqs.values()) {
            if (tcpConnectionEstablished && lastReceiveMillis == 0) {
                pendingReq.future().completeExceptionally(
                        new IgniteClientConnectionException(CONNECTION_ERR,
                                "Channel is closed, cluster might not have been initialised", endpoint(), cause));
            } else {
                pendingReq.future().completeExceptionally(
                        new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed", endpoint(), cause));
            }
        }

        for (CompletableFuture<PayloadInputChannel> handler : notificationHandlers.values()) {
            try {
                handler.completeExceptionally(
                        new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed", endpoint(), cause));
            } catch (Throwable ignored) {
                // Ignore.
            }
        }

        if (sock != null) {
            try {
                sock.close();
            } catch (Throwable t) {
                log.warn("Failed to close the channel [remoteAddress=" + cfg.getAddress() + "]: " + t.getMessage(), t);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onMessage(ByteBuf buf) {
        lastReceiveMillis = System.currentTimeMillis();

        try (var unpacker = new ClientMessageUnpacker(buf)) {
            processNextMessage(unpacker);
        } catch (Throwable t) {
            close(t, false);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onDisconnected(@Nullable Throwable e) {
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

            return send(opCode, id, payloadWriter, payloadReader, notificationFut, operationTimeout);

        } catch (Throwable t) {
            return failedFuture(t);
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
    private <T> CompletableFuture<T> send(
            int opCode,
            long id,
            @Nullable PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader,
            @Nullable CompletableFuture<PayloadInputChannel> notificationFut,
            long timeout
    ) {
        if (closed()) {
            throw new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed", endpoint());
        }

        var fut = new CompletableFuture<ClientMessageUnpacker>();

        pendingReqs.put(id, new TimeoutObjectImpl(timeout, fut));

        metrics.requestsActiveIncrement();

        PayloadOutputChannel payloadCh = new PayloadOutputChannel(this, new ClientMessagePacker(sock.getBuffer()), id);

        boolean expectedException = false;

        try {
            var req = payloadCh.out();

            req.packInt(opCode);
            req.packLong(id);

            if (payloadWriter != null) {
                payloadWriter.accept(payloadCh);
            }

            write(req).addListener(f -> {
                if (!f.isSuccess()) {
                    String msg = "Failed to send request async [id=" + id + ", op=" + opCode + ", remoteAddress=" + cfg.getAddress() + "]";
                    IgniteClientConnectionException ex = new IgniteClientConnectionException(CONNECTION_ERR, msg, endpoint(), f.cause());
                    fut.completeExceptionally(ex);
                    log.warn(msg + "]: " + f.cause().getMessage());

                    pendingReqs.remove(id);
                    metrics.requestsActiveDecrement();

                    // Close immediately, do not wait for onDisconnected call from Netty.
                    onDisconnected(ex);
                } else {
                    metrics.requestsSentIncrement();

                    Runnable action = payloadCh.onSentAction();
                    if (action != null) {
                        asyncContinuationExecutor.execute(action);
                    }
                }
            });

            // Allow parallelism for batch operations.
            if (PublicApiThreading.executingSyncPublicApi() && !ClientOp.isBatch(opCode)) {
                // We are in the public API (user) thread, deserialize the response here.
                try {
                    ClientMessageUnpacker unpacker = fut.join();

                    return completedFuture(complete(payloadReader, notificationFut, unpacker));
                } catch (Throwable t) {
                    expectedException = true;
                    throw sneakyThrow(ViewUtils.ensurePublicException(t));
                }
            }

            // Handle the response in the async continuation pool with completeAsync.
            CompletableFuture<T> resFut = new CompletableFuture<>();

            fut.handle((unpacker, err) -> {
                completeAsync(payloadReader, notificationFut, unpacker, err, resFut);
                return null;
            });

            return resFut;
        } catch (Throwable t) {
            if (expectedException) {
                // Just re-throw.
                throw sneakyThrow(t);
            }

            log.warn("Failed to send request sync [id=" + id + ", op=" + opCode + ", remoteAddress=" + cfg.getAddress() + "]: "
                    + t.getMessage(), t);

            // Close buffer manually on fail. Successful write closes the buffer automatically.
            payloadCh.close();
            pendingReqs.remove(id);

            metrics.requestsActiveDecrement();

            throw sneakyThrow(ViewUtils.ensurePublicException(t));
        }
    }

    private <T> void completeAsync(
            @Nullable PayloadReader<T> payloadReader,
            @Nullable CompletableFuture<PayloadInputChannel> notificationFut,
            ClientMessageUnpacker unpacker,
            @Nullable Throwable err,
            CompletableFuture<T> resFut
    ) {
        if (err != null) {
            assert unpacker == null : "unpacker must be null if err is not null";

            try {
                asyncContinuationExecutor.execute(() -> resFut.completeExceptionally(ViewUtils.ensurePublicException(err)));
            } catch (Throwable execError) {
                // Executor error, complete directly.
                execError.addSuppressed(err);
                resFut.completeExceptionally(ViewUtils.ensurePublicException(execError));
            }

            return;
        }

        try {
            // Use asyncContinuationExecutor explicitly to close unpacker if the executor throws.
            // With handleAsync et al we can't close the unpacker in that case.
            asyncContinuationExecutor.execute(() -> {
                try {
                    resFut.complete(complete(payloadReader, notificationFut, unpacker));
                } catch (Throwable t) {
                    resFut.completeExceptionally(ViewUtils.ensurePublicException(t));
                }
            });
        } catch (Throwable execErr) {
            unpacker.close();

            // Executor error, complete directly.
            resFut.completeExceptionally(ViewUtils.ensurePublicException(execErr));
        }
    }

    /**
     * Completes the request future.
     *
     * @param payloadReader Payload reader.
     * @param notificationFut Notify future.
     * @param unpacker Unpacked message.
     */
    private <T> @Nullable T complete(
            @Nullable PayloadReader<T> payloadReader,
            @Nullable CompletableFuture<PayloadInputChannel> notificationFut,
            ClientMessageUnpacker unpacker
    ) {
        try (unpacker) {
            if (payloadReader != null) {
                return payloadReader.apply(new PayloadInputChannel(this, unpacker, notificationFut));
            }

            return null;
        } catch (Throwable e) {
            log.error("Failed to deserialize server response [remoteAddress=" + cfg.getAddress() + "]: " + e.getMessage(), e);

            throw new IgniteException(PROTOCOL_ERR, "Failed to deserialize server response: " + e.getMessage(), e);
        }
    }

    /**
     * Process next message from the input stream and complete corresponding future.
     */
    private void processNextMessage(ClientMessageUnpacker unpacker) throws IgniteException {
        if (protocolCtx == null) {
            // Process handshake.
            completeRequestFuture(pendingReqs.remove(-1L).future(), unpacker);
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

        TimeoutObjectImpl pendingReq = pendingReqs.remove(resId);

        if (pendingReq == null) {
            log.error("Unexpected response ID [remoteAddress=" + cfg.getAddress() + "]: " + resId);

            throw new IgniteClientConnectionException(PROTOCOL_ERR, String.format("Unexpected response ID [%s]", resId), endpoint());
        }

        metrics.requestsActiveDecrement();

        if (err == null) {
            metrics.requestsCompletedIncrement();

            completeRequestFuture(pendingReq.future(), unpacker);
        } else {
            metrics.requestsFailedIncrement();
            notificationHandlers.remove(resId);

            pendingReq.future().completeExceptionally(err);
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
            // Default notification handler. Used to deliver delayed replication acks.
            if (err != null) {
                if (err instanceof ClientDelayedAckException) {
                    ClientDelayedAckException err0 = (ClientDelayedAckException) err;

                    inflights.removeInflight(err0.txId(), new TransactionException(err0.code(), err0.getMessage(), err0.getCause()));
                }

                // Can't do anything to remove stuck inflight.
                return;
            }

            UUID txId = unpacker.unpackUuid();
            inflights.removeInflight(txId, err);

            return;
        }

        completeNotificationFuture(handler, unpacker, err);
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

        int extSize = unpacker.tryUnpackNil() ? 0 : unpacker.unpackInt();
        int expectedSchemaVersion = -1;
        long[] sqlUpdateCounters = null;
        UUID txId = null;

        for (int i = 0; i < extSize; i++) {
            String key = unpacker.unpackString();

            if (key.equals(ErrorExtensions.EXPECTED_SCHEMA_VERSION)) {
                expectedSchemaVersion = unpacker.unpackInt();
            } else if (key.equals(ErrorExtensions.SQL_UPDATE_COUNTERS)) {
                sqlUpdateCounters = unpacker.unpackLongArray();
            } else if (key.equals(ErrorExtensions.DELAYED_ACK)) {
                txId = unpacker.unpackUuid();
            } else {
                // Unknown extension - ignore.
                unpacker.skipValues(1);
            }
        }

        if (txId != null) {
            return new ClientDelayedAckException(traceId, code, errMsg, txId, causeWithStackTrace);
        }

        if (sqlUpdateCounters != null) {
            errMsg = errMsg != null ? errMsg : "SQL batch execution error";
            return new SqlBatchException(traceId, code, sqlUpdateCounters, errMsg, causeWithStackTrace);
        }

        if (code == Table.SCHEMA_VERSION_MISMATCH_ERR) {
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

    @Override
    public ClientTransactionInflights inflights() {
        return inflights;
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
    private CompletableFuture<Object> handshakeAsync(ProtocolVersion ver) throws IgniteClientConnectionException {
        var fut = new CompletableFuture<ClientMessageUnpacker>();
        pendingReqs.put(-1L, new TimeoutObjectImpl(connectTimeout, fut));

        handshakeReqAsync(ver).addListener(f -> {
            if (!f.isSuccess()) {
                fut.completeExceptionally(
                        new IgniteClientConnectionException(CONNECTION_ERR, "Failed to send handshake request", endpoint(), f.cause()));
            }
        });

        CompletableFuture<Object> resFut = new CompletableFuture<>();

        fut.handle((unpacker, err) -> {
            completeAsync(r -> handshakeRes(r.in()), null, unpacker, err, resFut);
            return null;
        });

        return resFut.exceptionally(err -> {
            if (unwrapRootCause(err) instanceof TimeoutException) {
                metrics.handshakesFailedTimeoutIncrement();
                throw new IgniteClientConnectionException(CONNECTION_ERR, "Handshake timeout", endpoint(), err);
            }
            metrics.handshakesFailedIncrement();
            throw new IgniteClientConnectionException(CONNECTION_ERR, "Handshake error", endpoint(), err);
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

        req.packInt(HandshakeUtils.CLIENT_TYPE_GENERAL);

        HandshakeUtils.packFeatures(req, SUPPORTED_FEATURES);

        IgniteClientAuthenticator authenticator = cfg.clientConfiguration().authenticator();
        if (authenticator != null) {
            Map<HandshakeExtension, Object> extensions = Map.of(
                    HandshakeExtension.AUTHENTICATION_TYPE, authenticator.type(),
                    HandshakeExtension.AUTHENTICATION_IDENTITY, authenticator.identity(),
                    HandshakeExtension.AUTHENTICATION_SECRET, authenticator.secret());

            HandshakeUtils.packExtensions(req, extensions);
        } else {
            HandshakeUtils.packExtensions(req, Map.of());
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
            UUID clusterNodeId = unpacker.unpackUuid();
            var clusterNodeName = unpacker.unpackString();
            var addr = sock.remoteAddress();
            var clusterNode = new ClientClusterNode(clusterNodeId, clusterNodeName, new NetworkAddress(addr.getHostName(), addr.getPort()));

            int clusterIdsLen = unpacker.unpackInt();
            if (clusterIdsLen <= 0) {
                throw new IgniteClientConnectionException(PROTOCOL_ERR, "Unexpected cluster ids count: " + clusterIdsLen, endpoint());
            }

            List<UUID> clusterIds = new ArrayList<>(clusterIdsLen);
            for (int i = 0; i < clusterIdsLen; i++) {
                clusterIds.add(unpacker.unpackUuid());
            }

            var clusterName = unpacker.unpackString();

            long observableTimestamp = unpacker.unpackLong();
            observableTimestampListener.accept(observableTimestamp);

            byte major = unpacker.unpackByte(); // cluster version major
            byte minor = unpacker.unpackByte(); // cluster version minor
            byte maintenance = unpacker.unpackByte(); // cluster version maintenance
            Byte patch = unpacker.unpackByteNullable(); // cluster version patch
            String preRelease = unpacker.unpackStringNullable(); // cluster version pre release

            IgniteProductVersion nodeProductVersion = new IgniteProductVersion(major, minor, maintenance, patch, preRelease);

            BitSet serverFeatures = HandshakeUtils.unpackFeatures(unpacker);
            HandshakeUtils.unpackExtensions(unpacker);

            BitSet mutuallySupportedFeatures = HandshakeUtils.supportedFeatures(SUPPORTED_FEATURES, serverFeatures);
            EnumSet<ProtocolBitmaskFeature> features = ProtocolBitmaskFeature.enumSet(mutuallySupportedFeatures);

            protocolCtx = new ProtocolContext(srvVer, features, serverIdleTimeout, clusterNode, clusterIds, clusterName,
                    nodeProductVersion);

            return null;
        } catch (Throwable e) {
            log.warn("Failed to handle handshake response [remoteAddress=" + cfg.getAddress() + "]: " + e.getMessage(), e);

            throw e;
        }
    }

    /** Write bytes to the output stream. */
    private ChannelFuture write(ClientMessagePacker packer) throws IgniteClientConnectionException {
        // Ignore race condition here.
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

    @Override
    public String toString() {
        return S.toString(TcpClientChannel.class.getSimpleName(), "remoteAddress", sock.remoteAddress(), false);
    }

    @Override
    public String endpoint() {
        return cfg.getAddress().toString();
    }

    private static void completeRequestFuture(CompletableFuture<ClientMessageUnpacker> fut, ClientMessageUnpacker unpacker) {
        // Add reference count before jumping onto another thread (due to handleAsync() in send()).
        unpacker.retain();

        try {
            if (!fut.complete(unpacker)) {
                unpacker.close();
            }
        } catch (Throwable t) {
            unpacker.close();
            throw t;
        }
    }

    private void completeNotificationFuture(
            CompletableFuture<PayloadInputChannel> fut,
            ClientMessageUnpacker unpacker,
            @Nullable Throwable err) {
        if (err != null) {
            asyncContinuationExecutor.execute(() -> fut.completeExceptionally(err));
            return;
        }

        // Add reference count before jumping onto another thread.
        unpacker.retain();

        try {
            asyncContinuationExecutor.execute(() -> {
                try {
                    if (!fut.complete(new PayloadInputChannel(this, unpacker, null))) {
                        unpacker.close();
                    }
                } catch (Throwable e) {
                    unpacker.close();

                    log.error("Failed to handle server notification [remoteAddress=" + cfg.getAddress() + "]: " + e.getMessage(), e);
                }
            });
        } catch (Throwable t) {
            unpacker.close();
            throw t;
        }
    }

    void checkTimeouts(long now) {
        for (Entry<Long, TimeoutObjectImpl> req : pendingReqs.entrySet()) {
            TimeoutObject<CompletableFuture<ClientMessageUnpacker>> timeoutObject = req.getValue();

            if (timeoutObject != null && timeoutObject.endTime() > 0 && now > timeoutObject.endTime()) {
                // Client-facing future will fail with a timeout, but internal ClientRequestFuture will stay in the map -
                // otherwise we'll fail with "protocol breakdown" error when a late response arrives from the server.
                CompletableFuture<?> fut = timeoutObject.future();
                fut.completeExceptionally(new TimeoutException());
            }
        }
    }

    /**
     * Timeout object wrapper for the completable future.
     */
    private static class TimeoutObjectImpl implements TimeoutObject<CompletableFuture<ClientMessageUnpacker>> {
        /** End time (milliseconds since Unix epoch). */
        private final long endTime;

        /** Target future. */
        private final CompletableFuture<ClientMessageUnpacker> fut;

        /**
         * Constructor.
         *
         * @param timeout Timeout in milliseconds.
         * @param fut Target future.
         */
        private TimeoutObjectImpl(long timeout, CompletableFuture<ClientMessageUnpacker> fut) {
            this.endTime = timeout > 0 ? coarseCurrentTimeMillis() + timeout : 0;
            this.fut = fut;
        }

        @Override
        public long endTime() {
            return endTime;
        }

        @Override
        public CompletableFuture<ClientMessageUnpacker> future() {
            return fut;
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

                    if (heartbeatTimeout > 0) {
                        fut
                                .orTimeout(heartbeatTimeout, TimeUnit.MILLISECONDS)
                                .exceptionally(e -> {
                                    if (e instanceof TimeoutException) {
                                        long lastResponseAge = System.currentTimeMillis() - lastReceiveMillis;

                                        if (lastResponseAge < heartbeatTimeout) {
                                            // The last response was received within the timeout, so the connection is still alive.
                                            // Ignore the timeout from heartbeat message.
                                            return null;
                                        }

                                        log.warn("Heartbeat timeout, closing the channel [remoteAddress=" + cfg.getAddress() + ']');

                                        close(new IgniteClientConnectionException(
                                                CONNECTION_ERR, "Heartbeat timeout", endpoint(), e), false);
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
