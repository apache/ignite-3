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

    /** Protocol context. */
    private volatile ProtocolContext protocolCtx;

    /** Channel. */
    private final ClientConnection sock;

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

    /** Heartbeat timer. */
    private final Timer heartbeatTimer;

    /** Last send operation timestamp. */
    private volatile long lastSendMillis;

    /**
     * Constructor.
     *
     * @param cfg     Config.
     * @param connMgr Connection multiplexer.
     */
    TcpClientChannel(ClientChannelConfiguration cfg, ClientConnectionMultiplexer connMgr) {
        validateConfiguration(cfg);

        asyncContinuationExecutor = cfg.clientConfiguration().asyncContinuationExecutor() == null
                ? ForkJoinPool.commonPool()
                : cfg.clientConfiguration().asyncContinuationExecutor();

        connectTimeout = cfg.clientConfiguration().connectTimeout();

        sock = connMgr.open(cfg.getAddress(), this, this);

        handshake(DEFAULT_VERSION);

        // Netty has a built-in IdleStateHandler to detect idle connections (used on the server side).
        // However, to adjust the heartbeat interval dynamically, we have to use a timer here.
        heartbeatTimer = initHeartbeat(cfg.clientConfiguration().heartbeatInterval());
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        close(null);
    }

    /**
     * Close the channel with cause.
     */
    private void close(Exception cause) {
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
            throw new IgniteClientConnectionException(PROTOCOL_ERR, "Unexpected message type: " + type);
        }

        Long resId = unpacker.unpackLong();

        ClientRequestFuture pendingReq = pendingReqs.remove(resId);

        if (pendingReq == null) {
            throw new IgniteClientConnectionException(PROTOCOL_ERR, String.format("Unexpected response ID [%s]", resId));
        }

        int flags = unpacker.unpackInt();

        if (ResponseFlags.getPartitionAssignmentChangedFlag(flags)) {
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
    private IgniteException readError(ClientMessageUnpacker unpacker) {
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
    private void handshake(ProtocolVersion ver)
            throws IgniteClientConnectionException {
        ClientRequestFuture fut = new ClientRequestFuture();
        pendingReqs.put(-1L, fut);

        try {
            handshakeReq(ver);

            var res = connectTimeout > 0 ? fut.get(connectTimeout, TimeUnit.MILLISECONDS) : fut.get();
            handshakeRes(res, ver);
        } catch (Throwable e) {
            throw IgniteException.wrap(e);
        }
    }

    /** Send handshake request. */
    private void handshakeReq(ProtocolVersion proposedVer) {
        sock.send(Unpooled.wrappedBuffer(ClientMessageCommon.MAGIC_BYTES));

        var req = new ClientMessagePacker(sock.getBuffer());
        req.packInt(proposedVer.major());
        req.packInt(proposedVer.minor());
        req.packInt(proposedVer.patch());

        req.packInt(2); // Client type: general purpose.

        req.packBinaryHeader(0); // Features.
        req.packMapHeader(0); // Extensions.

        write(req).syncUninterruptibly();
    }

    /** Receive and handle handshake response. */
    private void handshakeRes(ClientMessageUnpacker unpacker, ProtocolVersion proposedVer) {
        try (unpacker) {
            ProtocolVersion srvVer = new ProtocolVersion(unpacker.unpackShort(), unpacker.unpackShort(),
                    unpacker.unpackShort());

            if (!unpacker.tryUnpackNil()) {
                if (!proposedVer.equals(srvVer) && supportedVers.contains(srvVer)) {
                    // Retry with server version.
                    handshake(srvVer);
                    return;
                }

                throw readError(unpacker);
            }

            var serverIdleTimeout = unpacker.unpackLong();
            var clusterNodeId = unpacker.unpackString();
            var clusterNodeName = unpacker.unpackString();
            var addr = sock.remoteAddress();
            var clusterNode = new ClusterNode(clusterNodeId, clusterNodeName, new NetworkAddress(addr.getHostName(), addr.getPort()));

            var featuresLen = unpacker.unpackBinaryHeader();
            unpacker.skipValues(featuresLen);

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValues(extensionsLen);

            protocolCtx = new ProtocolContext(srvVer, ProtocolBitmaskFeature.allFeaturesAsEnumSet(), serverIdleTimeout, clusterNode);
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
        public HeartbeatTask(long interval) {
            this.interval = interval;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                if (System.currentTimeMillis() - lastSendMillis > interval) {
                    serviceAsync(ClientOp.HEARTBEAT, null, null);
                }
            } catch (Throwable ignored) {
                // Ignore failed heartbeats.
            }
        }
    }
}
