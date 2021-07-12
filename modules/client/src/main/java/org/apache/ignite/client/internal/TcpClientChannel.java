/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.internal;

import org.apache.ignite.client.ClientErrorCode;
import org.apache.ignite.client.ClientMessagePacker;
import org.apache.ignite.client.ClientMessageUnpacker;
import org.apache.ignite.client.ClientOp;
import org.apache.ignite.client.IgniteClientAuthenticationException;
import org.apache.ignite.client.IgniteClientAuthorizationException;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.client.internal.io.ClientConnection;
import org.apache.ignite.client.internal.io.ClientConnectionMultiplexer;
import org.apache.ignite.client.internal.io.ClientConnectionStateHandler;
import org.apache.ignite.client.internal.io.ClientMessageHandler;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.msgpack.core.buffer.ByteBufferInput;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;


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

    /** Protocol context. */
    private volatile ProtocolContext protocolCtx;

    /** Server node ID. */
    private volatile UUID srvNodeId;

    /** Channel. */
    private final ClientConnection sock;

    /** Request id. */
    private final AtomicLong reqId = new AtomicLong(1);

    /** Pending requests. */
    private final Map<Long, ClientRequestFuture> pendingReqs = new ConcurrentHashMap<>();

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Executor for async operation listeners. */
    private final Executor asyncContinuationExecutor;

    /** Send/receive timeout in milliseconds. */
    private final int timeout;

    /** Constructor. */
    TcpClientChannel(ClientChannelConfiguration cfg, ClientConnectionMultiplexer connMgr)
            throws IgniteClientException, InterruptedException {
        validateConfiguration(cfg);

        asyncContinuationExecutor = ForkJoinPool.commonPool();

        timeout = cfg.getTimeout();

        sock = connMgr.open(cfg.getAddress(), this, this);

        handshake(DEFAULT_VERSION);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        close(null);
    }

    /** {@inheritDoc} */
    @Override public void onMessage(ByteBuffer buf) throws IOException {
        processNextMessage(buf);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(@Nullable Exception e) {
        close(e);
    }

    /**
     * Close the channel with cause.
     */
    private void close(Exception cause) {
        if (closed.compareAndSet(false, true)) {
            sock.close();

            for (ClientRequestFuture pendingReq : pendingReqs.values())
                pendingReq.completeExceptionally(new IgniteClientConnectionException("Channel is closed", cause));
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T service(
            ClientOp op,
            Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader
    ) throws IgniteClientException {
        ClientRequestFuture fut = send(op, payloadWriter);

        return receive(fut, payloadReader);
    }

    /** {@inheritDoc} */
    @Override public <T> CompletableFuture<T> serviceAsync(
            ClientOp op,
            Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader
    ) {
        try {
            ClientRequestFuture fut = send(op, payloadWriter);

            return receiveAsync(fut, payloadReader);
        }
        catch (Throwable t) {
            CompletableFuture<T> fut = new CompletableFuture<>();
            fut.completeExceptionally(t);

            return fut;
        }
    }

    /**
     * @param op Operation.
     * @param payloadWriter Payload writer to stream or {@code null} if request has no payload.
     * @return Request future.
     */
    private ClientRequestFuture send(ClientOp op, Consumer<PayloadOutputChannel> payloadWriter)
            throws IgniteClientException {
        long id = reqId.getAndIncrement();

        try (PayloadOutputChannel payloadCh = new PayloadOutputChannel(this)) {
            if (closed())
                throw new IgniteClientConnectionException("Channel is closed");

            ClientRequestFuture fut = new ClientRequestFuture();

            pendingReqs.put(id, fut);

            var req = payloadCh.out();

            req.packInt(op.code());
            req.packLong(id);

            if (payloadWriter != null)
                payloadWriter.accept(payloadCh);

            // TODO: We don't deal with message lengths here, it is the responsibility of the encoder.
            byte[] bytes = req.toByteArray();
            write(bytes, bytes.length);

            return fut;
        }
        catch (Throwable t) {
            pendingReqs.remove(id);

            throw convertException(t);
        }
    }

    /**
     * @param pendingReq Request future.
     * @param payloadReader Payload reader from stream.
     * @return Received operation payload or {@code null} if response has no payload.
     */
    private <T> T receive(ClientRequestFuture pendingReq, Function<PayloadInputChannel, T> payloadReader)
            throws IgniteClientException {
        try {
            ByteBuffer payload = timeout > 0 ? pendingReq.get(timeout, TimeUnit.MILLISECONDS) : pendingReq.get();

            if (payload == null || payloadReader == null)
                return null;

            return payloadReader.apply(new PayloadInputChannel(this, payload));
        }
        catch (Throwable e) {
            throw convertException(e);
        }
    }

    /**
     * Receives the response asynchronously.
     *
     * @param pendingReq Request future.
     * @param payloadReader Payload reader from stream.
     * @return Future for the operation.
     */
    private <T> CompletableFuture<T> receiveAsync(ClientRequestFuture pendingReq, Function<PayloadInputChannel, T> payloadReader) {
        return pendingReq.thenApply(payload -> {
            if (payload == null || payloadReader == null)
                return null;

            return payloadReader.apply(new PayloadInputChannel(this, payload));
        });
    }

    /**
     * Converts exception to {@link IgniteClientException}.
     * @param e Exception to convert.
     * @return Resulting exception.
     */
    private IgniteClientException convertException(Throwable e) {
        // For every known class derived from IgniteClientException, wrap cause in a new instance.
        // We could rethrow e.getCause() when instanceof IgniteClientException,
        // but this results in an incomplete stack trace from the receiver thread.
        // This is similar to IgniteUtils.exceptionConverters.
        if (e.getCause() instanceof IgniteClientConnectionException)
            return new IgniteClientConnectionException(e.getMessage(), e.getCause());

        if (e.getCause() instanceof IgniteClientAuthorizationException)
            return new IgniteClientAuthorizationException(e.getMessage(), e.getCause());

        return new IgniteClientException(e.getMessage(), ClientErrorCode.FAILED, e);
    }

    /**
     * Process next message from the input stream and complete corresponding future.
     */
    private void processNextMessage(ByteBuffer buf) throws IgniteClientException, IOException {
        try (var unpacker = new ClientMessageUnpacker(new ByteBufferInput(buf))) {
            if (protocolCtx == null) {
                // Process handshake.
                pendingReqs.remove(-1L).complete(buf);
                return;
            }

            Long resId = unpacker.unpackLong();

            int status = unpacker.unpackInt();

            ClientRequestFuture pendingReq = pendingReqs.remove(resId);

            if (pendingReq == null)
                throw new IgniteClientException(String.format("Unexpected response ID [%s]", resId));

            if (status == 0) {
                pendingReq.complete(buf);
            } else {
                var errMsg = unpacker.unpackString();
                var err = new IgniteClientException(errMsg, status);
                pendingReq.completeExceptionally(err);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public UUID serverNodeId() {
        return srvNodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean closed() {
        return closed.get();
    }

    private static void validateConfiguration(ClientChannelConfiguration cfg) {
        String error = null;

        InetSocketAddress addr = cfg.getAddress();

        if (addr == null)
            error = "At least one Ignite server node must be specified in the Ignite client configuration";
        else if (addr.getPort() < 1024 || addr.getPort() > 49151)
            error = String.format("Ignite client port %s is out of valid ports range 1024...49151", addr.getPort());

        if (error != null)
            throw new IllegalArgumentException(error);
    }

    /** Client handshake. */
    private void handshake(ProtocolVersion ver)
            throws IgniteClientConnectionException {
        ClientRequestFuture fut = new ClientRequestFuture();
        pendingReqs.put(-1L, fut);

        try {
            handshakeReq(ver);

            ByteBuffer res = timeout > 0 ? fut.get(timeout, TimeUnit.MILLISECONDS) : fut.get();
            handshakeRes(res, ver);
        }
        catch (Throwable e) {
            throw convertException(e);
        }
    }

    /** Send handshake request. */
    private void handshakeReq(ProtocolVersion proposedVer) throws IOException {
        try (var packer = new ClientMessagePacker()) {
            packer.packInt(proposedVer.major());
            packer.packInt(proposedVer.minor());
            packer.packInt(proposedVer.patch());

            packer.packInt(2); // Client type: general purpose.

            packer.packBinaryHeader(0); // Features.
            packer.packMapHeader(0); // Extensions.

            var bytes = packer.toByteArray();

            write(bytes, bytes.length);
        }
    }

    /**
     * @param ver Protocol version.
     * @return Protocol context for a version.
     */
    private ProtocolContext protocolContextFromVersion(ProtocolVersion ver) {
        return new ProtocolContext(ver, ProtocolBitmaskFeature.allFeaturesAsEnumSet());
    }

    /** Receive and handle handshake response. */
    private void handshakeRes(ByteBuffer buf, ProtocolVersion proposedVer)
            throws IgniteClientConnectionException, IgniteClientAuthenticationException {
        try (var unpacker = new ClientMessageUnpacker(new ByteBufferInput(buf))) {
            ProtocolVersion srvVer = new ProtocolVersion(unpacker.unpackShort(), unpacker.unpackShort(),
                    unpacker.unpackShort());

            var errCode = unpacker.unpackInt();

            if (errCode != ClientErrorCode.SUCCESS) {
                var msg = unpacker.unpackString();

                if (errCode == ClientErrorCode.AUTH_FAILED)
                    throw new IgniteClientAuthenticationException(msg);
                else if (proposedVer.equals(srvVer))
                    throw new IgniteClientException("Client protocol error: unexpected server response.");
                else if (!supportedVers.contains(srvVer))
                    throw new IgniteClientException(String.format(
                            "Protocol version mismatch: client %s / server %s. Server details: %s",
                            proposedVer,
                            srvVer,
                            msg
                    ));
                else { // Retry with server version.
                    handshake(srvVer);
                }

                throw new IgniteClientConnectionException(msg);
            }

            var featuresLen = unpacker.unpackBinaryHeader();
            unpacker.skipValue(featuresLen);

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValue(extensionsLen);

            protocolCtx = protocolContextFromVersion(srvVer);
        } catch (IOException e) {
            throw handleIOError(e);
        }
    }

    /** Write bytes to the output stream. */
    private void write(byte[] bytes, int len) throws IgniteClientConnectionException {
        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, len);

        try {
            sock.send(buf);
        }
        catch (IgniteException e) {
            throw new IgniteClientConnectionException(e.getMessage(), e);
        }
    }

    /**
     * @param ex IO exception (cause).
     */
    private IgniteClientException handleIOError(@Nullable IOException ex) {
        return handleIOError("sock=" + sock, ex);
    }

    /**
     * @param chInfo Additional channel info
     * @param ex IO exception (cause).
     */
    private IgniteClientException handleIOError(String chInfo, @Nullable IOException ex) {
        return new IgniteClientConnectionException("Ignite cluster is unavailable [" + chInfo + ']', ex);
    }

    /**
     *
     */
    private static class ClientRequestFuture extends CompletableFuture<ByteBuffer> {
    }
}
