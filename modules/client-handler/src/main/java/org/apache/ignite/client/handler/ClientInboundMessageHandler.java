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

package org.apache.ignite.client.handler;

import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.PLATFORM_COMPUTE_JOB;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.TX_DIRECT_MAPPING;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.firstNotNull;
import static org.apache.ignite.lang.ErrorGroups.Client.HANDSHAKE_HEADER_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_COMPATIBILITY_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.SSLException;
import org.apache.ignite.client.handler.configuration.ClientConnectorView;
import org.apache.ignite.client.handler.requests.cluster.ClientClusterGetNodesRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeCancelRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeChangePriorityRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteColocatedRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteMapReduceRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeExecutePartitionedRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeGetStateRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcCancelRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcCloseRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcColumnMetadataRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcConnectRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcExecuteBatchRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcExecuteRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcFetchRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcFinishTxRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcHasMoreRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcPreparedStmntBatchRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcPrimaryKeyMetadataRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcSchemasMetadataRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcTableMetadataRequest;
import org.apache.ignite.client.handler.requests.jdbc.JdbcMetadataCatalog;
import org.apache.ignite.client.handler.requests.sql.ClientSqlCancelRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlCursorCloseRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlCursorNextPageRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlExecuteBatchRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlExecuteRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlExecuteScriptRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlQueryMetadataRequest;
import org.apache.ignite.client.handler.requests.table.ClientSchemasGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientStreamerBatchSendRequest;
import org.apache.ignite.client.handler.requests.table.ClientStreamerWithReceiverBatchSendRequest;
import org.apache.ignite.client.handler.requests.table.ClientTableGetQualifiedRequest;
import org.apache.ignite.client.handler.requests.table.ClientTableGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTablePartitionPrimaryReplicasGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTablesGetQualifiedRequest;
import org.apache.ignite.client.handler.requests.table.ClientTablesGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleContainsAllKeysRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleContainsKeyRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteAllExactRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteExactRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndDeleteRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndReplaceRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndUpsertRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleInsertAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleInsertRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleReplaceExactRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleReplaceRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleUpsertAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleUpsertRequest;
import org.apache.ignite.client.handler.requests.table.partition.ClientTablePartitionPrimaryReplicasNodesGetRequest;
import org.apache.ignite.client.handler.requests.tx.ClientTransactionBeginRequest;
import org.apache.ignite.client.handler.requests.tx.ClientTransactionCommitRequest;
import org.apache.ignite.client.handler.requests.tx.ClientTransactionRollbackRequest;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.client.proto.ClientComputeJobPacker;
import org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker;
import org.apache.ignite.internal.client.proto.ClientMessageCommon;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.ErrorExtensions;
import org.apache.ignite.internal.client.proto.HandshakeExtension;
import org.apache.ignite.internal.client.proto.HandshakeUtils;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.client.proto.ResponseFlags;
import org.apache.ignite.internal.client.proto.ServerOp;
import org.apache.ignite.internal.client.proto.ServerOpResponseFlags;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.executor.platform.PlatformComputeConnection;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryCursorHandler;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.schema.SchemaVersionMismatchException;
import org.apache.ignite.internal.security.authentication.AnonymousRequest;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationRequest;
import org.apache.ignite.internal.security.authentication.UserDetails;
import org.apache.ignite.internal.security.authentication.UsernamePasswordRequest;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEvent;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEventParameters;
import org.apache.ignite.internal.security.authentication.event.AuthenticationProviderEventParameters;
import org.apache.ignite.internal.security.authentication.event.UserEventParameters;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersionsImpl;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.security.AuthenticationType;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;
import org.apache.ignite.sql.SqlBatchException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Handles messages from thin clients.
 *
 * <p>All message handling is sequential, {@link #channelRead} and other handlers are invoked on a single thread.</p>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientInboundMessageHandler
        extends ChannelInboundHandlerAdapter
        implements EventListener<AuthenticationEventParameters> {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ClientInboundMessageHandler.class);

    private static final byte STATE_BEFORE_HANDSHAKE = 0;

    private static final byte STATE_HANDSHAKE_REQUESTED = 1;

    private static final byte STATE_HANDSHAKE_RESPONSE_SENT = 2;

    /** Ignite tables API. */
    private final IgniteTablesInternal igniteTables;

    /** Transaction manager. */
    private final TxManager txManager;

    /** JDBC Handler. */
    private final JdbcQueryEventHandlerImpl jdbcQueryEventHandler;

    /** Connection resources. */
    private final ClientResourceRegistry resources = new ClientResourceRegistry();

    /** Configuration. */
    private final ClientConnectorView configuration;

    /** Compute. */
    private final IgniteComputeInternal compute;

    /** Cluster. */
    private final ClusterService clusterService;

    /** Query processor. */
    private final QueryProcessor queryProcessor;

    /** SQL query cursor handler. */
    private final JdbcQueryCursorHandler jdbcQueryCursorHandler;

    /** Cluster ID. */
    private final Supplier<ClusterInfo> clusterInfoSupplier;

    /** Metrics. */
    private final ClientHandlerMetricSource metrics;

    private final ClockService clockService;

    /** Context. */
    private ClientContext clientContext;

    /** Current state. */
    private byte state = STATE_BEFORE_HANDSHAKE;

    /** Chanel handler context. */
    private volatile ChannelHandlerContext channelHandlerContext;

    /** Primary replicas update counter. */
    private final AtomicLong primaryReplicaMaxStartTime;

    private final ClientPrimaryReplicaTracker primaryReplicaTracker;

    /** Authentication manager. */
    private final AuthenticationManager authenticationManager;

    private final SchemaVersions schemaVersions;

    private final long connectionId;

    private final Executor partitionOperationsExecutor;

    private final BitSet features;

    private final Map<HandshakeExtension, Object> extensions;

    private final Map<Long, CancelHandle> cancelHandles = new ConcurrentHashMap<>();

    private final Function<String, CompletableFuture<PlatformComputeConnection>> computeConnectionFunc;

    private final AtomicLong serverToClientRequestId = new AtomicLong(-1);

    private final Map<Long, CompletableFuture<ClientMessageUnpacker>> serverToClientRequests = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param igniteTables Ignite tables API entry point.
     * @param processor Sql query processor.
     * @param configuration Configuration.
     * @param compute Compute.
     * @param clusterService Cluster.
     * @param clusterInfoSupplier Cluster info supplier.
     * @param metrics Metrics.
     * @param authenticationManager Authentication manager.
     * @param clockService Clock service.
     * @param schemaSyncService Schema sync service.
     * @param catalogService Catalog service.
     * @param connectionId Connection ID.
     * @param primaryReplicaTracker Primary replica tracker.
     * @param partitionOperationsExecutor Partition operations executor.
     * @param features Features.
     * @param extensions Extensions.
     */
    public ClientInboundMessageHandler(
            IgniteTablesInternal igniteTables,
            TxManager txManager,
            QueryProcessor processor,
            ClientConnectorView configuration,
            IgniteComputeInternal compute,
            ClusterService clusterService,
            Supplier<ClusterInfo> clusterInfoSupplier,
            ClientHandlerMetricSource metrics,
            AuthenticationManager authenticationManager,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            long connectionId,
            ClientPrimaryReplicaTracker primaryReplicaTracker,
            Executor partitionOperationsExecutor,
            BitSet features,
            Map<HandshakeExtension, Object> extensions,
            Function<String, CompletableFuture<PlatformComputeConnection>> computeConnectionFunc
    ) {
        assert igniteTables != null;
        assert txManager != null;
        assert processor != null;
        assert configuration != null;
        assert compute != null;
        assert clusterService != null;
        assert clusterInfoSupplier != null;
        assert metrics != null;
        assert authenticationManager != null;
        assert clockService != null;
        assert schemaSyncService != null;
        assert catalogService != null;
        assert primaryReplicaTracker != null;
        assert partitionOperationsExecutor != null;
        assert features != null;
        assert extensions != null;

        this.igniteTables = igniteTables;
        this.txManager = txManager;
        this.configuration = configuration;
        this.compute = compute;
        this.clusterService = clusterService;
        this.queryProcessor = processor;
        this.clusterInfoSupplier = clusterInfoSupplier;
        this.metrics = metrics;
        this.authenticationManager = authenticationManager;
        this.clockService = clockService;
        this.primaryReplicaTracker = primaryReplicaTracker;
        this.partitionOperationsExecutor = partitionOperationsExecutor;

        jdbcQueryCursorHandler = new JdbcQueryCursorHandlerImpl(resources);
        jdbcQueryEventHandler = new JdbcQueryEventHandlerImpl(
                processor,
                new JdbcMetadataCatalog(clockService, schemaSyncService, catalogService),
                resources,
                txManager
        );

        schemaVersions = new SchemaVersionsImpl(schemaSyncService, catalogService, clockService);
        this.connectionId = connectionId;

        this.primaryReplicaMaxStartTime = new AtomicLong(HybridTimestamp.MIN_VALUE.longValue());

        this.features = features;
        this.extensions = extensions;

        this.computeConnectionFunc = computeConnectionFunc;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        authenticationEventsToSubscribe().forEach(event -> authenticationManager.listen(event, this));
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        authenticationEventsToSubscribe().forEach(event -> authenticationManager.removeListener(event, this));
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        channelHandlerContext = ctx;
        super.channelRegistered(ctx);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Connection registered [connectionId=" + connectionId + ", remoteAddress=" + ctx.channel().remoteAddress() + "]");
        }
    }

    /** {@inheritDoc} */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf byteBuf = (ByteBuf) msg;

        // Each inbound handler in a pipeline has to release the received messages.
        var unpacker = new ClientMessageUnpacker(byteBuf);
        metrics.bytesReceivedAdd(byteBuf.readableBytes() + ClientMessageCommon.HEADER_SIZE);

        switch (state) {
            case STATE_BEFORE_HANDSHAKE:
                state = STATE_HANDSHAKE_REQUESTED;
                metrics.bytesReceivedAdd(ClientMessageCommon.MAGIC_BYTES.length);
                // Packer buffer is released by Netty on send, or by inner exception handlers below.
                var packer = getPacker(ctx.alloc());
                handshake(ctx, unpacker, packer);

                break;

            case STATE_HANDSHAKE_REQUESTED:
                // Handshake is in progress, any messages are not allowed.
                throw new IgniteException(PROTOCOL_ERR, "Unexpected message received before handshake completion");

            case STATE_HANDSHAKE_RESPONSE_SENT:
                assert clientContext != null : "Client context != null";
                processOperation(ctx, unpacker);

                break;

            default:
                throw new IllegalStateException("Unexpected state: " + state);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        resources.close();

        super.channelInactive(ctx);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Connection closed [connectionId=" + connectionId + ", remoteAddress=" + ctx.channel().remoteAddress() + "]");
        }
    }

    private void handshake(ChannelHandlerContext ctx, ClientMessageUnpacker unpacker, ClientMessagePacker packer) {
        try {
            writeMagic(ctx);
            var clientVer = ProtocolVersion.unpack(unpacker);

            if (!clientVer.equals(ProtocolVersion.LATEST_VER)) {
                throw new IgniteException(PROTOCOL_COMPATIBILITY_ERR, "Unsupported version: "
                        + clientVer.major() + "." + clientVer.minor() + "." + clientVer.patch());
            }

            int clientCode = unpacker.unpackInt();

            BitSet clientFeatures = HandshakeUtils.unpackFeatures(unpacker);
            Map<HandshakeExtension, Object> clientHandshakeExtensions = HandshakeUtils.unpackExtensions(unpacker);
            String computeExecutorId = (String) clientHandshakeExtensions.get(HandshakeExtension.COMPUTE_EXECUTOR_ID);

            if (computeExecutorId != null) {
                CompletableFuture<PlatformComputeConnection> computeConnFut = computeConnectionFunc.apply(computeExecutorId);

                if (computeConnFut == null) {
                    LOG.warn("Invalid compute executor ID, client connection rejected [connectionId=" + connectionId
                            + ", remoteAddress=" + ctx.channel().remoteAddress() + "]: " + computeExecutorId);

                    ctx.close();
                    return;
                }

                // Bypass authentication for compute executor connections.
                handshakeSuccess(ctx, packer, UserDetails.UNKNOWN, clientFeatures, clientVer, clientCode);

                // Ready to handle compute requests now.
                computeConnFut.complete(this::executeJobAsync);

                return;
            }

            authenticationManager
                    .authenticateAsync(createAuthenticationRequest(clientHandshakeExtensions))
                    .handleAsync((user, err) -> {
                        if (err != null) {
                            handshakeError(ctx, packer, err);
                        } else {
                            handshakeSuccess(ctx, packer, user, clientFeatures, clientVer, clientCode);
                        }

                        return null;
                    }, ctx.executor());
        } catch (Throwable t) {
            handshakeError(ctx, packer, t);
        } finally {
            unpacker.close();
        }
    }

    private void handshakeSuccess(
            ChannelHandlerContext ctx,
            ClientMessagePacker packer,
            UserDetails user,
            BitSet clientFeatures,
            ProtocolVersion clientVer,
            int clientCode) {
        BitSet mutuallySupportedFeatures = HandshakeUtils.supportedFeatures(features, clientFeatures);

        clientContext = new ClientContext(clientVer, clientCode, mutuallySupportedFeatures, user);

        sendHandshakeResponse(ctx, packer);
    }

    private void handshakeError(ChannelHandlerContext ctx, ClientMessagePacker packer, Throwable t) {
        LOG.warn("Handshake failed [connectionId=" + connectionId + ", remoteAddress=" + ctx.channel().remoteAddress() + "]: "
                + t.getMessage(), t);

        packer.close();

        var errPacker = getPacker(ctx.alloc());

        try {
            ProtocolVersion.LATEST_VER.pack(errPacker);

            writeErrorCore(t, errPacker);

            write(errPacker, ctx);
        } catch (Throwable t2) {
            LOG.warn("Handshake failed [connectionId=" + connectionId + ", remoteAddress=" + ctx.channel().remoteAddress() + "]: "
                    + t2.getMessage(), t2);

            errPacker.close();
            exceptionCaught(ctx, t2);
        }

        metrics.sessionsRejectedIncrement();
    }

    private void sendHandshakeResponse(ChannelHandlerContext ctx, ClientMessagePacker packer) {
        ProtocolVersion.LATEST_VER.pack(packer);
        packer.packNil(); // No error.

        packer.packLong(configuration.idleTimeoutMillis());

        ClusterNode localMember = clusterService.topologyService().localMember();
        packer.packUuid(localMember.id());
        packer.packString(localMember.name());

        ClusterInfo clusterInfo = clusterInfoSupplier.get();

        // Cluster ID history, from the oldest to the newest (cluster ID can change during CMG/MG repair).
        packer.packInt(clusterInfo.idHistory().size());
        for (UUID clusterId : clusterInfo.idHistory()) {
            packer.packUuid(clusterId);
        }

        // Cluster name never changes.
        packer.packString(clusterInfo.name());

        packer.packLong(observableTimestamp(null));

        // Pack current version
        packer.packByte(IgniteProductVersion.CURRENT_VERSION.major());
        packer.packByte(IgniteProductVersion.CURRENT_VERSION.minor());
        packer.packByte(IgniteProductVersion.CURRENT_VERSION.maintenance());
        packer.packByteNullable(IgniteProductVersion.CURRENT_VERSION.patch());
        packer.packStringNullable(IgniteProductVersion.CURRENT_VERSION.preRelease());

        HandshakeUtils.packFeatures(packer, features);
        HandshakeUtils.packExtensions(packer, extensions);

        write(packer, ctx);

        state = STATE_HANDSHAKE_RESPONSE_SENT;

        metrics.sessionsAcceptedIncrement();
        metrics.sessionsActiveIncrement();

        ctx.channel().closeFuture().addListener(f -> metrics.sessionsActiveDecrement());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Handshake [connectionId=" + connectionId + ", remoteAddress=" + ctx.channel().remoteAddress() + "]: "
                    + clientContext);
        }
    }

    private static AuthenticationRequest<?, ?> createAuthenticationRequest(Map<HandshakeExtension, Object> extensions) {
        Object authnType = extensions.get(HandshakeExtension.AUTHENTICATION_TYPE);

        if (authnType == null) {
            return new AnonymousRequest();
        }

        if (authnType instanceof String && AuthenticationType.BASIC.name().equalsIgnoreCase((String) authnType)) {
            return new UsernamePasswordRequest(
                    (String) extensions.get(HandshakeExtension.AUTHENTICATION_IDENTITY),
                    (String) extensions.get(HandshakeExtension.AUTHENTICATION_SECRET));
        }

        throw new UnsupportedAuthenticationTypeException("Unsupported authentication type: " + authnType);
    }

    private void writeMagic(ChannelHandlerContext ctx) {
        ctx.write(Unpooled.wrappedBuffer(ClientMessageCommon.MAGIC_BYTES));
        metrics.bytesSentAdd(ClientMessageCommon.MAGIC_BYTES.length);
    }

    private void write(ClientMessagePacker packer, ChannelHandlerContext ctx) {
        var buf = packer.getBuffer();
        int bytes = buf.readableBytes();

        try {
            // writeAndFlush releases pooled buffer.
            ctx.writeAndFlush(buf);
        } catch (Throwable t) {
            buf.release();
            throw t;
        }

        metrics.bytesSentAdd(bytes);
    }

    private void writeResponseHeader(
            ClientMessagePacker packer, long requestId, ChannelHandlerContext ctx, boolean isNotification, boolean isError) {
        packer.packLong(requestId);
        writeFlags(packer, ctx, isNotification, isError);

        // Include server timestamp in error and notification responses as well:
        // an operation can modify data and then throw an exception (e.g. Compute task),
        // so we still need to update client-side timestamp to preserve causality guarantees.
        packer.packLong(observableTimestamp(null));
    }

    private void writeError(long requestId, int opCode, Throwable err, ChannelHandlerContext ctx, boolean isNotification) {
        if (isNotification) {
            LOG.warn("Error processing client notification [connectionId=" + connectionId + ", id=" + requestId
                    + ", remoteAddress=" + ctx.channel().remoteAddress() + "]:" + err.getMessage(), err);
        } else {
            LOG.warn("Error processing client request [connectionId=" + connectionId + ", id=" + requestId + ", op=" + opCode
                    + ", remoteAddress=" + ctx.channel().remoteAddress() + "]:" + err.getMessage(), err);
        }

        ClientMessagePacker packer = getPacker(ctx.alloc());

        try {
            assert err != null;

            writeResponseHeader(packer, requestId, ctx, isNotification, true);
            writeErrorCore(err, packer);

            write(packer, ctx);
        } catch (Throwable t) {
            packer.close();
            exceptionCaught(ctx, t);
        }
    }

    private void writeErrorCore(Throwable err, ClientMessagePacker packer) {
        SchemaVersionMismatchException schemaVersionMismatchException = findException(err, SchemaVersionMismatchException.class);
        SqlBatchException sqlBatchException = findException(err, SqlBatchException.class);

        err = firstNotNull(
                schemaVersionMismatchException,
                sqlBatchException,
                ExceptionUtils.unwrapCause(err)
        );

        // Trace ID and error code.
        if (err instanceof TraceableException) {
            TraceableException iex = (TraceableException) err;
            packer.packUuid(iex.traceId());
            packer.packInt(iex.code());
        } else {
            packer.packUuid(UUID.randomUUID());
            packer.packInt(INTERNAL_ERR);
        }

        // No need to send internal errors to client.
        assert err != null;
        Throwable pubErr = IgniteExceptionMapperUtil.mapToPublicException(err);

        // Class name and message.
        packer.packString(pubErr.getClass().getName());
        packer.packString(pubErr.getMessage());

        // Stack trace.
        if (configuration.sendServerExceptionStackTraceToClient()) {
            packer.packString(ExceptionUtils.getFullStackTrace(pubErr));
        } else {
            packer.packString("To see the full stack trace set clientConnector.sendServerExceptionStackTraceToClient:true");
        }

        // Extensions.
        if (schemaVersionMismatchException != null) {
            packer.packInt(1); // 1 extension.
            packer.packString(ErrorExtensions.EXPECTED_SCHEMA_VERSION);
            packer.packInt(schemaVersionMismatchException.expectedVersion());
        } else if (sqlBatchException != null) {
            packer.packInt(1); // 1 extension.
            packer.packString(ErrorExtensions.SQL_UPDATE_COUNTERS);
            packer.packLongArray(sqlBatchException.updateCounters());
        } else {
            packer.packNil(); // No extensions.
        }
    }

    private static ClientMessagePacker getPacker(ByteBufAllocator alloc) {
        // Outgoing messages are released on write.
        return new ClientMessagePacker(alloc.buffer());
    }

    private void processOperation(ChannelHandlerContext ctx, ClientMessageUnpacker in) {
        long requestId = -1;
        int opCode = -1;
        ClientMessagePacker out = null;

        metrics.requestsActiveIncrement();

        try {
            opCode = in.unpackInt();
            requestId = in.unpackLong();

            if (LOG.isTraceEnabled()) {
                LOG.trace("Client request started [id=" + requestId + ", op=" + opCode
                        + ", remoteAddress=" + ctx.channel().remoteAddress() + "]");
            }

            if (opCode == ClientOp.SERVER_OP_RESPONSE) {
                processServerOpResponse(requestId, in);
                return;
            }

            if (isPartitionOperation(opCode)) {
                long requestId0 = requestId;
                int opCode0 = opCode;

                partitionOperationsExecutor.execute(() -> {
                    // Packer buffer is released by Netty on send, or by inner exception handlers below.
                    var outPacker = getPacker(ctx.alloc());

                    try {
                        processOperationInternal(ctx, in, outPacker, requestId0, opCode0);
                    } catch (Throwable t) {
                        in.close();
                        outPacker.close();

                        writeError(requestId0, opCode0, t, ctx, false);

                        metrics.requestsFailedIncrement();
                    }
                });
            } else {
                // Packer buffer is released by Netty on send, or by inner exception handlers below.
                out = getPacker(ctx.alloc());

                processOperationInternal(ctx, in, out, requestId, opCode);
            }
        } catch (Throwable t) {
            in.close();

            if (out != null) {
                out.close();
            }

            writeError(requestId, opCode, t, ctx, false);

            metrics.requestsFailedIncrement();
        }
    }

    private @Nullable CompletableFuture processOperation(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            int opCode,
            long requestId
    ) throws IgniteInternalCheckedException {
        switch (opCode) {
            case ClientOp.HEARTBEAT:
                return null;

            case ClientOp.TABLES_GET:
                return ClientTablesGetRequest.process(out, igniteTables).thenRun(() -> out.meta(clockService.current()));

            case ClientOp.SCHEMAS_GET:
                return ClientSchemasGetRequest.process(in, out, igniteTables, schemaVersions);

            case ClientOp.TABLE_GET:
                return ClientTableGetRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_UPSERT:
                return ClientTupleUpsertRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_GET:
                return ClientTupleGetRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_UPSERT_ALL:
                return ClientTupleUpsertAllRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_GET_ALL:
                return ClientTupleGetAllRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_GET_AND_UPSERT:
                return ClientTupleGetAndUpsertRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_INSERT:
                return ClientTupleInsertRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_INSERT_ALL:
                return ClientTupleInsertAllRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_REPLACE:
                return ClientTupleReplaceRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_REPLACE_EXACT:
                return ClientTupleReplaceExactRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_GET_AND_REPLACE:
                return ClientTupleGetAndReplaceRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_DELETE:
                return ClientTupleDeleteRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_DELETE_ALL:
                return ClientTupleDeleteAllRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_DELETE_EXACT:
                return ClientTupleDeleteExactRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_DELETE_ALL_EXACT:
                return ClientTupleDeleteAllExactRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_GET_AND_DELETE:
                return ClientTupleGetAndDeleteRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_CONTAINS_KEY:
                return ClientTupleContainsKeyRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.TUPLE_CONTAINS_ALL_KEYS:
                return ClientTupleContainsAllKeysRequest.process(in, out, igniteTables, resources, txManager, clockService);

            case ClientOp.JDBC_CONNECT:
                return ClientJdbcConnectRequest.execute(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_EXEC:
                return ClientJdbcExecuteRequest.execute(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_CANCEL:
                return ClientJdbcCancelRequest.execute(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_EXEC_BATCH:
                return ClientJdbcExecuteBatchRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_SQL_EXEC_PS_BATCH:
                return ClientJdbcPreparedStmntBatchRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_NEXT:
                return ClientJdbcFetchRequest.process(in, out, jdbcQueryCursorHandler);

            case ClientOp.JDBC_MORE_RESULTS:
                return ClientJdbcHasMoreRequest.process(in, out, jdbcQueryCursorHandler);

            case ClientOp.JDBC_CURSOR_CLOSE:
                return ClientJdbcCloseRequest.process(in, out, jdbcQueryCursorHandler);

            case ClientOp.JDBC_TABLE_META:
                return ClientJdbcTableMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_COLUMN_META:
                return ClientJdbcColumnMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_SCHEMAS_META:
                return ClientJdbcSchemasMetadataRequest.process(in, out, jdbcQueryEventHandler).thenRun(() -> {
                    out.meta(clockService.current());
                });

            case ClientOp.JDBC_PK_META:
                return ClientJdbcPrimaryKeyMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.TX_BEGIN:
                return ClientTransactionBeginRequest.process(in, out, txManager, resources, metrics, igniteTables,
                        clientContext.hasFeature(TX_DIRECT_MAPPING));

            case ClientOp.TX_COMMIT:
                return ClientTransactionCommitRequest.process(in, out, resources, metrics, clockService, igniteTables,
                        clientContext.hasFeature(TX_DIRECT_MAPPING));

            case ClientOp.TX_ROLLBACK:
                return ClientTransactionRollbackRequest.process(in, resources, metrics, igniteTables,
                        clientContext.hasFeature(TX_DIRECT_MAPPING));

            case ClientOp.COMPUTE_EXECUTE:
                return ClientComputeExecuteRequest.process(in, out, compute, clusterService, notificationSender(requestId),
                        clientContext.hasFeature(PLATFORM_COMPUTE_JOB));

            case ClientOp.COMPUTE_EXECUTE_COLOCATED:
                return ClientComputeExecuteColocatedRequest.process(
                        in,
                        out,
                        compute,
                        igniteTables,
                        clusterService,
                        notificationSender(requestId),
                        clientContext.hasFeature(PLATFORM_COMPUTE_JOB)
                );

            case ClientOp.COMPUTE_EXECUTE_PARTITIONED:
                return ClientComputeExecutePartitionedRequest.process(
                        in,
                        out,
                        compute,
                        igniteTables,
                        clusterService,
                        notificationSender(requestId),
                        clientContext.hasFeature(PLATFORM_COMPUTE_JOB)
                );

            case ClientOp.COMPUTE_EXECUTE_MAPREDUCE:
                return ClientComputeExecuteMapReduceRequest.process(in, out, compute, notificationSender(requestId));

            case ClientOp.COMPUTE_GET_STATE:
                return ClientComputeGetStateRequest.process(in, out, compute);

            case ClientOp.COMPUTE_CANCEL:
                return ClientComputeCancelRequest.process(in, out, compute);

            case ClientOp.COMPUTE_CHANGE_PRIORITY:
                return ClientComputeChangePriorityRequest.process(in, out, compute);

            case ClientOp.CLUSTER_GET_NODES:
                return ClientClusterGetNodesRequest.process(out, clusterService);

            case ClientOp.SQL_EXEC:
                return ClientSqlExecuteRequest.process(
                        partitionOperationsExecutor, in, out, requestId, cancelHandles, queryProcessor, resources, metrics
                );

            case ClientOp.SQL_CANCEL_EXEC:
                return ClientSqlCancelRequest.process(in, out, cancelHandles);

            case ClientOp.SQL_CURSOR_NEXT_PAGE:
                return ClientSqlCursorNextPageRequest.process(in, out, resources);

            case ClientOp.SQL_CURSOR_CLOSE:
                return ClientSqlCursorCloseRequest.process(in, out, resources);

            case ClientOp.PARTITION_ASSIGNMENT_GET:
                return ClientTablePartitionPrimaryReplicasGetRequest.process(in, out, primaryReplicaTracker);

            case ClientOp.JDBC_TX_FINISH:
                return ClientJdbcFinishTxRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_EXEC_SCRIPT:
                return ClientSqlExecuteScriptRequest.process(
                        partitionOperationsExecutor, in, out, queryProcessor, requestId, cancelHandles
                );

            case ClientOp.SQL_QUERY_META:
                return ClientSqlQueryMetadataRequest.process(
                        partitionOperationsExecutor, in, out, queryProcessor, resources
                );

            case ClientOp.SQL_EXEC_BATCH:
                return ClientSqlExecuteBatchRequest.process(
                        partitionOperationsExecutor, in, out, queryProcessor, resources
                );

            case ClientOp.STREAMER_BATCH_SEND:
                return ClientStreamerBatchSendRequest.process(in, out, igniteTables);

            case ClientOp.PRIMARY_REPLICAS_GET:
                return ClientTablePartitionPrimaryReplicasNodesGetRequest.process(in, out, igniteTables);

            case ClientOp.STREAMER_WITH_RECEIVER_BATCH_SEND:
                return ClientStreamerWithReceiverBatchSendRequest.process(in, out, igniteTables);

            case ClientOp.TABLES_GET_QUALIFIED:
                return ClientTablesGetQualifiedRequest.process(out, igniteTables).thenRun(() -> {
                    out.meta(clockService.current());
                });

            case ClientOp.TABLE_GET_QUALIFIED:
                return ClientTableGetQualifiedRequest.process(in, out, igniteTables);

            default:
                throw new IgniteException(PROTOCOL_ERR, "Unexpected operation code: " + opCode);
        }
    }

    private static boolean isPartitionOperation(int opCode) {
        return opCode == ClientOp.TABLES_GET
                || opCode == ClientOp.TUPLE_UPSERT
                || opCode == ClientOp.TUPLE_GET
                || opCode == ClientOp.TUPLE_GET_AND_UPSERT
                || opCode == ClientOp.TUPLE_INSERT
                || opCode == ClientOp.TUPLE_REPLACE
                || opCode == ClientOp.TUPLE_REPLACE_EXACT
                || opCode == ClientOp.TUPLE_GET_AND_REPLACE
                || opCode == ClientOp.TUPLE_DELETE
                || opCode == ClientOp.TUPLE_DELETE_EXACT
                || opCode == ClientOp.TUPLE_GET_AND_DELETE
                || opCode == ClientOp.TUPLE_CONTAINS_KEY
                || opCode == ClientOp.STREAMER_BATCH_SEND;

                // Sql-related operation must do some bookkeeping first on the client's thread to avoid races
                // (for instance, cancellation must not be processed until execution request is registered).
                // || opCode == ClientOp.SQL_EXEC
                // || opCode == ClientOp.SQL_EXEC_BATCH
                // || opCode == ClientOp.SQL_EXEC_SCRIPT
                // || opCode == ClientOp.SQL_QUERY_META;

                // TODO: IGNITE-23641 The batch operations were excluded because fast switching leads to performance degradation for them.
                // || opCode == ClientOp.TUPLE_UPSERT_ALL
                // || opCode == ClientOp.TUPLE_GET_ALL
                // || opCode == ClientOp.TUPLE_INSERT_ALL
                // || opCode == ClientOp.TUPLE_DELETE_ALL
                // || opCode == ClientOp.TUPLE_DELETE_ALL_EXACT
                // || opCode == ClientOp.TUPLE_CONTAINS_ALL_KEYS;
    }

    private void processOperationInternal(
            ChannelHandlerContext ctx,
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            long requestId,
            int opCode
    ) {
        out.packLong(requestId);
        writeFlags(out, ctx, false, false);

        // Observable timestamp should be calculated after the operation is processed; reserve space, write later.
        int observableTimestampIdx = out.reserveLong();

        CompletableFuture fut;

        try {
            fut = processOperation(in, out, opCode, requestId);
        } catch (IgniteInternalCheckedException e) {
            fut = CompletableFuture.failedFuture(e);
        }

        if (fut == null) {
            // Operation completed synchronously.
            in.close();
            out.setLong(observableTimestampIdx, observableTimestamp(out));
            write(out, ctx);

            if (LOG.isTraceEnabled()) {
                LOG.trace("Client request processed synchronously [id=" + requestId + ", op=" + opCode
                        + ", remoteAddress=" + ctx.channel().remoteAddress() + "]");
            }

            metrics.requestsProcessedIncrement();
            metrics.requestsActiveDecrement();
        } else {
            var reqId = requestId;
            var op = opCode;

            fut.whenComplete((Object res, Object err) -> {
                in.close();
                metrics.requestsActiveDecrement();

                if (err != null) {
                    out.close();
                    writeError(reqId, op, (Throwable) err, ctx, false);

                    metrics.requestsFailedIncrement();
                } else {
                    out.setLong(observableTimestampIdx, observableTimestamp(out));
                    write(out, ctx);

                    metrics.requestsProcessedIncrement();

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Client request processed [id=" + reqId + ", op=" + op
                                + ", remoteAddress=" + ctx.channel().remoteAddress() + "]");
                    }
                }
            });
        }
    }

    private void writeFlags(ClientMessagePacker out, ChannelHandlerContext ctx, boolean isNotification, boolean isError) {
        // Notify the client about primary replica change that happened for ANY table since the last request.
        // We can't assume that the client only uses uses a particular table (e.g. the one present in the replica tracker), because
        // the client can be connected to multiple nodes.
        long lastSentMaxStartTime = primaryReplicaMaxStartTime.get();
        long currentMaxStartTime = primaryReplicaTracker.maxStartTime();
        boolean primaryReplicasUpdated = currentMaxStartTime > lastSentMaxStartTime
                && primaryReplicaMaxStartTime.compareAndSet(lastSentMaxStartTime, currentMaxStartTime);

        if (primaryReplicasUpdated && LOG.isInfoEnabled()) {
            LOG.info("Partition primary replica changed, notifying client [connectionId=" + connectionId + ", remoteAddress="
                    + ctx.channel().remoteAddress() + ']');
        }

        int flags = ResponseFlags.getFlags(primaryReplicasUpdated, isNotification, isError, false);
        out.packInt(flags);

        if (primaryReplicasUpdated) {
            out.packLong(currentMaxStartTime);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof SSLException || cause.getCause() instanceof SSLException) {
            metrics.sessionsRejectedTlsIncrement();
        }

        if (cause instanceof DecoderException && cause.getCause() instanceof IgniteException) {
            var err = (IgniteException) cause.getCause();

            if (err.code() == HANDSHAKE_HEADER_ERR) {
                metrics.sessionsRejectedIncrement();
            }
        }

        LOG.warn("Exception in client connector pipeline [connectionId=" + connectionId + ", remoteAddress="
                + ctx.channel().remoteAddress() + "]: " + cause.getMessage(), cause);

        ctx.close();
    }

    private static <T> @Nullable T findException(Throwable e, Class<T> cls) {
        while (e != null) {
            if (cls.isInstance(e)) {
                return (T) e;
            }

            e = e.getCause();
        }

        return null;
    }

    /**
     * Gets an observation timestamp for the operation being processed or {@link HybridTimestamp#MIN_VALUE} if the timestamp was not defined
     * by the operation.
     * The method returns a current timestamp for the handshake operation.
     *
     * @param out Output message packer.
     * @return A long representation of the observation timestamp.
     */
    private long observableTimestamp(@Nullable ClientMessagePacker out) {
        // Handshake has to synchronize the observation timestamp with the server node.
        if (out == null) {
            return clockService.currentLong();
        }

        if (out.meta() == null) {
            return HybridTimestamp.MIN_VALUE.longValue();
        }

        assert out.meta() instanceof HybridTimestamp : "Meta must contain a timestamp [metaCls=" + out.meta().getClass().getName() + ']';

        return ((HybridTimestamp) out.meta()).longValue();
    }

    private void sendNotification(long requestId, @Nullable Consumer<ClientMessagePacker> writer, @Nullable Throwable err) {
        if (err != null) {
            writeError(requestId, -1, err, channelHandlerContext, true);
            return;
        }

        var packer = getPacker(channelHandlerContext.alloc());

        try {
            writeResponseHeader(packer, requestId, channelHandlerContext, true, false);

            if (writer != null) {
                writer.accept(packer);
            }

            write(packer, channelHandlerContext);
        } catch (Throwable t) {
            packer.close();
            exceptionCaught(channelHandlerContext, t);
        }
    }

    private NotificationSender notificationSender(long requestId) {
        // Notification can be sent before the response to the current request.
        // This is fine, because the client registers a listener before sending the request.
        return (writer, err) -> sendNotification(requestId, writer, err);
    }

    @Override
    public CompletableFuture<Boolean> notify(AuthenticationEventParameters parameters) {
        var channelCtx = channelHandlerContext;

        if (channelCtx == null) {
            // Not connected yet.
            return falseCompletedFuture();
        }

        // Use Netty executor (single thread) to process the event sequentially with network operations - no need to synchronize.
        channelCtx.executor().submit(() -> {
            if (shouldCloseConnection(parameters)) {
                LOG.warn("Closing connection due to authentication event [connectionId=" + connectionId + ", remoteAddress="
                        + channelHandlerContext.channel().remoteAddress() + ", event=" + parameters.type() + ']');

                closeConnection();
            }
        });

        // No need to wait for the event processing to complete, return false to continue listening.
        return falseCompletedFuture();
    }

    private boolean shouldCloseConnection(AuthenticationEventParameters parameters) {
        switch (parameters.type()) {
            case AUTHENTICATION_ENABLED:
                return true;
            case AUTHENTICATION_PROVIDER_REMOVED:
            case AUTHENTICATION_PROVIDER_UPDATED:
                return currentUserAffected((AuthenticationProviderEventParameters) parameters);
            case USER_REMOVED:
            case USER_UPDATED:
                return currentUserAffected((UserEventParameters) parameters);
            default:
                return false;
        }
    }

    private boolean currentUserAffected(AuthenticationProviderEventParameters parameters) {
        return clientContext != null && clientContext.userDetails().providerName().equals(parameters.name());
    }

    private boolean currentUserAffected(UserEventParameters parameters) {
        return clientContext != null
                && clientContext.userDetails().providerName().equals(parameters.providerName())
                && clientContext.userDetails().username().equals(parameters.username());
    }

    private void closeConnection() {
        ChannelHandlerContext ctx = channelHandlerContext;

        if (ctx != null) {
            ctx.close();
        }
    }

    private static Set<AuthenticationEvent> authenticationEventsToSubscribe() {
        return Set.of(
                AuthenticationEvent.AUTHENTICATION_ENABLED,
                AuthenticationEvent.AUTHENTICATION_PROVIDER_UPDATED,
                AuthenticationEvent.AUTHENTICATION_PROVIDER_REMOVED,
                AuthenticationEvent.USER_UPDATED,
                AuthenticationEvent.USER_REMOVED
        );
    }

    @TestOnly
    public ClientResourceRegistry resources() {
        return resources;
    }

    @TestOnly
    public int cancelHandlesCount() {
        return cancelHandles.size();
    }

    private CompletableFuture<ComputeJobDataHolder> executeJobAsync(
            List<String> deploymentUnitPaths,
            String jobClassName,
            ComputeJobDataHolder arg) {
        return sendServerToClientRequest(ServerOp.COMPUTE_JOB_EXEC,
                packer -> {
                    packer.packString(jobClassName);

                    packer.packInt(deploymentUnitPaths.size());
                    for (String path : deploymentUnitPaths) {
                        packer.packString(path);
                    }

                    ClientComputeJobPacker.packJobArgument(arg, null, packer);
                })
                .thenApply(ClientComputeJobUnpacker::unpackJobArgumentWithoutMarshaller);
    }

    private CompletableFuture<ClientMessageUnpacker> sendServerToClientRequest(int serverOp, Consumer<ClientMessagePacker> writer) {
        // Server and client request ids do not clash, but we use negative to simplify the debugging.
        var requestId = serverToClientRequestId.decrementAndGet();
        var packer = getPacker(channelHandlerContext.alloc());

        try {
            packer.packLong(requestId);
            int flags = ResponseFlags.getFlags(false, false, false, true);
            packer.packInt(flags);
            packer.packLong(observableTimestamp(null));
            packer.packInt(serverOp);

            writer.accept(packer);

            var fut = new CompletableFuture<ClientMessageUnpacker>();
            serverToClientRequests.put(requestId, fut);

            write(packer, channelHandlerContext);

            return fut;
        } catch (Throwable t) {
            packer.close();
            serverToClientRequests.remove(requestId);

            return CompletableFuture.failedFuture(t);
        }
    }

    private void processServerOpResponse(long requestId, ClientMessageUnpacker in) {
        try (in) {
            CompletableFuture<ClientMessageUnpacker> fut = serverToClientRequests.remove(requestId);

            if (fut == null) {
                LOG.warn("Received SERVER_OP_RESPONSE with unknown id [id=" + requestId
                        + ", connectionId=" + connectionId + ", remoteAddress=" + channelHandlerContext.channel().remoteAddress() + ']');
                return;
            }

            int flags = in.unpackInt();
            boolean error = ServerOpResponseFlags.getErrorFlag(flags);

            if (!error) {
                fut.complete(in);
            } else {
                Throwable err = readErrorFromClient(requestId, in);
                fut.completeExceptionally(err);
            }
        } catch (Throwable t) {
            LOG.warn("Unexpected error while processing SERVER_OP_RESPONSE [id=" + requestId
                    + ", connectionId=" + connectionId + ", remoteAddress=" + channelHandlerContext.channel().remoteAddress()
                    + ", message=" + t.getMessage() + ']', t);
        }
    }

    private Throwable readErrorFromClient(long requestId, ClientMessageUnpacker r) {
        UUID traceId = r.tryUnpackNil() ? null : r.unpackUuid();
        Integer code = r.tryUnpackNil() ? null : r.unpackInt();
        String className = r.unpackString();
        String message = r.unpackStringNullable();
        String stackTrace = r.unpackStringNullable();

        int extCount = r.unpackInt();
        for (int i = 0; i < extCount; i++) {
            String extName = r.unpackString();

            LOG.warn("Ignoring unknown error extension from client [id=" + requestId
                    + ", connectionId=" + connectionId + ", remoteAddress=" + channelHandlerContext.channel().remoteAddress()
                    + ", key=" + extName + ']');

            r.skipValues(1);
        }

        traceId = traceId == null ? UUID.randomUUID() : traceId;
        code = code == null ? INTERNAL_ERR : code;
        String messageExt = "Client-side error: " + message;

        // Nest details to mix platform-specific and Java-side stack traces.
        Throwable cause = new RuntimeException(className + ": " + message + System.lineSeparator() + stackTrace);

        return new IgniteException(traceId, code, messageExt, cause);
    }
}
