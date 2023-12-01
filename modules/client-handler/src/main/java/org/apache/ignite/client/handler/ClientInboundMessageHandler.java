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
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.net.ssl.SSLException;
import org.apache.ignite.client.handler.configuration.ClientConnectorView;
import org.apache.ignite.client.handler.requests.cluster.ClientClusterGetNodesRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteColocatedRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcCloseRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcColumnMetadataRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcConnectRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcExecuteBatchRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcExecuteRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcFetchRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcFinishTxRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcPreparedStmntBatchRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcPrimaryKeyMetadataRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcQueryMetadataRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcSchemasMetadataRequest;
import org.apache.ignite.client.handler.requests.jdbc.ClientJdbcTableMetadataRequest;
import org.apache.ignite.client.handler.requests.jdbc.JdbcMetadataCatalog;
import org.apache.ignite.client.handler.requests.sql.ClientSqlCursorCloseRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlCursorNextPageRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlExecuteRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlExecuteScriptRequest;
import org.apache.ignite.client.handler.requests.table.ClientSchemasGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTableGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTablePartitionPrimaryReplicasGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTablesGetRequest;
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
import org.apache.ignite.client.handler.requests.tx.ClientTransactionBeginRequest;
import org.apache.ignite.client.handler.requests.tx.ClientTransactionCommitRequest;
import org.apache.ignite.client.handler.requests.tx.ClientTransactionRollbackRequest;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.client.proto.ClientMessageCommon;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.ErrorExtensions;
import org.apache.ignite.internal.client.proto.HandshakeExtension;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.client.proto.ResponseFlags;
import org.apache.ignite.internal.client.proto.ServerMessageType;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryCursorHandler;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.SchemaVersionMismatchException;
import org.apache.ignite.internal.security.authentication.AnonymousRequest;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationRequest;
import org.apache.ignite.internal.security.authentication.UserDetails;
import org.apache.ignite.internal.security.authentication.UsernamePasswordRequest;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEvent;
import org.apache.ignite.internal.security.authentication.event.AuthenticationListener;
import org.apache.ignite.internal.security.authentication.event.AuthenticationProviderEvent;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersionsImpl;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.security.AuthenticationType;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;
import org.apache.ignite.sql.IgniteSql;
import org.jetbrains.annotations.Nullable;

/**
 * Handles messages from thin clients.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientInboundMessageHandler extends ChannelInboundHandlerAdapter implements AuthenticationListener {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ClientInboundMessageHandler.class);

    /** Ignite tables API. */
    private final IgniteTablesInternal igniteTables;

    /** Ignite transactions API. */
    private final IgniteTransactionsImpl igniteTransactions;

    /** JDBC Handler. */
    private final JdbcQueryEventHandler jdbcQueryEventHandler;

    /** Connection resources. */
    private final ClientResourceRegistry resources = new ClientResourceRegistry();

    /** Configuration. */
    private final ClientConnectorView configuration;

    /** Compute. */
    private final IgniteCompute compute;

    /** Cluster. */
    private final ClusterService clusterService;

    /** SQL. */
    private final IgniteSql sql;

    /** SQL query cursor handler. */
    private final JdbcQueryCursorHandler jdbcQueryCursorHandler;

    /** Cluster ID. */
    private final CompletableFuture<UUID> clusterId;

    /** Metrics. */
    private final ClientHandlerMetricSource metrics;

    /** Hybrid clock. */
    private final HybridClock clock;

    /** Context. */
    private ClientContext clientContext;

    /** Chanel handler context. */
    private ChannelHandlerContext channelHandlerContext;

    /** Primary replicas update counter. */
    private final AtomicLong primaryReplicaMaxStartTime;

    private final ClientPrimaryReplicaTracker primaryReplicaTracker;

    /** Authentication manager. */
    private final AuthenticationManager authenticationManager;

    private final SchemaVersions schemaVersions;

    private final long connectionId;

    /**
     * Constructor.
     *
     * @param igniteTables Ignite tables API entry point.
     * @param igniteTransactions Ignite transactions API.
     * @param processor Sql query processor.
     * @param configuration Configuration.
     * @param compute Compute.
     * @param clusterService Cluster.
     * @param sql SQL.
     * @param clusterId Cluster ID.
     * @param metrics Metrics.
     * @param authenticationManager Authentication manager.
     * @param clock Hybrid clock.
     */
    public ClientInboundMessageHandler(
            IgniteTablesInternal igniteTables,
            IgniteTransactionsImpl igniteTransactions,
            QueryProcessor processor,
            ClientConnectorView configuration,
            IgniteCompute compute,
            ClusterService clusterService,
            IgniteSql sql,
            CompletableFuture<UUID> clusterId,
            ClientHandlerMetricSource metrics,
            AuthenticationManager authenticationManager,
            HybridClock clock,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            long connectionId,
            ClientPrimaryReplicaTracker primaryReplicaTracker
    ) {
        assert igniteTables != null;
        assert igniteTransactions != null;
        assert processor != null;
        assert configuration != null;
        assert compute != null;
        assert clusterService != null;
        assert sql != null;
        assert clusterId != null;
        assert metrics != null;
        assert authenticationManager != null;
        assert clock != null;
        assert schemaSyncService != null;
        assert catalogService != null;
        assert primaryReplicaTracker != null;

        this.igniteTables = igniteTables;
        this.igniteTransactions = igniteTransactions;
        this.configuration = configuration;
        this.compute = compute;
        this.clusterService = clusterService;
        this.sql = sql;
        this.clusterId = clusterId;
        this.metrics = metrics;
        this.authenticationManager = authenticationManager;
        this.clock = clock;
        this.primaryReplicaTracker = primaryReplicaTracker;

        jdbcQueryCursorHandler = new JdbcQueryCursorHandlerImpl(resources);
        jdbcQueryEventHandler = new JdbcQueryEventHandlerImpl(
                processor,
                new JdbcMetadataCatalog(clock, schemaSyncService, catalogService),
                resources,
                igniteTransactions
        );

        schemaVersions = new SchemaVersionsImpl(schemaSyncService, catalogService, clock);
        this.connectionId = connectionId;

        this.primaryReplicaMaxStartTime = new AtomicLong(HybridTimestamp.MIN_VALUE.longValue());
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
        var unpacker = getUnpacker(byteBuf);
        metrics.bytesReceivedAdd(byteBuf.readableBytes() + ClientMessageCommon.HEADER_SIZE);

        // Packer buffer is released by Netty on send, or by inner exception handlers below.
        var packer = getPacker(ctx.alloc());

        if (clientContext == null) {
            metrics.bytesReceivedAdd(ClientMessageCommon.MAGIC_BYTES.length);
            handshake(ctx, unpacker, packer);
        } else {
            processOperation(ctx, unpacker, packer);
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

            var clientCode = unpacker.unpackInt();
            var featuresLen = unpacker.unpackBinaryHeader();
            var features = BitSet.valueOf(unpacker.readPayload(featuresLen));

            Map<HandshakeExtension, Object> extensions = extractExtensions(unpacker);
            AuthenticationRequest<?, ?> authenticationRequest = createAuthenticationRequest(extensions);
            UserDetails userDetails = authenticationManager.authenticate(authenticationRequest);

            clientContext = new ClientContext(clientVer, clientCode, features, userDetails);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Handshake [connectionId=" + connectionId + ", remoteAddress=" + ctx.channel().remoteAddress() + "]: "
                        + clientContext);
            }

            // Response.
            ProtocolVersion.LATEST_VER.pack(packer);
            packer.packNil(); // No error.

            packer.packLong(configuration.idleTimeout());

            ClusterNode localMember = clusterService.topologyService().localMember();
            packer.packString(localMember.id());
            packer.packString(localMember.name());
            packer.packUuid(clusterId.join());

            packer.packBinaryHeader(0); // Features.
            packer.packInt(0); // Extensions.

            write(packer, ctx);

            metrics.sessionsAcceptedIncrement();
            metrics.sessionsActiveIncrement();

            ctx.channel().closeFuture().addListener(f -> metrics.sessionsActiveDecrement());
        } catch (Throwable t) {
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
        } finally {
            unpacker.close();
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

    private void writeError(long requestId, int opCode, Throwable err, ChannelHandlerContext ctx) {
        LOG.warn("Error processing client request [connectionId=" + connectionId + ", id=" + requestId + ", op=" + opCode
                + ", remoteAddress=" + ctx.channel().remoteAddress() + "]:" + err.getMessage(), err);

        var packer = getPacker(ctx.alloc());

        try {
            assert err != null;

            packer.packInt(ServerMessageType.RESPONSE);
            packer.packLong(requestId);
            writeFlags(packer, ctx);

            // Include server timestamp in error response as well:
            // an operation can modify data and then throw an exception (e.g. Compute task),
            // so we still need to update client-side timestamp to preserve causality guarantees.
            packer.packLong(observableTimestamp(null));

            writeErrorCore(err, packer);

            write(packer, ctx);
        } catch (Throwable t) {
            packer.close();
            exceptionCaught(ctx, t);
        }
    }

    private void writeErrorCore(Throwable err, ClientMessagePacker packer) {
        SchemaVersionMismatchException schemaVersionMismatchException = schemaVersionMismatchException(err);
        err = schemaVersionMismatchException == null ? ExceptionUtils.unwrapCause(err) : schemaVersionMismatchException;

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
        Throwable pubErr = IgniteExceptionMapperUtil.mapToPublicException(ExceptionUtils.unwrapCause(err));

        // Class name and message.
        packer.packString(pubErr.getClass().getName());
        packer.packString(pubErr.getMessage());

        // Stack trace.
        if (configuration.sendServerExceptionStackTraceToClient()) {
            packer.packString(ExceptionUtils.getFullStackTrace(pubErr));
        } else {
            packer.packNil();
        }

        // Extensions.
        if (schemaVersionMismatchException != null) {
            packer.packInt(1);
            packer.packString(ErrorExtensions.EXPECTED_SCHEMA_VERSION);
            packer.packInt(schemaVersionMismatchException.expectedVersion());
        } else {
            packer.packNil(); // No extensions.
        }
    }

    private static ClientMessagePacker getPacker(ByteBufAllocator alloc) {
        // Outgoing messages are released on write.
        return new ClientMessagePacker(alloc.buffer());
    }

    private static ClientMessageUnpacker getUnpacker(ByteBuf buf) {
        return new ClientMessageUnpacker(buf);
    }

    private void processOperation(ChannelHandlerContext ctx, ClientMessageUnpacker in, ClientMessagePacker out) {
        long requestId = -1;
        int opCode = -1;
        metrics.requestsActiveIncrement();

        try {
            opCode = in.unpackInt();
            requestId = in.unpackLong();

            if (LOG.isTraceEnabled()) {
                LOG.trace("Client request started [id=" + requestId + ", op=" + opCode
                        + ", remoteAddress=" + ctx.channel().remoteAddress() + "]");
            }

            out.packInt(ServerMessageType.RESPONSE);
            out.packLong(requestId);
            writeFlags(out, ctx);

            // Observable timestamp should be calculated after the operation is processed; reserve space, write later.
            int observableTimestampIdx = out.reserveLong();
            out.packNil(); // No error.

            var fut = processOperation(in, out, opCode, requestId);

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
                        writeError(reqId, op, (Throwable) err, ctx);

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
        } catch (Throwable t) {
            in.close();
            out.close();

            writeError(requestId, opCode, t, ctx);

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
                return ClientTablesGetRequest.process(out, igniteTables);

            case ClientOp.SCHEMAS_GET:
                return ClientSchemasGetRequest.process(in, out, igniteTables, schemaVersions);

            case ClientOp.TABLE_GET:
                return ClientTableGetRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_UPSERT:
                return ClientTupleUpsertRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_GET:
                return ClientTupleGetRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_UPSERT_ALL:
                return ClientTupleUpsertAllRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_GET_ALL:
                return ClientTupleGetAllRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_GET_AND_UPSERT:
                return ClientTupleGetAndUpsertRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_INSERT:
                return ClientTupleInsertRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_INSERT_ALL:
                return ClientTupleInsertAllRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_REPLACE:
                return ClientTupleReplaceRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_REPLACE_EXACT:
                return ClientTupleReplaceExactRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_GET_AND_REPLACE:
                return ClientTupleGetAndReplaceRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_DELETE:
                return ClientTupleDeleteRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_DELETE_ALL:
                return ClientTupleDeleteAllRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_DELETE_EXACT:
                return ClientTupleDeleteExactRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_DELETE_ALL_EXACT:
                return ClientTupleDeleteAllExactRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_GET_AND_DELETE:
                return ClientTupleGetAndDeleteRequest.process(in, out, igniteTables, resources);

            case ClientOp.TUPLE_CONTAINS_KEY:
                return ClientTupleContainsKeyRequest.process(in, out, igniteTables, resources);

            case ClientOp.JDBC_CONNECT:
                return ClientJdbcConnectRequest.execute(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_EXEC:
                return ClientJdbcExecuteRequest.execute(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_EXEC_BATCH:
                return ClientJdbcExecuteBatchRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_SQL_EXEC_PS_BATCH:
                return ClientJdbcPreparedStmntBatchRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_NEXT:
                return ClientJdbcFetchRequest.process(in, out, jdbcQueryCursorHandler);

            case ClientOp.JDBC_CURSOR_CLOSE:
                return ClientJdbcCloseRequest.process(in, out, jdbcQueryCursorHandler);

            case ClientOp.JDBC_TABLE_META:
                return ClientJdbcTableMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_COLUMN_META:
                return ClientJdbcColumnMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_SCHEMAS_META:
                return ClientJdbcSchemasMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_PK_META:
                return ClientJdbcPrimaryKeyMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.JDBC_QUERY_META:
                return ClientJdbcQueryMetadataRequest.process(in, out, jdbcQueryCursorHandler);

            case ClientOp.TX_BEGIN:
                return ClientTransactionBeginRequest.process(in, out, igniteTransactions, resources, metrics);

            case ClientOp.TX_COMMIT:
                return ClientTransactionCommitRequest.process(in, resources, metrics);

            case ClientOp.TX_ROLLBACK:
                return ClientTransactionRollbackRequest.process(in, resources, metrics);

            case ClientOp.COMPUTE_EXECUTE:
                return ClientComputeExecuteRequest.process(in, compute, clusterService, w -> sendNotification(requestId, w));

            case ClientOp.COMPUTE_EXECUTE_COLOCATED:
                return ClientComputeExecuteColocatedRequest.process(in, out, compute, igniteTables);

            case ClientOp.CLUSTER_GET_NODES:
                return ClientClusterGetNodesRequest.process(out, clusterService);

            case ClientOp.SQL_EXEC:
                return ClientSqlExecuteRequest.process(in, out, sql, resources, metrics, igniteTransactions);

            case ClientOp.SQL_CURSOR_NEXT_PAGE:
                return ClientSqlCursorNextPageRequest.process(in, out, resources, igniteTransactions);

            case ClientOp.SQL_CURSOR_CLOSE:
                return ClientSqlCursorCloseRequest.process(in, out, resources, igniteTransactions);

            case ClientOp.PARTITION_ASSIGNMENT_GET:
                return ClientTablePartitionPrimaryReplicasGetRequest.process(in, out, primaryReplicaTracker);

            case ClientOp.JDBC_TX_FINISH:
                return ClientJdbcFinishTxRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_EXEC_SCRIPT:
                return ClientSqlExecuteScriptRequest.process(in, sql, igniteTransactions);

            default:
                throw new IgniteException(PROTOCOL_ERR, "Unexpected operation code: " + opCode);
        }
    }

    private void writeFlags(ClientMessagePacker out, ChannelHandlerContext ctx) {
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

        int flags = ResponseFlags.getFlags(primaryReplicasUpdated);
        out.packInt(flags);

        if (primaryReplicasUpdated) {
            out.packLong(currentMaxStartTime);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
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

    private static Map<HandshakeExtension, Object> extractExtensions(ClientMessageUnpacker unpacker) {
        EnumMap<HandshakeExtension, Object> extensions = new EnumMap<>(HandshakeExtension.class);
        int mapSize = unpacker.unpackInt();
        for (int i = 0; i < mapSize; i++) {
            HandshakeExtension handshakeExtension = HandshakeExtension.fromKey(unpacker.unpackString());
            if (handshakeExtension != null) {
                extensions.put(handshakeExtension, unpackExtensionValue(handshakeExtension, unpacker));
            }
        }
        return extensions;
    }

    private static Object unpackExtensionValue(HandshakeExtension handshakeExtension, ClientMessageUnpacker unpacker) {
        Class<?> type = handshakeExtension.valueType();
        if (type == String.class) {
            return unpacker.unpackString();
        } else {
            throw new IllegalArgumentException("Unsupported extension type: " + type.getName());
        }
    }

    private static @Nullable SchemaVersionMismatchException schemaVersionMismatchException(Throwable e) {
        while (e != null) {
            if (e instanceof SchemaVersionMismatchException) {
                return (SchemaVersionMismatchException) e;
            }

            e = e.getCause();
        }

        return null;
    }

    private long observableTimestamp(@Nullable ClientMessagePacker out) {
        // Certain operations can override the timestamp and provide it in the meta object.
        if (out != null) {
            Object meta = out.meta();

            if (meta instanceof HybridTimestamp) {
                return ((HybridTimestamp) meta).longValue();
            }
        }

        return clock.now().longValue();
    }

    @Override
    public void onEvent(AuthenticationEvent event) {
        switch (event.type()) {
            case AUTHENTICATION_ENABLED:
                closeConnection();
                break;
            case AUTHENTICATION_PROVIDER_REMOVED:
            case AUTHENTICATION_PROVIDER_UPDATED:
                AuthenticationProviderEvent providerEvent = (AuthenticationProviderEvent) event;
                if (clientContext != null && clientContext.userDetails().providerName().equals(providerEvent.name())) {
                    closeConnection();
                }
                break;
            default:
                break;
        }
    }

    private void closeConnection() {
        if (channelHandlerContext != null) {
            channelHandlerContext.close();
        }
    }

    private void sendNotification(long requestId, Consumer<ClientMessagePacker> writer) {
        var packer = getPacker(channelHandlerContext.alloc());

        try {
            packer.packInt(ServerMessageType.NOTIFICATION);
            packer.packLong(requestId);
            writer.accept(packer);

            write(packer, channelHandlerContext);
        } catch (Throwable t) {
            packer.close();
            exceptionCaught(channelHandlerContext, t);
        }
    }
}
