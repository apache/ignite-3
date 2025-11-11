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

package org.apache.ignite.client.handler.requests.table;

import static org.apache.ignite.internal.client.proto.ClientMessageCommon.NO_VALUE;
import static org.apache.ignite.internal.client.proto.tx.ClientTxUtils.TX_ID_DIRECT;
import static org.apache.ignite.internal.client.proto.tx.ClientTxUtils.TX_ID_FIRST_DIRECT;
import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;
import static org.apache.ignite.lang.ErrorGroups.Client.TABLE_ID_NOT_FOUND_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR;

import java.util.BitSet;
import java.util.Collection;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.client.handler.requests.table.ClientTupleRequestBase.RequestOptions;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleContainer;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.InternalTxOptions;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxPriority;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleHelper;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Common table functionality.
 */
public class ClientTableCommon {
    /**
     * Writes a schema.
     *
     * @param packer Packer.
     * @param schemaVer Schema version.
     * @param schema Schema.
     */
    static void writeSchema(ClientMessagePacker packer, int schemaVer, SchemaDescriptor schema) {
        packer.packInt(schemaVer);

        if (schema == null) {
            packer.packNil();

            return;
        }

        var colCnt = schema.columns().size();
        packer.packInt(colCnt);

        for (var colIdx = 0; colIdx < colCnt; colIdx++) {
            var col = schema.column(colIdx);

            packer.packInt(7);
            packer.packString(col.name());
            packer.packInt(col.type().spec().id());
            packer.packInt(col.positionInKey());
            packer.packBoolean(col.nullable());
            packer.packInt(col.positionInColocation());
            packer.packInt(getDecimalScale(col.type()));
            packer.packInt(getPrecision(col.type()));
        }
    }

    static void writeTupleOrNil(ClientMessagePacker packer, Tuple tuple, TuplePart part, SchemaRegistry schemaRegistry) {
        if (tuple == null) {
            packer.packInt(schemaRegistry.lastKnownSchemaVersion());
            packer.packNil();

            return;
        }

        writeTuple(packer, tuple, false, part);
    }

    /**
     * Writes a tuple.
     *
     * @param packer Packer.
     * @param tuple Tuple.
     * @param skipHeader Whether to skip the tuple header.
     * @param part Which part of tuple to write.
     * @throws IgniteException on failed serialization.
     */
    private static void writeTuple(
            ClientMessagePacker packer,
            Tuple tuple,
            boolean skipHeader,
            TuplePart part
    ) {
        assert tuple != null;
        assert tuple instanceof SchemaAware : "Tuple must be a SchemaAware: " + tuple.getClass();
        assert part != TuplePart.VAL : "TuplePart.VAL is not supported";

        var schema = ((SchemaAware) tuple).schema();

        assert schema != null : "Schema must not be null: " + tuple.getClass();

        if (!skipHeader) {
            packer.packInt(schema.version());
        }

        assert tuple instanceof BinaryTupleContainer : "Tuple must be a BinaryTupleContainer: " + tuple.getClass();
        BinaryTupleReader binaryTuple = ((BinaryTupleContainer) tuple).binaryTuple();

        int elementCount = part == TuplePart.KEY ? schema.keyColumns().size() : schema.length();

        if (binaryTuple != null) {
            assert elementCount == binaryTuple.elementCount() :
                    "Tuple element count mismatch: " + elementCount + " != " + binaryTuple.elementCount() + " (" + tuple.getClass() + ")";

            packer.packBinaryTuple(binaryTuple);
        } else {
            // Underlying binary tuple is not available or can't be used as is, pack columns one by one.
            var builder = new BinaryTupleBuilder(elementCount);

            for (var i = 0; i < elementCount; i++) {
                var col = schema.column(i);
                Object v = TupleHelper.valueOrDefault(tuple, col.name(), NO_VALUE);

                ClientBinaryTupleUtils.appendValue(builder, col.type().spec(), col.name(), getDecimalScale(col.type()), v);
            }

            packer.packBinaryTuple(builder);
        }
    }

    static void writeTuples(
            ClientMessagePacker packer,
            Collection<Tuple> tuples,
            SchemaRegistry schemaRegistry) {
        writeTuples(packer, tuples, TuplePart.KEY_AND_VAL, schemaRegistry);
    }

    static void writeTuples(
            ClientMessagePacker packer,
            Collection<Tuple> tuples,
            TuplePart part,
            SchemaRegistry schemaRegistry
    ) {
        if (tuples == null || tuples.isEmpty()) {
            packer.packInt(schemaRegistry.lastKnownSchemaVersion());
            packer.packInt(0);

            return;
        }

        Integer schemaVer = null;

        for (Tuple tuple : tuples) {
            assert tuple != null;

            var tupleSchemaVer = ((SchemaAware) tuple).schema().version();

            if (schemaVer == null) {
                schemaVer = tupleSchemaVer;
                packer.packInt(tupleSchemaVer);
                packer.packInt(tuples.size());
            } else {
                assert schemaVer.equals(tupleSchemaVer) : "All tuples must have the same schema version";
            }

            writeTuple(packer, tuple, true, part);
        }
    }

    static void writeTuplesNullable(
            ClientMessagePacker packer,
            Collection<Tuple> tuples,
            TuplePart part,
            SchemaRegistry schemaRegistry
    ) {
        if (tuples == null || tuples.isEmpty()) {
            packer.packInt(schemaRegistry.lastKnownSchemaVersion());
            packer.packInt(0);

            return;
        }

        Integer schemaVer = null;

        for (Tuple tuple : tuples) {
            if (tuple != null) {
                schemaVer = ((SchemaAware) tuple).schema().version();
                break;
            }
        }

        packer.packInt(schemaVer == null ? schemaRegistry.lastKnownSchemaVersion() : schemaVer);
        packer.packInt(tuples.size());

        for (Tuple tuple : tuples) {
            if (tuple == null) {
                packer.packBoolean(false);
                continue;
            }

            assert schemaVer.equals(((SchemaAware) tuple).schema().version()) : "All tuples must have the same schema version";

            packer.packBoolean(true);
            writeTuple(packer, tuple, true, part);
        }
    }

    public static CompletableFuture<Tuple> readTuple(
            int schemaId, BitSet noValueSet, byte[] tupleBytes, TableViewInternal table, boolean keyOnly) {
        return readSchema(schemaId, table).thenApply(schema -> readTuple(noValueSet, tupleBytes, keyOnly, schema));
    }

    /**
     * Reads a tuple.
     *
     * @param noValueSet No value set.
     * @param tupleBytes Tuple bytes.
     * @param keyOnly Key only flag.
     * @param schema Schema.
     * @return Tuple.
     */
    public static Tuple readTuple(
            BitSet noValueSet,
            byte[] tupleBytes,
            boolean keyOnly,
            SchemaDescriptor schema
    ) {
        var cnt = keyOnly ? schema.keyColumns().size() : schema.length();

        // NOTE: noValueSet is only present for client -> server communication.
        // It helps disambiguate two cases: 1 - column value is not set, 2 - column value is set to null explicitly.
        // If the column has a default value, it should be applied only in case 1.
        // https://cwiki.apache.org/confluence/display/IGNITE/IEP-76+Thin+Client+Protocol+for+Ignite+3.0#IEP76ThinClientProtocolforIgnite3.0-NullvsNoValue
        var binaryTupleReader = new BinaryTupleReader(cnt, tupleBytes);

        return new ClientHandlerTuple(schema, noValueSet, binaryTupleReader, keyOnly);
    }

    static CompletableFuture<SchemaDescriptor> readSchema(int schemaId, TableViewInternal table) {
        // Use schemaAsync() as the schema version is coming from outside and we have no guarantees that this version is ready.
        return table.schemaView().schemaAsync(schemaId);
    }

    /**
     * Reads a table.
     *
     * @param tableId Table id.
     * @param tables Ignite tables.
     * @return Table.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *         <ul>
     *             <li>the node is stopping.</li>
     *         </ul>
     */
    public static CompletableFuture<TableViewInternal> readTableAsync(int tableId, IgniteTables tables) {
        try {
            IgniteTablesInternal tablesInternal = (IgniteTablesInternal) tables;

            // Fast path - in most cases, the table is already in the startedTables cache.
            // This method can return a table that is being stopped, but it's not a problem - any operation on such table will fail.
            TableViewInternal cachedTable = tablesInternal.cachedTable(tableId);
            if (cachedTable != null) {
                return CompletableFuture.completedFuture(cachedTable);
            }

            return tablesInternal.tableAsync(tableId)
                    .thenApply(t -> {
                        if (t == null) {
                            throw tableIdNotFoundException(tableId);
                        }

                        return t;
                    });
        } catch (NodeStoppingException e) {
            throw new IgniteException(e.traceId(), e.code(), e.getMessage(), e);
        }
    }

    /**
     * Write tx metadata.
     *
     * @param out Packer.
     * @param tsTracker Timestamp tracker.
     * @param clockService Clock service.
     * @param req Request.
     */
    static void writeTxMeta(
            ClientMessagePacker out, HybridTimestampTracker tsTracker, @Nullable ClockService clockService, ClientTupleRequestBase req) {
        writeTxMeta(out, tsTracker, clockService, req.tx(), req.resourceId());
    }

    /**
     * Write tx metadata.
     *
     * @param out Packer.
     * @param tsTracker Timestamp tracker.
     * @param clockService Clock service.
     * @param req Request.
     */
    static void writeTxMeta(
            ClientMessagePacker out, HybridTimestampTracker tsTracker, @Nullable ClockService clockService, ClientTuplesRequestBase req) {
        writeTxMeta(out, tsTracker, clockService, req.tx(), req.resourceId());
    }

    /**
     * Write tx metadata.
     *
     * @param out Packer.
     * @param tsTracker Timestamp tracker.
     * @param clockService Clock service.
     * @param tx Transaction.
     * @param resourceId Resource id.
     */
    public static void writeTxMeta(ClientMessagePacker out, HybridTimestampTracker tsTracker, @Nullable ClockService clockService,
            InternalTransaction tx, long resourceId) {
        if (resourceId != 0) {
            // Resource id is assigned on a first request in direct mode.
            out.packLong(resourceId);
            out.packUuid(tx.id());
            out.packUuid(tx.coordinatorId());
            out.packLong(tx.getTimeout());
        } else if (tx.remote()) {
            PendingTxPartitionEnlistment token = tx.enlistedPartition(null);
            out.packString(token.primaryNodeConsistentId());
            out.packLong(token.consistencyToken());
            out.packBoolean(TxState.ABORTED == tx.state()); // No-op enlistment.

            if (clockService != null) {
                tsTracker.update(clockService.current());
            }
        }
    }

    /**
     * Returns a new table id not found exception.
     *
     * @param tableId Table id.
     * @return Exception.
     */
    public static TableNotFoundException tableIdNotFoundException(Integer tableId) {
        return new TableNotFoundException(UUID.randomUUID(), TABLE_ID_NOT_FOUND_ERR, "Table does not exist: " + tableId, null);
    }

    /**
     * Reads transaction.
     *
     * @param in Unpacker.
     * @param tsUpdater Packer.
     * @param resources Resource registry.
     * @param txManager Tx manager.
     * @param notificationSender Notification sender.
     * @param resourceIdHolder Resource id holder.
     * @return Transaction, if present, or null.
     */
    public static @Nullable InternalTransaction readTx(
            ClientMessageUnpacker in,
            HybridTimestampTracker tsUpdater,
            ClientResourceRegistry resources,
            @Nullable TxManager txManager,
            @Nullable NotificationSender notificationSender,
            long[] resourceIdHolder) {
        return readTx(in, tsUpdater, resources, txManager, notificationSender, resourceIdHolder, EnumSet.noneOf(RequestOptions.class));
    }

    /**
     * Reads transaction.
     *
     * @param in Unpacker.
     * @param tsUpdater Packer.
     * @param resources Resource registry.
     * @param txManager Tx manager.
     * @param notificationSender Notification sender.
     * @param resourceIdHolder Resource id holder.
     * @return Transaction, if present, or null.
     */
    public static @Nullable InternalTransaction readTx(
            ClientMessageUnpacker in,
            HybridTimestampTracker tsUpdater,
            ClientResourceRegistry resources,
            @Nullable TxManager txManager,
            @Nullable NotificationSender notificationSender,
            long[] resourceIdHolder,
            EnumSet<RequestOptions> options) {
        if (in.tryUnpackNil()) {
            return null;
        }

        try {
            long id = in.unpackLong();
            if (id == TX_ID_FIRST_DIRECT) {
                long observableTs = in.unpackLong();

                // This is first mapping request, which piggybacks transaction creation.
                boolean readOnly = in.unpackBoolean();
                long timeoutMillis = in.unpackLong();
                boolean implicit = in.unpackBoolean(); // TODO compat

                var builder = InternalTxOptions.builder().timeoutMillis(timeoutMillis);
                if (implicit && options.contains(RequestOptions.GET_ALL_FRAGMENT))  {
                    // Currently we use low priority with get all fragments to avoid conflicts with subsequent RW transactions.
                    // TODO ticket to avoid starvation.
                    builder.priority(TxPriority.LOW);
                }

                InternalTxOptions txOptions = builder.build();
                var tx = startExplicitTx(tsUpdater, txManager, HybridTimestamp.nullableHybridTimestamp(observableTs), readOnly, txOptions);

                // Attach resource id only on first direct request.
                resourceIdHolder[0] = resources.put(new ClientResource(tx, tx::rollbackAsync));

                return tx;
            } else if (id == TX_ID_DIRECT) {
                // This is direct request mapping.
                long token = in.unpackLong();
                UUID txId = in.unpackUuid();
                int commitTableId = in.unpackInt();
                int commitPart = in.unpackInt();
                UUID coord = in.unpackUuid();
                long timeout = in.unpackLong();

                InternalTransaction remote = txManager.beginRemote(txId, new TablePartitionId(commitTableId, commitPart),
                        coord, token, timeout, err -> {
                            // Will be called for write txns.
                            notificationSender.sendNotification(w -> w.packUuid(txId), err, NULL_HYBRID_TIMESTAMP);
                        });

                // Remote transaction will be synchronously rolled back if the timeout has exceeded.
                if (remote.isRolledBackWithTimeoutExceeded()) {
                    throw new TransactionException(TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR,
                            "Transaction is already finished [tx=" + remote + "].");
                }

                return remote;
            }

            var tx = resources.get(id).get(InternalTransaction.class);

            if (tx != null && tx.isReadOnly()) {
                // For read-only tx, override observable timestamp that we send to the client:
                // use readTimestamp() instead of now().
                tsUpdater.update(tx.readTimestamp()); // TODO https://issues.apache.org/jira/browse/IGNITE-24592
            }

            return tx;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteException(e.traceId(), e.code(), e.getMessage(), e);
        }
    }

    static InternalTransaction readOrStartImplicitTx( // TODO GET RID!!
            ClientMessageUnpacker in,
            HybridTimestampTracker readTs,
            ClientResourceRegistry resources,
            TxManager txManager,
            EnumSet<RequestOptions> options,
            @Nullable NotificationSender notificationSender,
            long[] resourceIdHolder) {
        InternalTransaction tx = readTx(in, readTs, resources, txManager, notificationSender, resourceIdHolder, options);

        if (tx == null) {
            // Implicit transactions do not use an observation timestamp because RW never depends on it, and implicit RO is always direct.
            // The direct transaction uses a current timestamp on the primary replica by definition.
            tx = startImplicitTx(readTs, txManager, options.contains(RequestOptions.READ_ONLY));
        }

        return tx;
    }

    /**
     * Starts an explicit transaction.
     *
     * @param tsTracker Tracker.
     * @param txManager Ignite transactions.
     * @param currentTs Current observation timestamp or {@code null} if it is not defined.
     * @param readOnly Read only flag.
     * @param options Transaction options.
     * @return Transaction.
     */
    public static InternalTransaction startExplicitTx(
            HybridTimestampTracker tsTracker,
            TxManager txManager,
            @Nullable HybridTimestamp currentTs,
            boolean readOnly,
            InternalTxOptions options
    ) {
        if (readOnly) {
            tsTracker.update(currentTs);

            return txManager.beginExplicitRo(tsTracker, options);
        } else {
            return txManager.beginExplicitRw(tsTracker, options);
        }
    }

    private static InternalTransaction startImplicitTx(
            HybridTimestampTracker tsTracker,
            TxManager txManager,
            boolean readOnly
    ) {
        return txManager.beginImplicit(tsTracker, readOnly);
    }

    /**
     * Gets type scale.
     *
     * @param type Type.
     * @return Scale.
     */
    public static int getDecimalScale(NativeType type) {
        return type instanceof DecimalNativeType ? ((DecimalNativeType) type).scale() : 0;
    }

    /**
     * Gets type precision.
     *
     * @param type Type.
     * @return Precision.
     */
    public static int getPrecision(NativeType type) {
        if (type instanceof TemporalNativeType) {
            return ((TemporalNativeType) type).precision();
        }

        if (type instanceof DecimalNativeType) {
            return ((DecimalNativeType) type).precision();
        }

        return 0;
    }
}
