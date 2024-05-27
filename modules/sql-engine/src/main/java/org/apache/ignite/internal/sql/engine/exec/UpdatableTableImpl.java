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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.rowSchemaFromRelTypes;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.collectRejectedRowsResponsesWithRestoreOrder;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Sql.CONSTRAINT_VIOLATION_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Static;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.command.ZonePartitionIdMessage;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.table.distributed.storage.RowBatch;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.SqlException;

/**
 * Ignite table implementation.
 */
public final class UpdatableTableImpl implements UpdatableTable {

    private static final IgniteLogger LOG = Loggers.forClass(UpdatableTableImpl.class);

    private static final TableMessagesFactory MESSAGES_FACTORY = new TableMessagesFactory();

    private final int tableId;

    private final TableDescriptor desc;

    private final ClockService clockService;

    private final InternalTable table;

    private final ReplicaService replicaService;

    private final PartitionExtractor partitionExtractor;

    private final TableRowConverter rowConverter;

    private RowSchema rowSchema;

    /** Constructor. */
    UpdatableTableImpl(
            int tableId,
            TableDescriptor desc,
            int partitions,
            InternalTable table,
            ReplicaService replicaService,
            ClockService clockService,
            TableRowConverter rowConverter
    ) {
        this.tableId = tableId;
        this.table = table;
        this.desc = desc;
        this.replicaService = replicaService;
        this.clockService = clockService;
        this.partitionExtractor = (row) -> IgniteUtils.safeAbs(row.colocationHash()) % partitions;
        this.rowConverter = rowConverter;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> CompletableFuture<?> upsertAll(
            ExecutionContext<RowT> ectx,
            List<RowT> rows,
            ColocationGroup colocationGroup
    ) {
        TxAttributes txAttributes = ectx.txAttributes();
        ZonePartitionId zoneCommitPartitionId = txAttributes.zoneCommitPartition();

        assert zoneCommitPartitionId != null;

        validateNotNullConstraint(ectx.rowHandler(), rows);

        RelDataType rowType = descriptor().rowType(ectx.getTypeFactory(), null);
        Supplier<RowSchema> schemaSupplier = makeSchemaSupplier(ectx);

        rows = validateCharactersOverflowAndTrimIfPossible(rowType, ectx.rowHandler(), rows, schemaSupplier);

        Int2ObjectOpenHashMap<List<BinaryRow>> rowsByPartition = new Int2ObjectOpenHashMap<>();

        for (RowT row : rows) {
            BinaryRowEx binaryRow = rowConverter.toFullRow(ectx, row);

            rowsByPartition.computeIfAbsent(partitionExtractor.fromRow(binaryRow), k -> new ArrayList<>()).add(binaryRow);
        }

        @SuppressWarnings("unchecked")
        CompletableFuture<List<RowT>>[] futures = new CompletableFuture[rowsByPartition.size()];

        int batchNum = 0;

        for (Int2ObjectMap.Entry<List<BinaryRow>> partToRows : rowsByPartition.int2ObjectEntrySet()) {
            ZonePartitionId partGroupId = new ZonePartitionId(table.zoneId(), tableId, partToRows.getIntKey());

            NodeWithConsistencyToken nodeWithConsistencyToken = colocationGroup.assignments().get(partToRows.getIntKey());

            ReplicaRequest request = MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                    .groupId(partGroupId)
                    .zoneCommitPartitionId(serializeZonePartitionId(zoneCommitPartitionId))
                    .schemaVersion(partToRows.getValue().get(0).schemaVersion())
                    .binaryTuples(binaryRowsToBuffers(partToRows.getValue()))
                    .transactionId(txAttributes.id())
                    .enlistmentConsistencyToken(nodeWithConsistencyToken.enlistmentConsistencyToken())
                    .requestType(RequestType.RW_UPSERT_ALL)
                    .timestampLong(clockService.nowLong())
                    .skipDelayedAck(true)
                    .coordinatorId(txAttributes.coordinatorId())
                    .build();

            futures[batchNum++] = replicaService.invoke(nodeWithConsistencyToken.name(), request);
        }

        return CompletableFuture.allOf(futures);
    }

    private static List<ByteBuffer> binaryRowsToBuffers(Collection<BinaryRow> rows) {
        var result = new ArrayList<ByteBuffer>(rows.size());

        for (BinaryRow row : rows) {
            result.add(row.tupleSlice());
        }

        return result;
    }

    private static List<ByteBuffer> serializePrimaryKeys(Collection<BinaryRow> rows) {
        var result = new ArrayList<ByteBuffer>(rows.size());

        for (BinaryRow row : rows) {
            result.add(row.tupleSlice());
        }

        return result;
    }

    private ZonePartitionIdMessage serializeZonePartitionId(ZonePartitionId id) {
        return MESSAGES_FACTORY.zonePartitionIdMessage()
                .zoneId(id.zoneId())
                .tableId(id.tableId())
                .partitionId(id.partitionId())
                .build();
    }

    @Override
    public TableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> CompletableFuture<Void> insert(InternalTransaction tx, ExecutionContext<RowT> ectx, RowT row) {
        validateNotNullConstraint(ectx.rowHandler(), row);

        RelDataType rowType = descriptor().rowType(ectx.getTypeFactory(), null);
        Supplier<RowSchema> schemaSupplier = makeSchemaSupplier(ectx);

        RowT validatedRow = TypeUtils.validateCharactersOverflowAndTrimIfPossible(rowType, ectx.rowHandler(), row, schemaSupplier);

        BinaryRowEx tableRow = rowConverter.toFullRow(ectx, validatedRow);

        return table.insert(tableRow, tx)
                .thenApply(success -> {
                    if (success) {
                        return null;
                    }

                    RowHandler<RowT> rowHandler = ectx.rowHandler();

                    throw conflictKeysException(List.of(rowHandler.toString(validatedRow)));
                });
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> CompletableFuture<?> insertAll(
            ExecutionContext<RowT> ectx,
            List<RowT> rows,
            ColocationGroup colocationGroup
    ) {
        TxAttributes txAttributes = ectx.txAttributes();
        ZonePartitionId zoneCommitPartitionId = txAttributes.zoneCommitPartition();

        validateNotNullConstraint(ectx.rowHandler(), rows);

        RelDataType rowType = descriptor().rowType(ectx.getTypeFactory(), null);
        Supplier<RowSchema> schemaSupplier = makeSchemaSupplier(ectx);

        rows = validateCharactersOverflowAndTrimIfPossible(rowType, ectx.rowHandler(), rows, schemaSupplier);

        assert zoneCommitPartitionId != null;

        Int2ObjectMap<RowBatch> rowBatchByPartitionId = toRowBatchByPartitionId(ectx, rows);

        for (Int2ObjectMap.Entry<RowBatch> partitionRowBatch : rowBatchByPartitionId.int2ObjectEntrySet()) {
            int partitionId = partitionRowBatch.getIntKey();
            RowBatch rowBatch = partitionRowBatch.getValue();

            ZonePartitionId partGroupId = new ZonePartitionId(table.zoneId(), tableId, partitionId);

            NodeWithConsistencyToken nodeWithConsistencyToken = colocationGroup.assignments().get(partitionId);

            ReadWriteMultiRowReplicaRequest request = MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                    .groupId(partGroupId)
                    .zoneCommitPartitionId(serializeZonePartitionId(zoneCommitPartitionId))
                    .schemaVersion(rowBatch.requestedRows.get(0).schemaVersion())
                    .binaryTuples(binaryRowsToBuffers(rowBatch.requestedRows))
                    .transactionId(txAttributes.id())
                    .enlistmentConsistencyToken(nodeWithConsistencyToken.enlistmentConsistencyToken())
                    .requestType(RequestType.RW_INSERT_ALL)
                    .timestampLong(clockService.nowLong())
                    .skipDelayedAck(true)
                    .coordinatorId(txAttributes.coordinatorId())
                    .build();

            rowBatch.resultFuture = replicaService.invoke(nodeWithConsistencyToken.name(), request);
        }

        return handleInsertResults(ectx, rowBatchByPartitionId.values());
    }

    /**
     * Creates batches of rows for processing, grouped by partition ID.
     *
     * @param ectx Execution context.
     * @param rows Rows.
     */
    private <RowT> Int2ObjectMap<RowBatch> toRowBatchByPartitionId(ExecutionContext<RowT> ectx, List<RowT> rows) {
        Int2ObjectMap<RowBatch> rowBatchByPartitionId = new Int2ObjectOpenHashMap<>();

        int i = 0;

        for (RowT row : rows) {
            BinaryRowEx binaryRow = rowConverter.toFullRow(ectx, row);

            rowBatchByPartitionId.computeIfAbsent(partitionExtractor.fromRow(binaryRow), partitionId -> new RowBatch()).add(binaryRow, i++);
        }

        return rowBatchByPartitionId;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> CompletableFuture<?> deleteAll(
            ExecutionContext<RowT> ectx,
            List<RowT> rows,
            ColocationGroup colocationGroup
    ) {
        TxAttributes txAttributes = ectx.txAttributes();
        ZonePartitionId zoneCommitPartitionId = txAttributes.zoneCommitPartition();

        assert zoneCommitPartitionId != null;

        Int2ObjectOpenHashMap<List<BinaryRow>> keyRowsByPartition = new Int2ObjectOpenHashMap<>();

        for (RowT row : rows) {
            BinaryRowEx binaryRow = rowConverter.toKeyRow(ectx, row);

            keyRowsByPartition.computeIfAbsent(partitionExtractor.fromRow(binaryRow), k -> new ArrayList<>()).add(binaryRow);
        }

        @SuppressWarnings("unchecked")
        CompletableFuture<List<RowT>>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Int2ObjectMap.Entry<List<BinaryRow>> partToRows : keyRowsByPartition.int2ObjectEntrySet()) {
            ZonePartitionId partGroupId = new ZonePartitionId(table.zoneId(), tableId, partToRows.getIntKey());

            NodeWithConsistencyToken nodeWithConsistencyToken = colocationGroup.assignments().get(partToRows.getIntKey());

            ReplicaRequest request = MESSAGES_FACTORY.readWriteMultiRowPkReplicaRequest()
                    .groupId(partGroupId)
                    .zoneCommitPartitionId(serializeZonePartitionId(zoneCommitPartitionId))
                    .schemaVersion(partToRows.getValue().get(0).schemaVersion())
                    .primaryKeys(serializePrimaryKeys(partToRows.getValue()))
                    .transactionId(txAttributes.id())
                    .enlistmentConsistencyToken(nodeWithConsistencyToken.enlistmentConsistencyToken())
                    .requestType(RequestType.RW_DELETE_ALL)
                    .timestampLong(clockService.nowLong())
                    .skipDelayedAck(true)
                    .coordinatorId(txAttributes.coordinatorId())
                    .build();

            futures[batchNum++] = replicaService.invoke(nodeWithConsistencyToken.name(), request);
        }

        return CompletableFuture.allOf(futures);
    }

    private <RowT> CompletableFuture<List<RowT>> handleInsertResults(
            ExecutionContext<RowT> ectx,
            Collection<RowBatch> batches
    ) {
        return collectRejectedRowsResponsesWithRestoreOrder(batches)
                .thenApply(response -> {
                    if (nullOrEmpty(response)) {
                        return null;
                    }

                    RowHandler<RowT> handler = ectx.rowHandler();
                    IgniteTypeFactory typeFactory = ectx.getTypeFactory();
                    RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(desc.rowType(typeFactory, null)));
                    RowHandler.RowFactory<RowT> rowFactory = handler.factory(rowSchema);

                    ArrayList<String> conflictRows = new ArrayList<>(response.size());

                    for (BinaryRow row : response) {
                        conflictRows.add(handler.toString(rowConverter.toRow(ectx, row, rowFactory)));
                    }

                    throw conflictKeysException(conflictRows);
                });
    }

    /** Transforms keys list to appropriate exception. */
    private static RuntimeException conflictKeysException(List<String> conflictKeys) {
        LOG.debug("Unable to insert rows because of conflict [rows={}]", conflictKeys);

        return new SqlException(CONSTRAINT_VIOLATION_ERR, "PK unique constraint is violated");
    }

    /**
     * Extracts an identifier of partition from a given row.
     */
    @FunctionalInterface
    private interface PartitionExtractor {
        int fromRow(BinaryRowEx row);
    }

    private static <RowT> List<RowT> validateCharactersOverflowAndTrimIfPossible(
            RelDataType rowType,
            RowHandler<RowT> rowHandler,
            List<RowT> rows,
            Supplier<RowSchema> schemaSupplier
    ) {
        List<RowT> out = new ArrayList<>(rows.size());

        for (RowT row : rows) {
            out.add(TypeUtils.validateCharactersOverflowAndTrimIfPossible(rowType, rowHandler, row, schemaSupplier));
        }

        return out;
    }

    private <RowT> void validateNotNullConstraint(RowHandler<RowT> rowHandler, List<RowT> rows) {
        for (RowT row : rows) {
            validateNotNullConstraint(rowHandler, row);
        }
    }

    private <RowT> void validateNotNullConstraint(RowHandler<RowT> rowHandler, RowT row) {
        for (int i = 0; i < desc.columnsCount(); i++) {
            ColumnDescriptor column = desc.columnDescriptor(i);

            if (!column.nullable() && rowHandler.isNull(i, row)) {
                String message = Static.RESOURCE.columnNotNullable(column.name()).ex().getMessage();
                throw new SqlException(CONSTRAINT_VIOLATION_ERR, message);
            }
        }
    }

    private <RowT> Supplier<RowSchema> makeSchemaSupplier(ExecutionContext<RowT> ectx) {
        return () -> {
            if (rowSchema != null) {
                return rowSchema;
            }

            RelDataType rowType = descriptor().rowType(ectx.getTypeFactory(), null);
            rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
            return rowSchema;
        };
    }
}
