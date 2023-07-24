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

import static org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl.DEFAULT_VALUE_PLACEHOLDER;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Sql.CONSTRAINT_VIOLATION_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.engine.metadata.NodeWithTerm;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
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

    private final HybridClock clock;

    private final ReplicaService replicaService;

    private final SchemaDescriptor schemaDescriptor;

    private final int[] columnsOrderedByPhysSchema;

    private final PartitionExtractor partitionExtractor;

    private final TableRowConverter rowConverter;

    /**
     * Constructor.
     *
     * @param desc  Table descriptor.
     */
    public UpdatableTableImpl(
            int tableId,
            TableDescriptor desc,
            int partitions,
            ReplicaService replicaService,
            HybridClock clock,
            TableRowConverter rowConverter,
            SchemaDescriptor schemaDescriptor
    ) {
        this.tableId = tableId;
        this.desc = desc;
        this.replicaService = replicaService;
        this.clock = clock;
        this.schemaDescriptor = schemaDescriptor;
        this.partitionExtractor = (row) -> IgniteUtils.safeAbs(row.colocationHash()) % partitions;

        columnsOrderedByPhysSchema = new int[desc.columnsCount()];
        for (int i = 0; i < desc.columnsCount(); i++) {
            ColumnDescriptor col = desc.columnDescriptor(i);

            int physIndex = schemaDescriptor.column(col.name()).schemaIndex();

            columnsOrderedByPhysSchema[physIndex] = col.logicalIndex();
        }

        this.rowConverter = rowConverter;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> CompletableFuture<?> upsertAll(
            ExecutionContext<RowT> ectx,
            List<RowT> rows
    ) {
        TxAttributes txAttributes = ectx.txAttributes();
        TablePartitionId commitPartitionId = txAttributes.commitPartition();

        assert commitPartitionId != null;

        Int2ObjectOpenHashMap<List<BinaryRow>> rowsByPartition = new Int2ObjectOpenHashMap<>();

        for (RowT row : rows) {
            BinaryRowEx binaryRow = convertRow(row, ectx, false);

            rowsByPartition.computeIfAbsent(partitionExtractor.fromRow(binaryRow), k -> new ArrayList<>()).add(binaryRow);
        }

        CompletableFuture<List<RowT>>[] futures = new CompletableFuture[rowsByPartition.size()];

        int batchNum = 0;

        for (Int2ObjectMap.Entry<List<BinaryRow>> partToRows : rowsByPartition.int2ObjectEntrySet()) {
            TablePartitionId partGroupId = new TablePartitionId(tableId, partToRows.getIntKey());
            NodeWithTerm nodeWithTerm = ectx.description().mapping().updatingTableAssignments().get(partToRows.getIntKey());

            ReplicaRequest request = MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                    .groupId(partGroupId)
                    .commitPartitionId(commitPartitionId)
                    .binaryRowsBytes(serializeBinaryRows(partToRows.getValue()))
                    .transactionId(txAttributes.id())
                    .term(nodeWithTerm.term())
                    .requestType(RequestType.RW_UPSERT_ALL)
                    .timestampLong(clock.nowLong())
                    .build();

            futures[batchNum++] = replicaService.invoke(nodeWithTerm.name(), request);
        }

        return CompletableFuture.allOf(futures);
    }

    private static List<ByteBuffer> serializeBinaryRows(Collection<BinaryRow> rows) {
        var result = new ArrayList<ByteBuffer>(rows.size());

        for (BinaryRow row : rows) {
            result.add(row.byteBuffer());
        }

        return result;
    }

    @Override
    public TableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> CompletableFuture<?> insertAll(
            ExecutionContext<RowT> ectx,
            List<RowT> rows
    ) {
        TxAttributes txAttributes = ectx.txAttributes();
        TablePartitionId commitPartitionId = txAttributes.commitPartition();

        assert commitPartitionId != null;

        RowHandler<RowT> handler = ectx.rowHandler();

        Int2ObjectOpenHashMap<List<BinaryRow>> rowsByPartition = new Int2ObjectOpenHashMap<>();

        for (RowT row : rows) {
            BinaryRowEx binaryRow = convertRow(row, ectx, false);

            rowsByPartition.computeIfAbsent(partitionExtractor.fromRow(binaryRow), k -> new ArrayList<>()).add(binaryRow);
        }

        CompletableFuture<List<RowT>>[] futures = new CompletableFuture[rowsByPartition.size()];

        int batchNum = 0;

        for (Int2ObjectMap.Entry<List<BinaryRow>> partToRows : rowsByPartition.int2ObjectEntrySet()) {
            TablePartitionId partGroupId = new TablePartitionId(tableId, partToRows.getIntKey());
            NodeWithTerm nodeWithTerm = ectx.description().mapping().updatingTableAssignments().get(partToRows.getIntKey());

            ReplicaRequest request = MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                    .groupId(partGroupId)
                    .commitPartitionId(commitPartitionId)
                    .binaryRowsBytes(serializeBinaryRows(partToRows.getValue()))
                    .transactionId(txAttributes.id())
                    .term(nodeWithTerm.term())
                    .requestType(RequestType.RW_INSERT_ALL)
                    .timestampLong(clock.nowLong())
                    .build();

            futures[batchNum++] = replicaService.invoke(nodeWithTerm.name(), request)
                .thenApply(result -> {
                    Collection<BinaryRow> binaryRows = (Collection<BinaryRow>) result;

                    if (binaryRows.isEmpty()) {
                        return List.of();
                    }

                    List<RowT> conflictRows = new ArrayList<>(binaryRows.size());
                    IgniteTypeFactory typeFactory = ectx.getTypeFactory();
                    RowHandler.RowFactory<RowT> rowFactory = handler.factory(
                            ectx.getTypeFactory(),
                            desc.insertRowType(typeFactory)
                    );

                    for (BinaryRow row : binaryRows) {
                        conflictRows.add(rowConverter.toRow(ectx, row, rowFactory, null));
                    }

                    return conflictRows;
                });
        }

        return handleInsertResults(handler, futures);
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> CompletableFuture<?> deleteAll(
            ExecutionContext<RowT> ectx,
            List<RowT> rows
    ) {
        TxAttributes txAttributes = ectx.txAttributes();
        TablePartitionId commitPartitionId = txAttributes.commitPartition();

        assert commitPartitionId != null;

        Int2ObjectOpenHashMap<List<BinaryRow>> keyRowsByPartition = new Int2ObjectOpenHashMap<>();

        for (RowT row : rows) {
            BinaryRowEx binaryRow = convertRow(row, ectx, true);

            keyRowsByPartition.computeIfAbsent(partitionExtractor.fromRow(binaryRow), k -> new ArrayList<>()).add(binaryRow);
        }

        CompletableFuture<List<RowT>>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Int2ObjectMap.Entry<List<BinaryRow>> partToRows : keyRowsByPartition.int2ObjectEntrySet()) {
            TablePartitionId partGroupId = new TablePartitionId(tableId, partToRows.getIntKey());
            NodeWithTerm nodeWithTerm = ectx.description().mapping().updatingTableAssignments().get(partToRows.getIntKey());

            ReplicaRequest request = MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                    .groupId(partGroupId)
                    .commitPartitionId(commitPartitionId)
                    .binaryRowsBytes(serializeBinaryRows(partToRows.getValue()))
                    .transactionId(txAttributes.id())
                    .term(nodeWithTerm.term())
                    .requestType(RequestType.RW_DELETE_ALL)
                    .timestampLong(clock.nowLong())
                    .build();

            futures[batchNum++] = replicaService.invoke(nodeWithTerm.name(), request);
        }

        return CompletableFuture.allOf(futures);
    }

    private <RowT> BinaryRowEx convertRow(RowT row, ExecutionContext<RowT> ectx, boolean keyOnly) {
        RowHandler<RowT> hnd = ectx.rowHandler();

        for (int i : columnsOrderedByPhysSchema) {
            ColumnDescriptor colDesc = desc.columnDescriptor(i);

            if (keyOnly && !colDesc.key()) {
                continue;
            }

            Object value = hnd.get(colDesc.logicalIndex(), row);

            // TODO Remove this check when https://issues.apache.org/jira/browse/IGNITE-19096 is complete
            assert value != DEFAULT_VALUE_PLACEHOLDER;

            if (value == null) {
                break;
            }
        }

        RowAssembler rowAssembler = keyOnly ? RowAssembler.keyAssembler(schemaDescriptor) : new RowAssembler(schemaDescriptor);

        for (int i : columnsOrderedByPhysSchema) {
            ColumnDescriptor colDesc = desc.columnDescriptor(i);

            if (keyOnly && !colDesc.key()) {
                continue;
            }

            Object val = hnd.get(colDesc.logicalIndex(), row);

            val = TypeUtils.fromInternal(val, NativeTypeSpec.toClass(colDesc.physicalType().spec(), colDesc.nullable()));

            RowAssembler.writeValue(rowAssembler, colDesc.physicalType(), val);
        }

        return new Row(schemaDescriptor, rowAssembler.build());
    }

    private static <RowT> CompletableFuture<List<RowT>> handleInsertResults(
            RowHandler<RowT> handler,
            CompletableFuture<List<RowT>>[] futs
    ) {
        return CompletableFuture.allOf(futs)
                .thenApply(response -> {
                    List<String> conflictRows = null;

                    for (CompletableFuture<List<RowT>> future : futs) {
                        List<RowT> values = future.join();

                        if (nullOrEmpty(values)) {
                            continue;
                        }

                        if (conflictRows == null) {
                            conflictRows = new ArrayList<>(values.size());
                        }

                        for (RowT row : values) {
                            conflictRows.add(handler.toString(row));
                        }
                    }

                    if (conflictRows != null) {
                        throw conflictKeysException(conflictRows);
                    }

                    return null;
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
}
