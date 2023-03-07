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

package org.apache.ignite.internal.sql.engine.schema;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.UpdateableTable;
import org.apache.ignite.internal.sql.engine.exec.exp.RexImpTable;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.NodeWithTerm;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite table implementation.
 */
public class IgniteTableImpl extends AbstractTable implements IgniteTable, UpdateableTable {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteTableImpl.class);

    private static final TableMessagesFactory MESSAGES_FACTORY = new TableMessagesFactory();

    private final TableDescriptor desc;

    private final int ver;

    private final InternalTable table;
    private final HybridClock clock;
    private final ReplicaService replicaService;

    private final SchemaRegistry schemaRegistry;

    public final SchemaDescriptor schemaDescriptor;

    private final Statistic statistic;

    private final Map<String, IgniteIndex> indexes = new HashMap<>();

    private final List<ColumnDescriptor> columnsOrderedByPhysSchema;

    private final PartitionExtractor partitionExtractor;

    /**
     * Constructor.
     *
     * @param desc  Table descriptor.
     * @param table Physical table this schema object created for.
     */
    IgniteTableImpl(
            TableDescriptor desc,
            InternalTable table,
            ReplicaService replicaService,
            HybridClock clock,
            SchemaRegistry schemaRegistry
    ) {
        this.ver = schemaRegistry.lastSchemaVersion();
        this.desc = desc;
        this.table = table;
        this.replicaService = replicaService;
        this.clock = clock;
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaRegistry.schema();
        this.partitionExtractor = table::partitionId;

        assert schemaDescriptor != null;

        List<ColumnDescriptor> tmp = new ArrayList<>(desc.columnsCount());
        for (int i = 0; i < desc.columnsCount(); i++) {
            tmp.add(desc.columnDescriptor(i));
        }

        tmp.sort(Comparator.comparingInt(ColumnDescriptor::physicalIndex));

        columnsOrderedByPhysSchema = tmp;

        statistic = new StatisticsImpl();
    }

    private IgniteTableImpl(IgniteTableImpl t) {
        this.desc = t.desc;
        this.ver = t.ver;
        this.table = t.table;
        this.replicaService = t.replicaService;
        this.clock = t.clock;
        this.schemaRegistry = t.schemaRegistry;
        this.schemaDescriptor = t.schemaDescriptor;
        this.statistic = t.statistic;
        this.columnsOrderedByPhysSchema = t.columnsOrderedByPhysSchema;
        this.partitionExtractor = t.partitionExtractor;
        this.indexes.putAll(t.indexes);
    }

    public static IgniteTableImpl copyOf(IgniteTableImpl v) {
        return new IgniteTableImpl(v);
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return table.tableId();
    }

    /** {@inheritDoc} */
    @Override
    public int version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return table().name();
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns) {
        return desc.rowType((IgniteTypeFactory) typeFactory, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public Statistic getStatistic() {
        return statistic;
    }


    /** {@inheritDoc} */
    @Override
    public TableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override
    public InternalTable table() {
        return table;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogicalTableScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTbl,
            List<RelHint> hints,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        RelTraitSet traitSet = cluster.traitSetOf(distribution());

        return IgniteLogicalTableScan.create(cluster, traitSet, hints, relOptTbl, proj, cond, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogicalIndexScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTable,
            String idxName,
            List<RexNode> proj,
            RexNode condition,
            ImmutableBitSet requiredCols
    ) {
        IgniteIndex index = getIndex(idxName);

        RelCollation collation = TraitUtils.createCollation(index.columns(), index.collations(), descriptor());

        RelTraitSet traitSet = cluster.traitSetOf(Convention.Impl.NONE)
                .replace(distribution())
                .replace(index.type() == Type.HASH ? RelCollations.EMPTY : collation);

        return IgniteLogicalIndexScan.create(cluster, traitSet, relOptTable, idxName, proj, condition, requiredCols);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return desc.distribution();
    }

    /** {@inheritDoc} */
    @Override
    public ColocationGroup colocationGroup(MappingQueryContext ctx) {
        return partitionedGroup();
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, IgniteIndex> indexes() {
        return Collections.unmodifiableMap(indexes);
    }

    /** {@inheritDoc} */
    @Override
    public void addIndex(IgniteIndex idxTbl) {
        indexes.put(idxTbl.name(), idxTbl);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteIndex getIndex(String idxName) {
        return indexes.get(idxName);
    }

    /** {@inheritDoc} */
    @Override
    public void removeIndex(String idxName) {
        indexes.remove(idxName);
    }

    /** {@inheritDoc} */
    @Override
    public <C> C unwrap(Class<C> cls) {
        if (cls.isInstance(desc)) {
            return cls.cast(desc);
        }

        return super.unwrap(cls);
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> RowT toRow(
            ExecutionContext<RowT> ectx,
            BinaryRow binaryRow,
            RowHandler.RowFactory<RowT> factory,
            @Nullable BitSet requiredColumns
    ) {
        RowHandler<RowT> handler = factory.handler();

        assert handler == ectx.rowHandler();

        RowT res = factory.create();

        assert handler.columnCount(res) == (requiredColumns == null ? desc.columnsCount() : requiredColumns.cardinality());

        Row row = schemaRegistry.resolve(binaryRow, schemaDescriptor);

        if (requiredColumns == null) {
            for (int i = 0; i < desc.columnsCount(); i++) {
                ColumnDescriptor colDesc = desc.columnDescriptor(i);

                handler.set(i, res, TypeUtils.toInternal(row.value(colDesc.physicalIndex())));
            }
        } else {
            for (int i = 0, j = requiredColumns.nextSetBit(0); j != -1; j = requiredColumns.nextSetBit(j + 1), i++) {
                ColumnDescriptor colDesc = desc.columnDescriptor(j);

                handler.set(i, res, TypeUtils.toInternal(row.value(colDesc.physicalIndex())));
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> CompletableFuture<?> upsertAll(
            ExecutionContext<RowT> ectx,
            List<RowT> rows
    ) {
        TxAttributes txAttributes = ectx.txAttributes();
        ReplicationGroupId commitPartitionId = txAttributes.commitPartition();

        assert commitPartitionId != null;

        UUID tableId = table.tableId();

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
                    .commitPartitionId((TablePartitionId) commitPartitionId)
                    .binaryRows(partToRows.getValue())
                    .transactionId(txAttributes.id())
                    .term(nodeWithTerm.term())
                    .requestType(RequestType.RW_UPSERT_ALL)
                    .timestamp(clock.now())
                    .build();

            futures[batchNum++] = replicaService.invoke(nodeWithTerm.name(), request);
        }

        return CompletableFuture.allOf(futures);
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> CompletableFuture<?> insertAll(
            ExecutionContext<RowT> ectx,
            List<RowT> rows
    ) {
        TxAttributes txAttributes = ectx.txAttributes();
        ReplicationGroupId commitPartitionId = txAttributes.commitPartition();

        assert commitPartitionId != null;

        RowHandler<RowT> handler = ectx.rowHandler();

        UUID tableId = table.tableId();

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
                    .commitPartitionId((TablePartitionId) commitPartitionId)
                    .binaryRows(partToRows.getValue())
                    .transactionId(txAttributes.id())
                    .term(nodeWithTerm.term())
                    .requestType(RequestType.RW_INSERT_ALL)
                    .timestamp(clock.now())
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
                        conflictRows.add(toRow(ectx, row, rowFactory, null));
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
        ReplicationGroupId commitPartitionId = txAttributes.commitPartition();

        assert commitPartitionId != null;

        UUID tableId = table.tableId();

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
                    .commitPartitionId((TablePartitionId) commitPartitionId)
                    .binaryRows(partToRows.getValue())
                    .transactionId(txAttributes.id())
                    .term(nodeWithTerm.term())
                    .requestType(RequestType.RW_DELETE_ALL)
                    .timestamp(clock.now())
                    .build();

            futures[batchNum++] = replicaService.invoke(nodeWithTerm.name(), request);
        }

        return CompletableFuture.allOf(futures);
    }

    private <RowT> BinaryRowEx convertRow(RowT row, ExecutionContext<RowT> ectx, boolean keyOnly) {
        RowHandler<RowT> hnd = ectx.rowHandler();

        boolean hasNulls = false;
        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            if (keyOnly && !colDesc.key()) {
                continue;
            }

            Object value = hnd.get(colDesc.logicalIndex(), row);

            assert value != RexImpTable.DEFAULT_VALUE_PLACEHOLDER;

            if (value == null) {
                hasNulls = true;
                break;
            }
        }

        RowAssembler rowAssembler = keyOnly
                ? RowAssembler.keyAssembler(schemaDescriptor, hasNulls)
                : new RowAssembler(schemaDescriptor, hasNulls);

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            if (keyOnly && !colDesc.key()) {
                continue;
            }

            Object val = hnd.get(colDesc.logicalIndex(), row);

            val = TypeUtils.fromInternal(val, NativeTypeSpec.toClass(colDesc.physicalType().spec(), colDesc.nullable()));

            RowAssembler.writeValue(rowAssembler, colDesc.physicalType(), val);
        }

        return new Row(schemaDescriptor, rowAssembler.build());
    }

    private ColocationGroup partitionedGroup() {
        List<List<NodeWithTerm>> assignments = table.primaryReplicas().stream()
                .map(primaryReplica -> new NodeWithTerm(primaryReplica.node().name(), primaryReplica.term()))
                .map(Collections::singletonList)
                .collect(Collectors.toList());

        return ColocationGroup.forAssignments(assignments);
    }

    private class StatisticsImpl implements Statistic {
        private static final int STATS_CLI_UPDATE_THRESHOLD = 200;

        AtomicInteger statReqCnt = new AtomicInteger();

        private volatile long localRowCnt;

        /** {@inheritDoc} */
        @Override
        public Double getRowCount() {
            if (statReqCnt.getAndIncrement() % STATS_CLI_UPDATE_THRESHOLD == 0) {
                int parts = table.storage().configuration().partitions().value();

                long size = 0L;

                for (int p = 0; p < parts; ++p) {
                    @Nullable MvPartitionStorage part = table.storage().getMvPartition(p);

                    if (part != null) {
                        size += part.rowsCount();
                    }
                }

                localRowCnt = size;
            }

            // Forbid zero result, to prevent zero cost for table and index scans.
            return Math.max(10_000.0, (double) localRowCnt);
        }

        /** {@inheritDoc} */
        @Override
        public boolean isKey(ImmutableBitSet cols) {
            return false; // TODO
        }

        /** {@inheritDoc} */
        @Override
        public List<ImmutableBitSet> getKeys() {
            return null; // TODO
        }

        /** {@inheritDoc} */
        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return List.of();
        }

        /** {@inheritDoc} */
        @Override
        public List<RelCollation> getCollations() {
            return List.of(); // The method isn't used
        }

        /** {@inheritDoc} */
        @Override
        public IgniteDistribution getDistribution() {
            return distribution();
        }
    }

    private static int[] project(int columnCount, int[] source, BitSet usedFields) {
        int[] deltas = new int[columnCount];
        int[] projection = new int[source.length];

        for (int currentIndex = 0, nextIndex = usedFields.nextSetBit(0);
                nextIndex != -1; nextIndex = usedFields.nextSetBit(nextIndex + 1), currentIndex++) {
            deltas[nextIndex] = -(nextIndex - currentIndex);
        }

        for (int i = 0; i < source.length; i++) {
            projection[i] = source[i] + deltas[source[i]];
        }

        return projection;
    }

    private static <RowT> CompletableFuture<List<RowT>> handleInsertResults(
            RowHandler<RowT> handler,
            CompletableFuture<List<RowT>>[] futs
    ) {
        return CompletableFuture.allOf(futs)
                .thenApply(response -> {
                    List<String> conflictRows = new ArrayList<>();

                    for (CompletableFuture<List<RowT>> future : futs) {
                        List<RowT> values = future.join();

                        if (values != null) {
                            for (RowT row : values) {
                                conflictRows.add(handler.toString(row));
                            }
                        }
                    }

                    if (!conflictRows.isEmpty()) {
                        throw conflictKeysException(conflictRows);
                    }

                    return null;
                });
    }

    /** Transforms keys list to appropriate exception. */
    private static RuntimeException conflictKeysException(List<String> conflictKeys) {
        LOG.debug("Unable to insert rows because of conflict [rows={}]", conflictKeys);

        return new SqlException(ErrorGroups.Sql.DUPLICATE_KEYS_ERR, "PK unique constraint is violated");
    }

    /**
     * Extracts an identifier of partition from a given row.
     */
    @FunctionalInterface
    private interface PartitionExtractor {
        int fromRow(BinaryRowEx row);
    }
}
