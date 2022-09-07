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

package org.apache.ignite.internal.sql.engine.schema;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.RexImpTable;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.ModifyRow.Operation;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.RewindabilityTrait;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite table implementation.
 */
public class IgniteTableImpl extends AbstractTable implements InternalIgniteTable {
    private final TableDescriptor desc;

    private final int ver;

    private final InternalTable table;

    private final SchemaRegistry schemaRegistry;

    public final SchemaDescriptor schemaDescriptor;

    private final Statistic statistic;

    private Map<String, IgniteIndex> indexes = new HashMap<>();

    private final List<ColumnDescriptor> columnsOrderedByPhysSchema;

    /**
     * Constructor.
     *
     * @param desc  Table descriptor.
     * @param table Physical table this schema object created for.
     */
    public IgniteTableImpl(
            TableDescriptor desc,
            InternalTable table,
            SchemaRegistry schemaRegistry
    ) {
        this.ver = schemaRegistry.lastSchemaVersion();
        this.desc = desc;
        this.table = table;
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaRegistry.schema();

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
        this.schemaRegistry = t.schemaRegistry;
        this.schemaDescriptor = t.schemaDescriptor;
        this.statistic = t.statistic;
        this.columnsOrderedByPhysSchema = t.columnsOrderedByPhysSchema;
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
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        RelTraitSet traitSet = cluster.traitSetOf(distribution())
                .replace(RewindabilityTrait.REWINDABLE);

        return IgniteLogicalTableScan.create(cluster, traitSet, relOptTbl, proj, cond, requiredColumns);
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
                .replace(RewindabilityTrait.REWINDABLE)
                .replace(collation);

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
            @Nullable ImmutableBitSet requiredColumns
    ) {
        RowHandler<RowT> handler = factory.handler();

        assert handler == ectx.rowHandler();

        RowT res = factory.create();

        assert handler.columnCount(res) == (requiredColumns == null ? desc.columnsCount() : requiredColumns.cardinality());

        Row row = schemaRegistry.resolve(binaryRow, schemaDescriptor);

        if (requiredColumns == null) {
            for (int i = 0; i < desc.columnsCount(); i++) {
                ColumnDescriptor colDesc = desc.columnDescriptor(i);

                handler.set(i, res, TypeUtils.toInternal(ectx, row.value(colDesc.physicalIndex())));
            }
        } else {
            for (int i = 0, j = requiredColumns.nextSetBit(0); j != -1; j = requiredColumns.nextSetBit(j + 1), i++) {
                ColumnDescriptor colDesc = desc.columnDescriptor(j);

                handler.set(i, res, TypeUtils.toInternal(ectx, row.value(colDesc.physicalIndex())));
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> ModifyRow toModifyRow(
            ExecutionContext<RowT> ectx,
            RowT row,
            TableModify.Operation op,
            List<String> arg
    ) {
        switch (op) {
            case INSERT:
                return insertTuple(row, ectx);
            case DELETE:
                return deleteTuple(row, ectx);
            case UPDATE:
                return updateTuple(row, arg, 0, ectx);
            case MERGE:
                return mergeTuple(row, arg, ectx);
            default:
                throw new AssertionError();
        }
    }

    private <RowT> ModifyRow insertTuple(RowT row, ExecutionContext<RowT> ectx) {
        int nonNullVarlenKeyCols = 0;
        int nonNullVarlenValCols = 0;

        RowHandler<RowT> hnd = ectx.rowHandler();

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            if (colDesc.physicalType().spec().fixedLength()) {
                continue;
            }

            Object val = hnd.get(colDesc.logicalIndex(), row);

            if (val != null) {
                if (colDesc.key()) {
                    nonNullVarlenKeyCols++;
                } else {
                    nonNullVarlenValCols++;
                }
            }
        }

        RowAssembler rowAssembler = new RowAssembler(schemaDescriptor, nonNullVarlenKeyCols, nonNullVarlenValCols);

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            Object val;

            if (colDesc.key() && Commons.implicitPkEnabled() && Commons.IMPLICIT_PK_COL_NAME.equals(colDesc.name())) {
                val = colDesc.defaultValue();
            } else {
                val = replaceDefault(hnd.get(colDesc.logicalIndex(), row), colDesc);
            }

            val = TypeUtils.fromInternal(ectx, val, NativeTypeSpec.toClass(colDesc.physicalType().spec(), colDesc.nullable()));

            RowAssembler.writeValue(rowAssembler, colDesc.physicalType(), val);
        }

        return new ModifyRow(new Row(schemaDescriptor, rowAssembler.build()), Operation.INSERT_ROW);
    }

    private <RowT> ModifyRow mergeTuple(RowT row, List<String> updateColList, ExecutionContext<RowT> ectx) {
        RowHandler<RowT> hnd = ectx.rowHandler();

        int rowColumnsCnt = hnd.columnCount(row);

        if (desc.columnsCount() == rowColumnsCnt) { // Only WHEN NOT MATCHED clause in MERGE.
            return insertTuple(row, ectx);
        } else if (desc.columnsCount() + updateColList.size() == rowColumnsCnt) { // Only WHEN MATCHED clause in MERGE.
            return updateTuple(row, updateColList, 0, ectx);
        } else { // Both WHEN MATCHED and WHEN NOT MATCHED clauses in MERGE.
            int off = columnsOrderedByPhysSchema.size();

            if (hnd.get(off, row) == null) {
                return insertTuple(row, ectx);
            } else {
                return updateTuple(row, updateColList, off, ectx);
            }
        }
    }

    private <RowT> ModifyRow updateTuple(RowT row, List<String> updateColList, int offset, ExecutionContext<RowT> ectx) {
        RowHandler<RowT> hnd = ectx.rowHandler();

        Object2IntMap<String> columnToIndex = new Object2IntOpenHashMap<>(updateColList.size());

        for (int i = 0; i < updateColList.size(); i++) {
            columnToIndex.put(updateColList.get(i), i + desc.columnsCount() + offset);
        }

        int nonNullVarlenKeyCols = 0;
        int nonNullVarlenValCols = 0;

        int keyOffset = schemaDescriptor.keyColumns().firstVarlengthColumn();

        if (keyOffset > -1) {
            nonNullVarlenKeyCols = countNotNullColumns(
                    keyOffset,
                    schemaDescriptor.keyColumns().length(),
                    columnToIndex, offset, hnd, row);
        }

        int valOffset = schemaDescriptor.valueColumns().firstVarlengthColumn();

        if (valOffset > -1) {
            nonNullVarlenValCols = countNotNullColumns(
                    schemaDescriptor.keyColumns().length() + valOffset,
                    schemaDescriptor.length(),
                    columnToIndex, offset, hnd, row);
        }

        RowAssembler rowAssembler = new RowAssembler(schemaDescriptor, nonNullVarlenKeyCols, nonNullVarlenValCols);

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            int colIdx = columnToIndex.getOrDefault(colDesc.name(), colDesc.logicalIndex() + offset);

            Object val = TypeUtils.fromInternal(ectx, hnd.get(colIdx, row),
                    NativeTypeSpec.toClass(colDesc.physicalType().spec(), colDesc.nullable()));

            RowAssembler.writeValue(rowAssembler, colDesc.physicalType(), val);
        }

        return new ModifyRow(new Row(schemaDescriptor, rowAssembler.build()), Operation.UPDATE_ROW);
    }

    private <RowT> int countNotNullColumns(int start, int end, Object2IntMap<String> columnToIndex, int offset,
            RowHandler<RowT> hnd, RowT row) {
        int nonNullCols = 0;

        for (int i = start; i < end; i++) {
            ColumnDescriptor colDesc = Objects.requireNonNull(columnsOrderedByPhysSchema.get(i));

            assert !colDesc.physicalType().spec().fixedLength();

            int colIdInRow = columnToIndex.getOrDefault(colDesc.name(), colDesc.logicalIndex() + offset);

            if (hnd.get(colIdInRow, row) != null) {
                nonNullCols++;
            }
        }

        return nonNullCols;
    }

    private <RowT> ModifyRow deleteTuple(RowT row, ExecutionContext<RowT> ectx) {
        int nonNullVarlenKeyCols = 0;

        RowHandler<RowT> hnd = ectx.rowHandler();

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            if (!colDesc.key()) {
                break;
            }

            if (colDesc.physicalType().spec().fixedLength()) {
                continue;
            }

            Object val = hnd.get(colDesc.logicalIndex(), row);

            if (val != null) {
                nonNullVarlenKeyCols++;
            }
        }

        RowAssembler rowAssembler = new RowAssembler(schemaDescriptor, nonNullVarlenKeyCols, 0);

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            if (!colDesc.key()) {
                break;
            }

            Object val = TypeUtils.fromInternal(ectx, hnd.get(colDesc.logicalIndex(), row),
                    NativeTypeSpec.toClass(colDesc.physicalType().spec(), colDesc.nullable()));

            RowAssembler.writeValue(rowAssembler, colDesc.physicalType(), val);
        }

        return new ModifyRow(new Row(schemaDescriptor, rowAssembler.build()), Operation.DELETE_ROW);
    }

    private ColocationGroup partitionedGroup() {
        List<List<String>> assignments = table.assignments().stream()
                .map(Collections::singletonList)
                .collect(Collectors.toList());

        return ColocationGroup.forAssignments(assignments);
    }

    private Object replaceDefault(Object val, ColumnDescriptor desc) {
        return val == RexImpTable.DEFAULT_VALUE_PLACEHOLDER ? desc.defaultValue() : val;
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

            return (double) localRowCnt;
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
}
