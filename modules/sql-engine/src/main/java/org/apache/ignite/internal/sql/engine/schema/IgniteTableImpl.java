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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.table.InternalTable;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite table implementation.
 */
public class IgniteTableImpl extends AbstractTable implements IgniteTable {

    private final TableDescriptor desc;

    private final int ver;

    private final int id;

    private final String name;

    private final Statistic statistic;

    private final Map<String, IgniteIndex> indexes = new HashMap<>();

    /**
     * Constructor.
     *
     * @param desc Table descriptor.
     * @param tableId Table id.
     * @param name Table name.
     */
    IgniteTableImpl(TableDescriptor desc, int tableId, String name, int version,
            DoubleSupplier rowCount) {
        this.ver = version;
        this.desc = desc;
        this.id = tableId;
        this.name = name;
        this.statistic = new IgniteStatistic(rowCount, desc.distribution());
    }

    private IgniteTableImpl(IgniteTableImpl t) {
        this.desc = t.desc;
        this.ver = t.ver;
        this.id = t.id;
        this.name = t.name;
        this.statistic = t.statistic;
        this.indexes.putAll(t.indexes);
    }

    public static IgniteTableImpl copyOf(IgniteTableImpl v) {
        return new IgniteTableImpl(v);
    }

    /** {@inheritDoc} */
    @Override
    public int id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public int version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
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

    static DoubleSupplier rowCountStatistic(InternalTable table) {
        return new RowCountStatistic(table);
    }

    private static final class RowCountStatistic implements DoubleSupplier {
        private static final int UPDATE_THRESHOLD = DEFAULT_PARTITION_COUNT;

        private final AtomicLong lastUpd = new AtomicLong();

        private volatile long localRowCnt = 0L;

        private final InternalTable table;

        private RowCountStatistic(InternalTable table) {
            this.table = table;
        }

        /** {@inheritDoc} */
        @Override
        // TODO: need to be refactored https://issues.apache.org/jira/browse/IGNITE-19558
        public double getAsDouble() {
            int parts = table.storage().getTableDescriptor().getPartitions();

            long partitionsRevisionCounter = 0L;

            for (int p = 0; p < parts; ++p) {
                @Nullable MvPartitionStorage part = table.storage().getMvPartition(p);

                if (part == null) {
                    continue;
                }

                long upd = part.lastAppliedIndex();

                partitionsRevisionCounter += upd;
            }

            long prev = lastUpd.get();

            if (partitionsRevisionCounter - prev > UPDATE_THRESHOLD) {
                synchronized (this) {
                    if (lastUpd.compareAndSet(prev, partitionsRevisionCounter)) {
                        long size = 0L;

                        for (int p = 0; p < parts; ++p) {
                            @Nullable MvPartitionStorage part = table.storage().getMvPartition(p);

                            if (part == null) {
                                continue;
                            }

                            try {
                                size += part.rowsCount();
                            } catch (StorageRebalanceException ignore) {
                                // No-op.
                            }
                        }

                        localRowCnt = size;
                    }
                }
            }

            // Forbid zero result, to prevent zero cost for table and index scans.
            return Math.max(10_000.0, (double) localRowCnt);
        }
    }
}
