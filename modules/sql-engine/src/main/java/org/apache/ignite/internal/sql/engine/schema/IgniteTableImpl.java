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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.NodeWithTerm;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
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

    private final InternalTable table;

    private final Statistic statistic;

    private final Map<String, IgniteIndex> indexes = new HashMap<>();

    private final List<ColumnDescriptor> columnsOrderedByPhysSchema;

    /**
     * Constructor.
     *
     * @param desc Table descriptor.
     * @param table Physical table this schema object created for.
     */
    IgniteTableImpl(TableDescriptor desc, InternalTable table, int version) {
        this.ver = version;
        this.desc = desc;
        this.table = table;

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
        this.statistic = t.statistic;
        this.columnsOrderedByPhysSchema = t.columnsOrderedByPhysSchema;
        this.indexes.putAll(t.indexes);
    }

    public static IgniteTableImpl copyOf(IgniteTableImpl v) {
        return new IgniteTableImpl(v);
    }

    /** {@inheritDoc} */
    @Override
    public int id() {
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
        return table.name();
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

    private ColocationGroup partitionedGroup() {
        List<List<NodeWithTerm>> assignments = table.primaryReplicas().stream()
                .map(primaryReplica -> new NodeWithTerm(primaryReplica.node().name(), primaryReplica.term()))
                .map(Collections::singletonList)
                .collect(Collectors.toList());

        return ColocationGroup.forAssignments(assignments);
    }

    private class StatisticsImpl implements Statistic {
        private final int updateThreshold = DistributionZoneManager.DEFAULT_PARTITION_COUNT;

        private final AtomicLong lastUpd = new AtomicLong();

        private volatile long localRowCnt = 0L;

        /** {@inheritDoc} */
        @Override
        // TODO: need to be refactored https://issues.apache.org/jira/browse/IGNITE-19558
        public Double getRowCount() {
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

            if (partitionsRevisionCounter - prev > updateThreshold) {
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
