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

package org.apache.ignite.internal.sql.engine.framework;

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.table.InternalTable;
import org.jetbrains.annotations.Nullable;

/**
 * A test table that implements all the necessary for the optimizer methods to be used
 * to prepare a query, as well as provides access to the data to use this table in
 * execution-related scenarios.
 */
public class TestTable implements IgniteTable {
    private static final String DATA_PROVIDER_NOT_CONFIGURED_MESSAGE_TEMPLATE =
            "DataProvider is not configured [table={}, node={}]";

    private final UUID id = UUID.randomUUID();
    private final Map<String, IgniteIndex> indexes = new HashMap<>();

    private final String name;
    private final double rowCnt;
    private final ColocationGroup colocationGroup;
    private final TableDescriptor descriptor;
    private final Map<String, DataProvider<?>> dataProviders;


    /** Constructor. */
    public TestTable(
            TableDescriptor descriptor,
            String name,
            ColocationGroup colocationGroup,
            double rowCnt
    ) {
        this.descriptor = descriptor;
        this.name = name;
        this.rowCnt = rowCnt;
        this.colocationGroup = colocationGroup;

        dataProviders = Collections.emptyMap();
    }

    /** Constructor. */
    public TestTable(
            TableDescriptor descriptor,
            String name,
            Map<String, DataProvider<?>> dataProviders,
            double rowCnt
    ) {
        this.descriptor = descriptor;
        this.name = name;
        this.rowCnt = rowCnt;
        this.dataProviders = dataProviders;

        this.colocationGroup = ColocationGroup.forNodes(List.copyOf(dataProviders.keySet()));
    }

    /**
     * Returns the data provider for the given node.
     *
     * @param nodeName Name of the node of interest.
     * @param <RowT> A type of the rows the data provider should produce.
     * @return A data provider for the node of interest.
     * @throws AssertionError in case data provider is not configured for the given node.
     */
    <RowT> DataProvider<RowT> dataProvider(String nodeName) {
        if (!dataProviders.containsKey(nodeName)) {
            throw new AssertionError(format(DATA_PROVIDER_NOT_CONFIGURED_MESSAGE_TEMPLATE, name, nodeName));
        }

        return (DataProvider<RowT>) dataProviders.get(nodeName);
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public int version() {
        return 0;
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
        return IgniteLogicalTableScan.create(cluster, cluster.traitSet(), hints, relOptTbl, proj, cond, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogicalIndexScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTbl,
            String idxName,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        return IgniteLogicalIndexScan.create(cluster, cluster.traitSet(), relOptTbl, idxName, proj, cond, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet bitSet) {
        return descriptor.rowType((IgniteTypeFactory) typeFactory, bitSet);
    }

    /** {@inheritDoc} */
    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            /** {@inheritDoc} */
            @Override
            public Double getRowCount() {
                return rowCnt;
            }

            /** {@inheritDoc} */
            @Override
            public boolean isKey(ImmutableBitSet cols) {
                return false;
            }

            /** {@inheritDoc} */
            @Override
            public List<ImmutableBitSet> getKeys() {
                throw new AssertionError();
            }

            /** {@inheritDoc} */
            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                throw new AssertionError();
            }

            /** {@inheritDoc} */
            @Override
            public List<RelCollation> getCollations() {
                return Collections.emptyList();
            }

            /** {@inheritDoc} */
            @Override
            public RelDistribution getDistribution() {
                throw new AssertionError();
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public Schema.TableType getJdbcTableType() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isRolledUp(String col) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean rolledUpColumnValidInsideAgg(
            String column,
            SqlCall call,
            SqlNode parent,
            CalciteConnectionConfig config
    ) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public ColocationGroup colocationGroup(MappingQueryContext ctx) {
        return colocationGroup;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return descriptor.distribution();
    }

    /** {@inheritDoc} */
    @Override
    public TableDescriptor descriptor() {
        return descriptor;
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
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public InternalTable table() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow row, RowFactory<RowT> factory,
            @Nullable BitSet requiredColumns) {
        throw new AssertionError();
    }
}
