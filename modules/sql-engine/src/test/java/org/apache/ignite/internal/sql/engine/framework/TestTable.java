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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchemaIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/**
 * A test table that implements all the necessary for the optimizer methods to be used to prepare a query, as well as provides access to the
 * data to use this table in execution-related scenarios.
 */
public class TestTable implements IgniteTable {
    private static final String DATA_PROVIDER_NOT_CONFIGURED_MESSAGE_TEMPLATE =
            "DataProvider is not configured [table={}, node={}]";

    private static final AtomicInteger ID = new AtomicInteger();

    private final int id = ID.incrementAndGet();
    private final Map<String, IgniteSchemaIndex> indexes;

    private final String name;
    private final double rowCnt;
    private final TableDescriptor descriptor;
    private final Map<String, DataProvider<?>> dataProviders;


    /** Constructor. */
    public TestTable(
            TableDescriptor descriptor,
            String name,
            double rowCnt,
            Map<String, IgniteSchemaIndex> indexMap
    ) {
        this(descriptor, name, rowCnt, indexMap, Map.of());
    }

    /** Constructor. */
    public TestTable(
            TableDescriptor descriptor,
            String name,
            double rowCnt,
            Map<String, IgniteSchemaIndex> indexMap,
            Map<String, DataProvider<?>> dataProviders
    ) {
        this.descriptor = descriptor;
        this.name = name;
        this.rowCnt = rowCnt;
        this.dataProviders = dataProviders;
        indexes = indexMap;
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
    public int id() {
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
        return new TestStatistic(rowCnt);
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
    public Map<String, IgniteSchemaIndex> indexes() {
        return indexes;
    }

    /** {@inheritDoc} */
    @Override
    public void addIndex(IgniteIndex idxTbl) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@inheritDoc} */
    @Override
    public IgniteIndex getIndex(String idxName) {
        throw new UnsupportedOperationException("Not implemented yet");
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
}
