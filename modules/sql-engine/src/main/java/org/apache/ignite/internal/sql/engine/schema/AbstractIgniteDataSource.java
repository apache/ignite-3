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

import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;


/**
 * Base implementation of {@link IgniteDataSource}.
 */
public abstract class AbstractIgniteDataSource extends AbstractTable implements IgniteDataSource {

    private final String name;

    private final TableDescriptor desc;

    private final int id;

    private final int version;

    private final Statistic statistic;

    /** Constructor. */
    public AbstractIgniteDataSource(String name, int id,  int version, TableDescriptor desc,
            Statistic statistic) {

        this.id = id;
        this.name = name;
        this.desc = desc;
        this.version = version;
        this.statistic = statistic;
    }

    /** {@inheritDoc} */
    @Override
    public int id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public int version() {
        return version;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public TableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return getRowType(typeFactory, null);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns) {
        return desc.rowType((IgniteTypeFactory) typeFactory, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public final TableScan toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        RelTraitSet traitSet = cluster.traitSetOf(distribution());
        return toRel(cluster, traitSet, relOptTable, context.getTableHints());
    }

    /** Converts this data source to a scan expression. */
    protected abstract TableScan toRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable relOptTbl, List<RelHint> hints);

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return desc.distribution();
    }

    /** {@inheritDoc} */
    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
            @Nullable SqlNode parent,
            @Nullable CalciteConnectionConfig config) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Statistic getStatistic() {
        return statistic;
    }

    /** {@inheritDoc} */
    @Override
    public <C> @Nullable C unwrap(Class<C> cls) {
        if (cls.isInstance(desc)) {
            return cls.cast(desc);
        }
        return super.unwrap(cls);
    }
}
