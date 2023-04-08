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

package org.apache.ignite.internal.sql.engine.table;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;

/**
 * TestTableDescriptor.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class TestTableDescriptor implements TableDescriptor {
    private final Supplier<IgniteDistribution> distributionSupp;

    private final RelDataType rowType;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    TestTableDescriptor(Supplier<IgniteDistribution> distribution, RelDataType rowType) {
        this.distributionSupp = distribution;
        this.rowType = rowType;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return distributionSupp.get();
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType rowType(IgniteTypeFactory factory, ImmutableBitSet usedColumns) {
        return rowType;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnDescriptor columnDescriptor(String fieldName) {
        RelDataTypeField field = rowType.getField(fieldName, false, false);

        NativeType nativeType = field.getType() instanceof BasicSqlType ? IgniteTypeFactory.relDataTypeToNative(field.getType()) : null;

        return new TestColumnDescriptor(field.getIndex(), fieldName, nativeType);
    }

    /** {@inheritDoc} */
    @Override
    public ColumnDescriptor columnDescriptor(int idx) {
        RelDataTypeField field = rowType.getFieldList().get(idx);

        NativeType nativeType = field.getType() instanceof BasicSqlType ? IgniteTypeFactory.relDataTypeToNative(field.getType()) : null;

        return new TestColumnDescriptor(field.getIndex(), field.getName(), nativeType);
    }

    /** {@inheritDoc} */
    @Override
    public int columnsCount() {
        return rowType.getFieldCount();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isGeneratedAlways(RelOptTable table, int idxColumn) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public ColumnStrategy generationStrategy(RelOptTable table, int idxColumn) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public RexNode newColumnDefaultValue(RelOptTable table, int idxColumn, InitializerContext context) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public BiFunction<InitializerContext, RelNode, RelNode> postExpressionConversionHook() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public RexNode newAttributeInitializer(RelDataType type, SqlFunction constructor, int idxAttribute,
            List<RexNode> constructorArgs, InitializerContext context) {
        throw new AssertionError();
    }
}
