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

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.native2relationalType;
import static org.apache.ignite.internal.util.IgniteUtils.newHashMap;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.jetbrains.annotations.Nullable;

/**
 * TableDescriptorImpl.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class TableDescriptorImpl extends NullInitializerExpressionFactory implements TableDescriptor {
    private static final ColumnDescriptor[] DUMMY = new ColumnDescriptor[0];

    private final ColumnDescriptor[] descriptors;

    private final Map<String, ColumnDescriptor> descriptorsMap;

    private final IgniteDistribution distribution;

    private final RelDataType rowType;

    /**
     * Constructor.
     *
     * @param columnDescriptors Column descriptors.
     * @param distribution Distribution specification.
     */
    public TableDescriptorImpl(List<ColumnDescriptor> columnDescriptors, IgniteDistribution distribution) {
        this.distribution = distribution;

        Map<String, ColumnDescriptor> descriptorsMap = newHashMap(columnDescriptors.size());

        RelDataTypeFactory factory = Commons.typeFactory();
        RelDataTypeFactory.Builder typeBuilder = new RelDataTypeFactory.Builder(factory);
        for (ColumnDescriptor descriptor : columnDescriptors) {
            typeBuilder.add(descriptor.name(), deriveLogicalType(factory, descriptor));
            descriptorsMap.put(descriptor.name(), descriptor);
        }

        this.descriptors = columnDescriptors.toArray(DUMMY);
        this.descriptorsMap = descriptorsMap;

        this.rowType = typeBuilder.build();
    }

    @Override
    public Iterator<ColumnDescriptor> iterator() {
        return Arrays.stream(descriptors).iterator();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return distribution;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnStrategy generationStrategy(RelOptTable tbl, int colIdx) {
        if (descriptors[colIdx].defaultStrategy() != DefaultValueStrategy.DEFAULT_NULL) {
            return ColumnStrategy.DEFAULT;
        }

        return super.generationStrategy(tbl, colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public RexNode newColumnDefaultValue(RelOptTable tbl, int colIdx, InitializerContext ctx) {
        var descriptor = descriptors[colIdx];
        var rexBuilder = ctx.getRexBuilder();

        switch (descriptor.defaultStrategy()) {
            case DEFAULT_NULL: {
                final RelDataType fieldType = tbl.getRowType().getFieldList().get(colIdx).getType();

                return rexBuilder.makeNullLiteral(fieldType);
            }
            case DEFAULT_CONSTANT: {
                Class<?> storageType = Commons.nativeTypeToClass(descriptor.physicalType());
                Object defaultVal = descriptor.defaultValue();
                Object internalValue = TypeUtils.toInternal(defaultVal, storageType);
                RelDataType relDataType = deriveLogicalType(rexBuilder.getTypeFactory(), descriptor);

                return rexBuilder.makeLiteral(internalValue, relDataType, false);
            }
            case DEFAULT_COMPUTED: {
                assert descriptor.key() : "DEFAULT_COMPUTED is only supported for primary key columns. Column: " + descriptor.name();

                return rexBuilder.makeCall(IgniteSqlOperatorTable.GEN_RANDOM_UUID);
            }
            default:
                throw new IllegalStateException("Unknown default strategy: " + descriptor.defaultStrategy());
        }
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType rowType(IgniteTypeFactory factory, @Nullable ImmutableBitSet usedColumns) {
        if (usedColumns == null || usedColumns.cardinality() == descriptors.length) {
            return rowType;
        } else {
            return new RelDataTypeFactory.Builder(factory).addAll(rowType.getFieldList().stream()
                    .filter(field -> usedColumns.get(field.getIndex()))
                    .collect(Collectors.toList())).build();
        }
    }

    /** {@inheritDoc} */
    @Override
    public ColumnDescriptor columnDescriptor(String fieldName) {
        return fieldName == null ? null : descriptorsMap.get(fieldName);
    }

    /** {@inheritDoc} */
    @Override
    public ColumnDescriptor columnDescriptor(int idx) {
        return idx < 0 || idx >= descriptors.length ? null : descriptors[idx];
    }

    /** {@inheritDoc} */
    @Override
    public int columnsCount() {
        return descriptors.length;
    }

    private RelDataType deriveLogicalType(RelDataTypeFactory factory, ColumnDescriptor desc) {
        return native2relationalType(factory, desc.physicalType(), desc.nullable());
    }
}
