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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;

/**
 * TableDescriptorImpl.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class TableDescriptorImpl extends NullInitializerExpressionFactory implements TableDescriptor {
    private static final ColumnDescriptor[] DUMMY = new ColumnDescriptor[0];

    private final ColumnDescriptor[] descriptors;

    private final Map<String, ColumnDescriptor> descriptorsMap;

    private final ImmutableBitSet insertFields;

    private final ImmutableBitSet keyFields;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public TableDescriptorImpl(
            List<ColumnDescriptor> columnDescriptors
    ) {
        ImmutableBitSet.Builder keyFieldsBuilder = ImmutableBitSet.builder();

        Map<String, ColumnDescriptor> descriptorsMap = new HashMap<>(columnDescriptors.size());
        boolean implicitKeyFound = false;

        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (ColumnDescriptor descriptor : columnDescriptors) {
            if (descriptor.key()) {
                keyFieldsBuilder.set(descriptor.logicalIndex());
            }

            if (Commons.implicitPkEnabled() && Commons.IMPLICIT_PK_COL_NAME.equals(descriptor.name())) {
                assert !implicitKeyFound;

                implicitKeyFound = true;
                descriptor = injectDefault(descriptor);
            } else {
                builder.set(descriptor.logicalIndex());
            }

            descriptorsMap.put(descriptor.name(), descriptor);
        }

        if (implicitKeyFound) {
            columnDescriptors = columnDescriptors.stream().map(desc -> descriptorsMap.get(desc.name())).collect(Collectors.toList());
        }

        this.descriptors = columnDescriptors.toArray(DUMMY);
        this.descriptorsMap = descriptorsMap;

        insertFields = builder.build();
        keyFields = keyFieldsBuilder.build();
    }

    private ColumnDescriptor injectDefault(ColumnDescriptor desc) {
        assert Commons.implicitPkEnabled() && Commons.IMPLICIT_PK_COL_NAME.equals(desc.name()) : desc;

        return new ColumnDescriptorImpl(
                desc.name(),
                desc.key(),
                desc.logicalIndex(),
                desc.physicalIndex(),
                desc.physicalType(),
                null
        ) {
            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return UUID.randomUUID().toString();
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType insertRowType(IgniteTypeFactory factory) {
        return rowType(factory, insertFields);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType deleteRowType(IgniteTypeFactory factory) {
        return rowType(factory, keyFields);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return IgniteDistributions.random();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
        return !descriptors[colIdx].key();
    }

    /** {@inheritDoc} */
    @Override
    public ColumnStrategy generationStrategy(RelOptTable tbl, int colIdx) {
        if (descriptors[colIdx].hasDefaultValue()) {
            return ColumnStrategy.DEFAULT;
        }

        return super.generationStrategy(tbl, colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public RexNode newColumnDefaultValue(RelOptTable tbl, int colIdx, InitializerContext ctx) {
        final ColumnDescriptor desc = descriptors[colIdx];

        if (!desc.hasDefaultValue()) {
            return super.newColumnDefaultValue(tbl, colIdx, ctx);
        }

        final RexBuilder rexBuilder = ctx.getRexBuilder();
        final IgniteTypeFactory typeFactory = (IgniteTypeFactory) rexBuilder.getTypeFactory();

        return rexBuilder.makeLiteral(desc.defaultValue(), desc.logicalType(typeFactory), false);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType rowType(IgniteTypeFactory factory, ImmutableBitSet usedColumns) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(factory);

        if (usedColumns == null) {
            for (int i = 0; i < descriptors.length; i++) {
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
            }
        } else {
            for (int i = usedColumns.nextSetBit(0); i != -1; i = usedColumns.nextSetBit(i + 1)) {
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
            }
        }

        return TypeUtils.sqlType(factory, b.build());
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
}
