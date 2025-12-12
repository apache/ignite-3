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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.Lazy;
import org.jetbrains.annotations.Nullable;

/**
 * Table implementation for sql engine.
 */
public class IgniteTableImpl extends AbstractIgniteDataSource implements IgniteTable {
    private final ImmutableIntList keyColumns;
    private final @Nullable ImmutableIntList columnsToInsert;
    private final @Nullable ImmutableIntList columnsToUpdate;

    private final Map<String, IgniteIndex> indexMap;

    private final int partitions;
    private final int zoneId;

    private final Lazy<NativeType[]> colocationColumnTypes;

    /** Constructor. */
    public IgniteTableImpl(
            String name,
            int id,
            int version,
            long timestamp,
            TableDescriptor desc,
            ImmutableIntList keyColumns,
            Statistic statistic,
            Map<String, IgniteIndex> indexMap,
            int partitions,
            int zoneId
    ) {
        super(name, id, version, timestamp, desc, statistic);

        this.keyColumns = keyColumns;
        this.indexMap = indexMap;
        this.partitions = partitions;
        this.zoneId = zoneId;
        this.columnsToInsert = deriveColumnsToInsert(desc);
        this.columnsToUpdate = deriveColumnsToUpdate(desc);

        colocationColumnTypes = new Lazy<>(this::evaluateTypes);
    }

    private static @Nullable ImmutableIntList deriveColumnsToInsert(TableDescriptor desc) {
        /*
        Columns to insert are columns which will be expanded in case user omit
        columns list in insert statement as in example below:

            INSERT INTO table VALUES (1, 1); -- mind omitted columns list after table identifier

        Although hidden columns are currently not supported by Ignite, we have special mode
        where we allow to omit primary key declaration during table creation. In that case, we
        inject the column that will serve as primary key, but this column must be hidden during
        star expansion (SELECT * FROM ... clause), as well as must be ignored during columns
        list inference for INSERT INTO statement.

        See org.apache.ignite.internal.sql.engine.util.Commons.implicitPkEnabled, and
        org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl.injectDefault for details.
         */
        if (!desc.hasHiddenColumns()) {
            return null; // 'null' means that full projection will be used for insert.
        }

        IntList columnsToInsert = new IntArrayList(desc.columnsCount());

        for (ColumnDescriptor columnDescriptor : desc) {
            if (!columnDescriptor.hidden()) {
                columnsToInsert.add(columnDescriptor.logicalIndex());
            }
        }

        return ImmutableIntList.of(columnsToInsert.toIntArray());
    }

    private static @Nullable ImmutableIntList deriveColumnsToUpdate(TableDescriptor desc) {
        if (!desc.hasVirtualColumns()) {
            return null; // 'null' means that full projection will be used for update.
        }

        IntList columnsToUpdate = new IntArrayList(desc.columnsCount());

        for (ColumnDescriptor columnDescriptor : desc) {
            if (!columnDescriptor.virtual()) {
                columnsToUpdate.add(columnDescriptor.logicalIndex());
            }
        }

//        return ImmutableIntList.of(columnsToUpdate.toIntArray());
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Supplier<PartitionCalculator> partitionCalculator() {
        return () -> new PartitionCalculator(partitions, Objects.requireNonNull(colocationColumnTypes.get()));
    }

    private NativeType[] evaluateTypes() {
        int fieldCnt = descriptor().distribution().getKeys().size();
        NativeType[] fieldTypes = new NativeType[fieldCnt];

        int[] colocationColumns = descriptor().distribution().getKeys().toIntArray();

        for (int i = 0; i < fieldCnt; i++) {
            ColumnDescriptor colDesc = descriptor().columnDescriptor(colocationColumns[i]);

            fieldTypes[i] = colDesc.physicalType();
        }

        return fieldTypes;
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, IgniteIndex> indexes() {
        return indexMap;
    }

    /** {@inheritDoc} */
    @Override
    public int partitions() {
        return partitions;
    }

    @Override
    public int zoneId() {
        return zoneId;
    }

    @Override
    public ImmutableIntList keyColumns() {
        return keyColumns;
    }

    /** {@inheritDoc} */
    @Override
    protected TableScan toRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable relOptTbl, List<RelHint> hints) {
        return IgniteLogicalTableScan.create(cluster, traitSet, hints, relOptTbl, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isUpdateAllowed(int colIdx) {
        ColumnDescriptor columnDescriptor = descriptor().columnDescriptor(colIdx);
        return !columnDescriptor.key() && !columnDescriptor.virtual();
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType rowTypeForInsert(IgniteTypeFactory factory) {
        return descriptor().rowType(factory, columnsToInsert);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType rowTypeForUpdate(IgniteTypeFactory factory) {
        return descriptor().rowType(factory, columnsToUpdate);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType rowTypeForDelete(IgniteTypeFactory factory) {
        return descriptor().rowType(factory, keyColumns);
    }
}
