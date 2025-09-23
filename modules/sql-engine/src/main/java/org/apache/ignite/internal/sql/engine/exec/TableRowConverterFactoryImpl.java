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

package org.apache.ignite.internal.sql.engine.exec;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of {@link TableRowConverterFactory}.
 */
public class TableRowConverterFactoryImpl implements TableRowConverterFactory {
    private final SchemaRegistry schemaRegistry;
    private final SchemaDescriptor schemaDescriptor;
    private final TableRowConverter fullRowConverter;
    private final int[] tableColumnSet;
    private final Int2ObjectArrayMap<IntFunction<VirtualColumn>> virtualColumnsFactory = new Int2ObjectArrayMap<>();

    /**
     * Creates a factory from given schema and indexes of primary key.
     *
     * @param tableDescriptor Table descriptor.
     * @param schemaRegistry Registry of all schemas known so far. Used in case table returned
     *      a row in older version than required to make an upgrade.
     * @param schemaDescriptor Actual schema descriptor. Used as a target schema to convert
     *      rows from sql format to one accepted by underlying table.
     */
    public TableRowConverterFactoryImpl(
            TableDescriptor tableDescriptor,
            SchemaRegistry schemaRegistry,
            SchemaDescriptor schemaDescriptor
    ) {
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaDescriptor;

        fullRowConverter = new TableRowConverterImpl(
                schemaRegistry,
                schemaDescriptor
        );

        tableColumnSet = IntStream.range(0, tableDescriptor.columnsCount()).toArray();

        addVirtualColumn(tableDescriptor.columnDescriptor(Commons.PART_COL_NAME));
        addVirtualColumn(tableDescriptor.columnDescriptor(Commons.PART_COL_NAME_LEGACY));
    }

    private void addVirtualColumn(@Nullable ColumnDescriptor columnDescriptor) {
        if (columnDescriptor == null) {
            return;
        }

        assert columnDescriptor.virtual();

        int columnIndex = columnDescriptor.logicalIndex();

        virtualColumnsFactory.put(columnIndex, (partId) -> new VirtualColumn(columnIndex, NativeTypes.INT32, false, partId));
    }

    @Override
    public TableRowConverter create(int @Nullable [] projection) {
        // TODO: IGNITE-22823 fix this. UpdatableTable must pass the project with updatable columns.
        if (projection == null) {
            return fullRowConverter;
        }

        return create(projection, -1);
    }

    @Override
    public TableRowConverter create(int @Nullable [] projection, int partId) {
        int[] mapping = projection == null
                ? tableColumnSet
                : projection;

        if (Commons.isIdentityMapping(mapping, schemaDescriptor.length())) {
            return fullRowConverter;
        }

        Int2ObjectMap<VirtualColumn> extraColumns = createVirtualColumns(mapping, partId);

        return new ProjectedTableRowConverterImpl(
                schemaRegistry,
                schemaDescriptor,
                mapping,
                extraColumns
        );
    }

    private Int2ObjectMap<VirtualColumn> createVirtualColumns(int[] requiredColumns, int partId) {
        if (virtualColumnsFactory.isEmpty()) {
            return Int2ObjectMaps.emptyMap();
        }

        Int2ObjectMap<VirtualColumn> columnsMap = new Int2ObjectArrayMap<>(virtualColumnsFactory.size());

        for (int i : requiredColumns) {
            if (i >= schemaDescriptor.length()) {
                columnsMap.put(i, virtualColumnsFactory.get(i).apply(partId));
            }
        }

        return columnsMap;
    }
}
