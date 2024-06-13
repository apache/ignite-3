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

import java.util.BitSet;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of {@link TableRowConverterFactory}.
 */
public class TableRowConverterFactoryImpl implements TableRowConverterFactory {
    private final SchemaRegistry schemaRegistry;
    private final SchemaDescriptor schemaDescriptor;
    private final BinaryTupleSchema fullTupleSchema;
    private final TableRowConverter fullRowConverter;
    private int partVirtualColumnIndex = -1;

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
        this.fullTupleSchema = BinaryTupleSchema.createRowSchema(schemaDescriptor);

        fullRowConverter = new TableRowConverterImpl(
                schemaRegistry,
                schemaDescriptor
        );

        ColumnDescriptor columnDescriptor = tableDescriptor.columnDescriptor(Commons.PART_COL_NAME);

        if (columnDescriptor != null) {
            assert columnDescriptor.system();

            partVirtualColumnIndex = columnDescriptor.logicalIndex();
        }
    }

    @Override
    public TableRowConverter create(@Nullable BitSet requiredColumns) {
        return create(requiredColumns, -1);
    }

    @Override
    public TableRowConverter create(@Nullable BitSet requiredColumns, int partId) {
        boolean hasVirtualColumn = requiredColumns != null && partVirtualColumnIndex != -1 && requiredColumns.get(partVirtualColumnIndex);

        if (requiredColumns == null || (requiredColumns.cardinality() == schemaDescriptor.length() && !hasVirtualColumn)) {
            return fullRowConverter;
        }

        return new ProjectedTableRowConverterImpl(
                schemaRegistry,
                fullTupleSchema,
                schemaDescriptor,
                requiredColumns,
                hasVirtualColumn ? new VirtualColumn(partVirtualColumnIndex, partId) : null
        );
    }
}
