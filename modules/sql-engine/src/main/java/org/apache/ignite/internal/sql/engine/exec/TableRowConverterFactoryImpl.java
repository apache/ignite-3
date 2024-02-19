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

import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.type.NativeType;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of {@link TableRowConverterFactory}.
 */
public class TableRowConverterFactoryImpl implements TableRowConverterFactory {
    private final SchemaRegistry schemaRegistry;
    private final SchemaDescriptor schemaDescriptor;
    private final BinaryTupleSchema fullTupleSchema;
    private final TableRowConverter fullRowConverter;
    private final int[] fullRowColocationIndexes;
    private final int[] keyOnlyColocationIndexes;
    private final NativeType[] colocationColumnTypes;

    /**
     * Creates a factory from given schema and indexes of primary key.
     *
     * @param primaryKeyLogicalIndexes Indexes of a primary key column in a logical order. Used to
     *      properly build key only rows.
     * @param schemaRegistry Registry of all schemas known so far. Used in case table returned
     *      a row in older version than required to make an upgrade.
     * @param schemaDescriptor Actual schema descriptor. Used as a target schema to convert
     *      rows from sql format to one accepted by underlying table.
     */
    public TableRowConverterFactoryImpl(
            ImmutableIntList primaryKeyLogicalIndexes,
            SchemaRegistry schemaRegistry,
            SchemaDescriptor schemaDescriptor
    ) {
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaDescriptor;
        this.fullTupleSchema = BinaryTupleSchema.createRowSchema(schemaDescriptor);
        this.fullRowColocationIndexes = deriveFullRowColocationColumnIndexes(schemaDescriptor);
        this.keyOnlyColocationIndexes = deriveKeyOnlyColocationColumnIndexes(
                primaryKeyLogicalIndexes, schemaDescriptor
        );
        this.colocationColumnTypes = Arrays.stream(schemaDescriptor.colocationColumns())
                .map(Column::type)
                .toArray(NativeType[]::new);

        fullRowConverter = new TableRowConverterImpl(
                schemaRegistry,
                fullTupleSchema,
                fullRowColocationIndexes,
                keyOnlyColocationIndexes,
                colocationColumnTypes,
                schemaDescriptor,
                null
        );
    }

    private static int[] deriveFullRowColocationColumnIndexes(SchemaDescriptor descriptor) {
        Column[] colocationColumns = descriptor.colocationColumns();
        int[] result = new int[colocationColumns.length];

        int idx = 0;
        for (Column column : colocationColumns) {
            result[idx++] = column.schemaIndex();
        }

        return result;
    }

    private static int[] deriveKeyOnlyColocationColumnIndexes(
            ImmutableIntList primaryKeyLogicalIndexes,
            SchemaDescriptor descriptor
    ) {
        List<Column> columns = descriptor.columns().stream()
                .sorted(Comparator.comparingInt(Column::columnOrder))
                .collect(Collectors.toList());

        int[] mapping = new int[columns.size()];
        Arrays.fill(mapping, -1);

        int idx = 0;
        for (int keyColumnIdx : primaryKeyLogicalIndexes) {
            Column column = columns.get(keyColumnIdx);

            mapping[column.schemaIndex()] = idx++;
        }

        Column[] colocationColumns = descriptor.colocationColumns();
        int[] result = new int[colocationColumns.length];

        idx = 0;
        for (Column column : colocationColumns) {
            int remappedIndex = mapping[column.schemaIndex()];

            assert remappedIndex != -1
                    : "Columns=" + columns
                    + ", keyColumns=" + primaryKeyLogicalIndexes
                    + ", colocationColumn=" + column;

            result[idx++] = remappedIndex;
        }

        return result;
    }

    @Override
    public TableRowConverter create(@Nullable BitSet requiredColumns) {
        if (requiredColumns == null || requiredColumns.cardinality() == schemaDescriptor.length()) {
            return fullRowConverter;
        }

        return new TableRowConverterImpl(
                schemaRegistry,
                fullTupleSchema,
                fullRowColocationIndexes,
                keyOnlyColocationIndexes,
                colocationColumnTypes,
                schemaDescriptor,
                requiredColumns
        );
    }
}
