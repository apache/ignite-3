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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.BitSet;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.util.FieldDeserializingProjectedTuple;
import org.apache.ignite.internal.sql.engine.util.FormatAwareProjectedTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Converts rows to execution engine representation.
 */
public class TableRowConverterImpl implements TableRowConverter {

    private final SchemaRegistry schemaRegistry;

    private final SchemaDescriptor schemaDescriptor;

    private final BinaryTupleSchema fullTupleSchema;

    private final BinaryTupleSchema keyTupleSchema;

    /**
     * Mapping of required columns to their indexes in physical schema.
     */
    private final int[] requiredColumnsMapping;

    private final int[] colocationKeysFullRowMapping;

    private final int[] colocationKeysKeyOnlyMapping;

    private final boolean skipReshuffling;

    /** Constructor. */
    TableRowConverterImpl(
            SchemaRegistry schemaRegistry,
            SchemaDescriptor schemaDescriptor,
            @Nullable BitSet requiredColumns
    ) {
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaDescriptor;

        this.fullTupleSchema = BinaryTupleSchema.createRowSchema(schemaDescriptor);
        this.keyTupleSchema = BinaryTupleSchema.createKeySchema(schemaDescriptor);

        this.skipReshuffling = requiredColumns == null;

        int elementCount = schemaDescriptor.length();
        int size = requiredColumns == null ? elementCount : requiredColumns.cardinality();

        requiredColumnsMapping = new int[size];

        int idx = 0;
        for (Column column : schemaDescriptor.columns()) {
            if (requiredColumns == null || requiredColumns.get(column.order())) {
                requiredColumnsMapping[idx++] = column.order();
            }
        }

        Int2IntMap columnOrderToPositionInKey = new Int2IntOpenHashMap(schemaDescriptor.keyColumns().size());
        idx = 0;
        for (Column column : schemaDescriptor.keyColumns()) {
            columnOrderToPositionInKey.put(column.order(), idx++);
        }

        colocationKeysFullRowMapping = new int[schemaDescriptor.fullRowColocationColumns().size()];
        colocationKeysKeyOnlyMapping = new int[schemaDescriptor.fullRowColocationColumns().size()];
        idx = 0;
        for (Column column : schemaDescriptor.fullRowColocationColumns()) {
            colocationKeysFullRowMapping[idx] = column.order();
            colocationKeysKeyOnlyMapping[idx++] = columnOrderToPositionInKey.get(column.order());
        }
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> BinaryRowEx toBinaryRow(ExecutionContext<RowT> ectx, RowT row, boolean key) {
        BinaryTuple binaryTuple = ectx.rowHandler().toBinaryTuple(row);

        if (!key) {
            return SqlOutputBinaryRow.newRow(schemaDescriptor, binaryTuple, colocationKeysFullRowMapping, fullTupleSchema);
        } else {
            return SqlOutputBinaryRow.newRow(schemaDescriptor, binaryTuple, colocationKeysKeyOnlyMapping, keyTupleSchema);
        }
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> RowT toRow(
            ExecutionContext<RowT> ectx,
            BinaryRow tableRow,
            RowHandler.RowFactory<RowT> factory
    ) {
        InternalTuple tuple;
        if (tableRow.schemaVersion() == schemaDescriptor.version()) {
            InternalTuple tableTuple = new BinaryTuple(schemaDescriptor.length(), tableRow.tupleSlice());

            tuple = skipReshuffling
                    ? tableTuple
                    : new FormatAwareProjectedTuple(tableTuple, requiredColumnsMapping);
        } else {
            InternalTuple tableTuple = schemaRegistry.resolve(tableRow, schemaDescriptor);

            tuple = new FieldDeserializingProjectedTuple(
                    fullTupleSchema,
                    tableTuple,
                    requiredColumnsMapping
            );
        }

        return factory.create(tuple);
    }
}
