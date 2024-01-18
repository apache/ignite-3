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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

    private final BinaryTupleSchema binaryTupleSchema;

    private final BinaryTupleSchema keyTupleSchema;

    /**
     * Mapping of required columns to their indexes in physical schema.
     */
    private final int[] requiredColumnsMapping;

    /**
     * Mapping of all columns to their indexes in physical schema.
     */
    private final int[] fullMapping;

    /**
     * Mapping of key column indexes to its ordinals in the ordered list.
     * It is used during a delete operation to assemble key-only binary row from
     * "truncated" relational node row containing only primary key columns.
     */
    private final int[] keyMapping;

    /** Constructor. */
    TableRowConverterImpl(
            SchemaRegistry schemaRegistry,
            SchemaDescriptor schemaDescriptor,
            @Nullable BitSet requiredColumns
    ) {
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaDescriptor;

        this.binaryTupleSchema = BinaryTupleSchema.createRowSchema(schemaDescriptor);
        this.keyTupleSchema = BinaryTupleSchema.createKeySchema(schemaDescriptor);

        int size = requiredColumns == null ? schemaDescriptor.length() : requiredColumns.cardinality();

        requiredColumnsMapping = new int[size];
        fullMapping = new int[schemaDescriptor.length()];

        List<Map.Entry<Integer, Column>> tableOrder = new ArrayList<>(schemaDescriptor.length());

        for (int i = 0; i < schemaDescriptor.length(); i++) {
            Column column = schemaDescriptor.column(i);
            tableOrder.add(Map.entry(column.columnOrder(), column));
        }

        tableOrder.sort(Entry.comparingByKey());

        int keyIndex = 0;
        int requiredIndex = 0;
        keyMapping = new int[schemaDescriptor.keyColumns().length()];

        for (Map.Entry<Integer, Column> e : tableOrder) {
            Column column = e.getValue();
            int i = column.schemaIndex();

            fullMapping[i] = column.columnOrder();

            if (schemaDescriptor.isKeyColumn(i)) {
                keyMapping[keyIndex++] = i;
            }

            if (requiredColumns == null || requiredColumns.get(column.columnOrder())) {
                requiredColumnsMapping[requiredIndex++] = i;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> BinaryRowEx toBinaryRow(ExecutionContext<RowT> ectx, RowT row, boolean key) {
        BinaryTuple binaryTuple = ectx.rowHandler().toBinaryTuple(row);

        if (!key) {
            FormatAwareProjectedTuple tuple = new FormatAwareProjectedTuple(binaryTuple, fullMapping);
            return SqlOutputBinaryRow.newRow(tuple, schemaDescriptor, binaryTupleSchema);
        } else {
            FormatAwareProjectedTuple tuple = new FormatAwareProjectedTuple(binaryTuple, keyMapping);
            return SqlOutputBinaryRow.newRow(tuple, schemaDescriptor, keyTupleSchema);
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

            tuple = new FormatAwareProjectedTuple(
                    tableTuple,
                    requiredColumnsMapping
            );
        } else {
            InternalTuple tableTuple = schemaRegistry.resolve(tableRow, schemaDescriptor);

            tuple = new FieldDeserializingProjectedTuple(
                    binaryTupleSchema,
                    tableTuple,
                    requiredColumnsMapping
            );
        }

        return factory.create(tuple);
    }
}
