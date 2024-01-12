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
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
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
            TableDescriptor descriptor,
            @Nullable BitSet requiredColumns
    ) {
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaDescriptor;

        this.binaryTupleSchema = BinaryTupleSchema.createRowSchema(schemaDescriptor);

        int size = requiredColumns == null ? descriptor.columnsCount() : requiredColumns.cardinality();

        requiredColumnsMapping = new int[size];
        fullMapping = new int[descriptor.columnsCount()];
        keyMapping = new int[schemaDescriptor.keyColumns().length()];

        for (int i = 0, j = 0, k = 0; i < descriptor.columnsCount(); i++) {
            ColumnDescriptor column = descriptor.columnDescriptor(i);
            int schemaIndex = schemaDescriptor.column(column.name()).schemaIndex();

            fullMapping[schemaIndex] = i;

            if (column.key()) {
                keyMapping[j++] = schemaIndex;
            }

            if (requiredColumns == null || requiredColumns.get(column.logicalIndex())) {
                requiredColumnsMapping[k++] = schemaIndex;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> BinaryRowEx toBinaryRow(ExecutionContext<RowT> ectx, RowT row, boolean key) {
        BinaryTuple binaryTuple = ectx.rowHandler().toBinaryTuple(row);

        if (!key) {
            FormatAwareProjectedTuple tuple = new FormatAwareProjectedTuple(binaryTuple, fullMapping);
            BinaryRowImpl binaryRow = new BinaryRowImpl(schemaDescriptor.version(), tuple.byteBuffer());

            return Row.wrapBinaryRow(schemaDescriptor, binaryRow);
        } else {
            FormatAwareProjectedTuple tuple = new FormatAwareProjectedTuple(binaryTuple, keyMapping);
            BinaryRowImpl binaryRow = new BinaryRowImpl(schemaDescriptor.version(), tuple.byteBuffer());

            return Row.wrapKeyOnlyBinaryRow(schemaDescriptor, binaryRow);
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
