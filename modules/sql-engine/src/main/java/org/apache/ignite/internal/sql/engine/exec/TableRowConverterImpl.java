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

    /**
     * Mapping of required columns to their indexes in physical schema.
     */
    private final int[] requiredColumnsMapping;

    private final boolean skipTrimming;

    /** Constructor. */
    TableRowConverterImpl(
            SchemaRegistry schemaRegistry,
            BinaryTupleSchema fullTupleSchema,
            SchemaDescriptor schemaDescriptor,
            @Nullable BitSet requiredColumns
    ) {
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaDescriptor;
        this.fullTupleSchema = fullTupleSchema;

        this.skipTrimming = requiredColumns == null;

        int size = requiredColumns == null
                ? schemaDescriptor.length()
                : requiredColumns.cardinality();

        requiredColumnsMapping = new int[size];

        int requiredIndex = 0;
        for (Column column : schemaDescriptor.columns()) {
            if (requiredColumns == null || requiredColumns.get(column.positionInRow())) {
                requiredColumnsMapping[requiredIndex++] = column.positionInRow();
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> BinaryRowEx toBinaryRow(ExecutionContext<RowT> ectx, RowT row, boolean key) {
        BinaryTuple binaryTuple = ectx.rowHandler().toBinaryTuple(row);

        return SqlOutputBinaryRow.newRow(schemaDescriptor, key, binaryTuple);
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

            tuple = skipTrimming
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
