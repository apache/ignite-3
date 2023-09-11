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
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.ProjectedTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Converts rows to execution engine representation.
 */
public class TableRowConverterImpl implements TableRowConverter {
    private final SchemaRegistry schemaRegistry;

    private final SchemaDescriptor schemaDescriptor;

    private final BinaryTupleSchema binaryTupleSchema;

    private final int[] mapping;

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

        mapping = new int[size];

        int currentIdx = 0;
        for (ColumnDescriptor column : descriptor) {
            if (requiredColumns != null && !requiredColumns.get(column.logicalIndex())) {
                continue;
            }

            mapping[currentIdx++] = column.physicalIndex();
        }
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> RowT toRow(
            ExecutionContext<RowT> ectx,
            BinaryRow tableRow,
            RowHandler.RowFactory<RowT> factory
    ) {
        InternalTuple tableTuple;
        if (tableRow.schemaVersion() == schemaDescriptor.version()) {
            tableTuple = new BinaryTuple(schemaDescriptor.length(), tableRow.tupleSlice());
        } else {
            tableTuple = schemaRegistry.resolve(tableRow, schemaDescriptor);
        }

        InternalTuple tuple = new ProjectedTuple(
                binaryTupleSchema,
                tableTuple,
                mapping
        );

        return factory.create(tuple);
    }
}
