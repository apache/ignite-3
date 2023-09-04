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
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Converts rows to execution engine representation.
 */
public class TableRowConverterImpl implements TableRowConverter {

    private final SchemaRegistry schemaRegistry;

    private final SchemaDescriptor schemaDescriptor;

    private final TableDescriptor desc;

    private final BinaryTupleSchema binaryTupleSchema;

    /** Constructor. */
    public TableRowConverterImpl(SchemaRegistry schemaRegistry, SchemaDescriptor schemaDescriptor, TableDescriptor desc) {
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaDescriptor;
        this.desc = desc;

        this.binaryTupleSchema = BinaryTupleSchema.createRowSchema(schemaDescriptor);
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> RowT toRow(
            ExecutionContext<RowT> ectx,
            BinaryRow binaryRow,
            RowHandler.RowFactory<RowT> factory,
            @Nullable BitSet requiredColumns
    ) {
        Row row = schemaRegistry.resolve(binaryRow, schemaDescriptor);

        BinaryTupleBuilder builder = requiredColumns == null
                ? allColumnsTuple(row, binaryTupleSchema)
                : requiredColumnsTuple(row, binaryTupleSchema, requiredColumns);

        return factory.wrap(new BinaryTuple(builder.numElements(), builder.build()));
    }

    private BinaryTupleBuilder allColumnsTuple(Row row, BinaryTupleSchema binarySchema) {
        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(desc.columnsCount());

        for (int i = 0; i < desc.columnsCount(); i++) {
            int index = desc.columnDescriptor(i).physicalIndex();

            BinaryRowConverter.appendValue(tupleBuilder, binarySchema.element(index), binarySchema.value(row, index));
        }

        return tupleBuilder;
    }

    private BinaryTupleBuilder requiredColumnsTuple(Row row, BinaryTupleSchema binarySchema, BitSet requiredColumns) {
        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(requiredColumns.cardinality());

        for (int i = requiredColumns.nextSetBit(0); i != -1; i = requiredColumns.nextSetBit(i + 1)) {
            int index = desc.columnDescriptor(i).physicalIndex();

            BinaryRowConverter.appendValue(tupleBuilder, binarySchema.element(index), binarySchema.value(row, index));
        }

        return tupleBuilder;
    }
}
