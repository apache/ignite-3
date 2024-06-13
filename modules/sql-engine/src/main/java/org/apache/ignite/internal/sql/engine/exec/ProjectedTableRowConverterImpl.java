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
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.util.FieldDeserializingProjectedTuple;
import org.apache.ignite.internal.sql.engine.util.FormatAwareProjectedTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Converts rows to execution engine representation.
 */
public class ProjectedTableRowConverterImpl extends TableRowConverterImpl {
    /**
     * Mapping of required columns to their indexes in physical schema.
     */
    private final int[] requiredColumnsMapping;

    private final BinaryTupleSchema fullTupleSchema;

    private final VirtualColumn virtualColumn;

    /** Constructor. */
    ProjectedTableRowConverterImpl(
            SchemaRegistry schemaRegistry,
            BinaryTupleSchema fullTupleSchema,
            SchemaDescriptor schemaDescriptor,
            BitSet requiredColumns,
            @Nullable VirtualColumn virtualColumn
    ) {
        super(schemaRegistry, schemaDescriptor);

        this.fullTupleSchema = fullTupleSchema;
        this.virtualColumn = virtualColumn;

        int size = requiredColumns.cardinality();

        requiredColumnsMapping = new int[size];

        int requiredIndex = 0;
        for (Column column : schemaDescriptor.columns()) {
            if (requiredColumns.get(column.positionInRow())) {
                requiredColumnsMapping[requiredIndex++] = column.positionInRow();
            }
        }

        if (virtualColumn != null) {
            requiredColumnsMapping[requiredIndex] = virtualColumn.columnIndex;
        }
    }

    @Override
    public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow tableRow, RowFactory<RowT> factory) {
        InternalTuple tuple;
        boolean rowSchemaMatches = tableRow.schemaVersion() == schemaDescriptor.version();

        InternalTuple tableTuple = rowSchemaMatches
                ? new BinaryTuple(schemaDescriptor.length(), tableRow.tupleSlice())
                : schemaRegistry.resolve(tableRow, schemaDescriptor);

        if (virtualColumn != null) {
            tuple = injectVirtualColumn(tableTuple);
        } else if (rowSchemaMatches) {
            tuple = new FormatAwareProjectedTuple(tableTuple, requiredColumnsMapping);
        } else {
            tuple = new FieldDeserializingProjectedTuple(fullTupleSchema, tableTuple, requiredColumnsMapping);
        }

        return factory.create(tuple);
    }

    private InternalTuple injectVirtualColumn(InternalTuple tableTuple) {
        return new FieldDeserializingProjectedTuple(
                fullTupleSchema,
                tableTuple,
                requiredColumnsMapping
        ) {
            @Override
            public int intValue(int col) {
                if (projection[col] == virtualColumn.columnIndex) {
                    return (Integer) virtualColumn.value;
                }

                return super.intValue(col);
            }

            @Override
            public Integer intValueBoxed(int col) {
                if (projection[col] == virtualColumn.columnIndex) {
                    return (Integer) virtualColumn.value;
                }

                return super.intValueBoxed(col);
            }

            @Override
            protected void normalize() {
                var builder = new BinaryTupleBuilder(projection.length);
                var newProjection = new int[projection.length];

                for (int i = 0; i < projection.length; i++) {
                    int col = projection[i];

                    newProjection[i] = i;

                    if (col == virtualColumn.columnIndex) {
                        builder.appendInt((Integer) virtualColumn.value);
                        continue;
                    }

                    Element element = schema.element(col);

                    BinaryRowConverter.appendValue(builder, element, schema.value(delegate, col));
                }

                delegate = new BinaryTuple(projection.length, builder.build());
                projection = newProjection;
            }
        };
    }
}
