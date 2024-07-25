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
import java.util.List;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.util.ExtendedFieldDeserializingProjectedTuple;
import org.apache.ignite.internal.sql.engine.util.FieldDeserializingProjectedTuple;
import org.apache.ignite.internal.sql.engine.util.FormatAwareProjectedTuple;

/**
 * Converts rows to execution engine representation.
 */
public class ProjectedTableRowConverterImpl extends TableRowConverterImpl {
    /**
     * Mapping of required columns to their indexes in physical schema.
     */
    private final int[] requiredColumnsMapping;

    private final BinaryTupleSchema fullTupleSchema;

    private final List<VirtualColumn> virtualColumns;

    /** Constructor. */
    ProjectedTableRowConverterImpl(
            SchemaRegistry schemaRegistry,
            BinaryTupleSchema fullTupleSchema,
            SchemaDescriptor schemaDescriptor,
            BitSet requiredColumns,
            List<VirtualColumn> extraColumns
    ) {
        super(schemaRegistry, schemaDescriptor);

        this.fullTupleSchema = fullTupleSchema;
        this.virtualColumns = extraColumns;

        int size = requiredColumns.cardinality();

        requiredColumnsMapping = new int[size];

        int requiredIndex = 0;
        for (Column column : schemaDescriptor.columns()) {
            if (requiredColumns.get(column.positionInRow())) {
                requiredColumnsMapping[requiredIndex++] = column.positionInRow();
            }
        }

        for (VirtualColumn col : extraColumns) {
            requiredColumnsMapping[requiredIndex++] = col.columnIndex();
        }
    }

    @Override
    public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow tableRow, RowFactory<RowT> factory) {
        InternalTuple tuple;
        boolean rowSchemaMatches = tableRow.schemaVersion() == schemaDescriptor.version();

        InternalTuple tableTuple = rowSchemaMatches
                ? new BinaryTuple(schemaDescriptor.length(), tableRow.tupleSlice())
                : schemaRegistry.resolve(tableRow, schemaDescriptor);

        if (!virtualColumns.isEmpty()) {
            tuple = new ExtendedFieldDeserializingProjectedTuple(fullTupleSchema, tableTuple, requiredColumnsMapping, virtualColumns);
        } else if (rowSchemaMatches) {
            tuple = new FormatAwareProjectedTuple(tableTuple, requiredColumnsMapping);
        } else {
            tuple = new FieldDeserializingProjectedTuple(fullTupleSchema, tableTuple, requiredColumnsMapping);
        }

        return factory.create(tuple);
    }
}
