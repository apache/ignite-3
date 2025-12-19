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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.InternalTupleEx;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.util.ExtendedProjectedTuple;
import org.apache.ignite.internal.sql.engine.util.ProjectedTuple;

/**
 * Converts rows to execution engine representation.
 */
public class ProjectedTableRowConverterImpl extends TableRowConverterImpl {
    /**
     * Mapping of required columns to their indexes in physical schema.
     */
    private final int[] requiredColumnsMapping;

    private final Int2ObjectMap<VirtualColumn> virtualColumns;

    /** Constructor. */
    ProjectedTableRowConverterImpl(
            SchemaRegistry schemaRegistry,
            SchemaDescriptor schemaDescriptor,
            int[] requiredColumns,
            Int2ObjectMap<VirtualColumn> extraColumns
    ) {
        super(schemaRegistry, schemaDescriptor);

        this.requiredColumnsMapping = requiredColumns;
        this.virtualColumns = extraColumns;
    }

    @Override
    public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow tableRow, RowFactory<RowT> factory) {
        InternalTuple tuple;
        boolean rowSchemaMatches = tableRow.schemaVersion() == schemaDescriptor.version();

        InternalTupleEx tableTuple = rowSchemaMatches
                ? new BinaryTuple(schemaDescriptor.length(), tableRow.tupleSlice())
                : schemaRegistry.resolve(tableRow, schemaDescriptor);

        if (!virtualColumns.isEmpty()) {
            tuple = new ExtendedProjectedTuple(tableTuple, requiredColumnsMapping, virtualColumns);
        } else {
            tuple = new ProjectedTuple(tableTuple, requiredColumnsMapping);
        }

        return factory.create(tuple);
    }
}
