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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link TableRowConverterImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class TableRowConverterSelfTest extends BaseIgniteAbstractTest {

    @Mock
    private SchemaRegistry schemaRegistry;

    @Mock
    private ExecutionContext<RowWrapper> executionContext;

    /** Test checks conversion from storage row to execution engine row. */
    @Test
    public void testToEngineRowSameVersion() {
        SchemaDescriptor schema = newSchema(
                List.of("c2", "c1"),
                List.of(
                        Map.entry("c1", NativeTypes.INT32)
                ),
                List.of(
                        Map.entry("c2", NativeTypes.STRING)
                )
        );

        RowSchema rowSchema = RowSchema.builder()
                .addField(NativeTypes.STRING)
                .addField(NativeTypes.INT32)
                .build();

        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> rowFactory = rowHandler.factory(rowSchema);

        ByteBuffer tupleBuf = new BinaryTupleBuilder(schema.length())
                .appendInt(100)
                .appendString("ABC")
                .build();

        BinaryRow binaryRow = new BinaryRowImpl(schema.version(), tupleBuf);

        TableRowConverterImpl converter = new TableRowConverterImpl(schemaRegistry, schema, null);

        RowWrapper row = converter.toRow(executionContext, binaryRow, rowFactory);
        assertEquals("ABC", rowHandler.get(0, row));
        assertEquals(100, rowHandler.get(1, row));
    }

    /** Test conversion to storage places columns in the order expected by a physical schema. */
    @Test
    public void testToKeyValueRow() {
        SchemaDescriptor schema = newSchema(
                List.of("c4", "c2", "c3", "c1"),
                List.of(
                        Map.entry("c1", NativeTypes.INT32),
                        Map.entry("c2", NativeTypes.BOOLEAN)
                ),
                List.of(
                        Map.entry("c3", NativeTypes.INT32),
                        Map.entry("c4", NativeTypes.STRING)
                )
        );

        RowSchema rowSchema = RowSchema.builder()
                .addField(NativeTypes.STRING)
                .addField(NativeTypes.BOOLEAN)
                .addField(NativeTypes.INT32)
                .addField(NativeTypes.INT32)
                .build();

        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> rowFactory = rowHandler.factory(rowSchema);

        when(executionContext.rowHandler()).thenReturn(rowHandler);

        RowWrapper wrapper = rowFactory.create("654", true, (int) Short.MAX_VALUE, 5);

        TableRowConverterImpl converter = new TableRowConverterImpl(schemaRegistry, schema, null);
        BinaryRowEx convertedRow = converter.toBinaryRow(executionContext, wrapper, false);

        BinaryTupleReader reader = new BinaryTupleReader(schema.length(), convertedRow.tupleSlice());

        // Schema stores in key columns in the following order: c2, c1
        assertEquals(true, reader.booleanValue(0));
        assertEquals(5, reader.intValue(1));
        assertEquals(Short.MAX_VALUE, reader.intValue(2));
        assertEquals("654", reader.stringValue(3));
    }

    /** Test conversion to storage that key-only binary row keys in the order expected by a physical schema. */
    @Test
    public void testToKeyOnlyRowDifferentKeyOrder() {
        SchemaDescriptor schema = newSchema(
                List.of("c1", "c2", "c3", "c4"),
                List.of(
                        Map.entry("c1", NativeTypes.INT32),
                        Map.entry("c2", NativeTypes.BOOLEAN)
                ),
                List.of(
                        Map.entry("c3", NativeTypes.INT32),
                        Map.entry("c4", NativeTypes.STRING)
                )
        );

        RowSchema rowSchema = RowSchema.builder()
                .addField(NativeTypes.INT32)
                .addField(NativeTypes.BOOLEAN)
                .build();

        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> rowFactory = rowHandler.factory(rowSchema);

        when(executionContext.rowHandler()).thenReturn(rowHandler);

        RowWrapper wrapper = rowFactory.create((int) Short.MAX_VALUE, true);

        TableRowConverterImpl converter = new TableRowConverterImpl(schemaRegistry, schema, null);
        BinaryRowEx convertedRow = converter.toBinaryRow(executionContext, wrapper, true);

        Columns keyColumns = schema.keyColumns();
        BinaryTupleReader reader = new BinaryTupleReader(keyColumns.length(), convertedRow.tupleSlice());

        // Schema stores in key columns in the following order: c2, c1
        assertEquals(true, reader.booleanValue(0));
        assertEquals(Short.MAX_VALUE, reader.intValue(1));
    }

    /** Test conversion to storage that key-only binary row keys in the order expected by a physical schema. */
    @Test
    public void testToKeyOnlyRowSameKeyOrder() {
        SchemaDescriptor schema = newSchema(
                List.of("c1", "c2", "c3", "c4"),
                List.of(
                        Map.entry("c2", NativeTypes.INT32),
                        Map.entry("c4", NativeTypes.STRING)
                ),
                List.of(
                        Map.entry("c1", NativeTypes.INT32),
                        Map.entry("c3", NativeTypes.INT32)
                )
        );

        List<String> schemaOrder = new ArrayList<>();
        for (int i = 0; i < schema.length(); i++) {
            schemaOrder.add(schema.column(i).name());
        }

        assertEquals(List.of("c2", "c4", "c1", "c3"), schemaOrder);

        RowSchema rowSchema = RowSchema.builder()
                .addField(NativeTypes.INT32)
                .addField(NativeTypes.STRING)
                .build();

        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> rowFactory = rowHandler.factory(rowSchema);

        when(executionContext.rowHandler()).thenReturn(rowHandler);

        RowWrapper wrapper = rowFactory.create(2, "abc");

        TableRowConverterImpl converter = new TableRowConverterImpl(schemaRegistry, schema, null);
        BinaryRowEx convertedRow = converter.toBinaryRow(executionContext, wrapper, true);

        Columns keyColumns = schema.keyColumns();
        BinaryTupleReader reader = new BinaryTupleReader(keyColumns.length(), convertedRow.tupleSlice());

        // Schema stores in key columns in the following order: c2, c4
        assertEquals(2, reader.intValue(0));
        assertEquals("abc", reader.stringValue(1));

        convertedRow.colocationHash();
    }

    private static SchemaDescriptor newSchema(List<String> definitionOrder, List<Map.Entry<String, NativeType>> keyCols,
            List<Map.Entry<String, NativeType>> valueCols) {

        Column[] keyColDescriptors = new Column[keyCols.size()];
        int i = 0;

        for (Map.Entry<String, NativeType> col : keyCols) {
            String name = col.getKey();
            int order = definitionOrder.indexOf(name);

            keyColDescriptors[i] = new Column(order, name, col.getValue(), false);
            i++;
        }

        Column[] valColDescriptors = new Column[valueCols.size()];
        i = 0;

        for (Map.Entry<String, NativeType> col : valueCols) {
            String name = col.getKey();
            int order = definitionOrder.indexOf(name);

            valColDescriptors[i] = new Column(order, name, col.getValue(), true);
            i++;
        }

        return new SchemaDescriptor(1, keyColDescriptors, valColDescriptors);
    }
}
