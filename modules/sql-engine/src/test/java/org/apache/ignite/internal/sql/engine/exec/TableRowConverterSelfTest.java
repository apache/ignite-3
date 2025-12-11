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

import static org.apache.ignite.internal.sql.engine.util.Commons.readValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.internal.util.ColocationUtils;
import org.apache.ignite.internal.util.HashCalculator;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.Mockito;
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
                List.of(
                        Map.entry("c2", NativeTypes.STRING),
                        Map.entry("c1", NativeTypes.INT32)
                ),
                List.of("c1"),
                null
        );

        StructNativeType rowSchema = NativeTypes.rowBuilder()
                .addField("C1", NativeTypes.STRING, false)
                .addField("C2", NativeTypes.INT32, false)
                .build();

        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> rowFactory = rowHandler.create(rowSchema);

        ByteBuffer tupleBuf = new BinaryTupleBuilder(schema.length())
                .appendString("ABC")
                .appendInt(100)
                .build();

        BinaryRow binaryRow = new BinaryRowImpl(schema.version(), tupleBuf);

        TableRowConverterImpl converter = new TableRowConverterImpl(
                schemaRegistry,
                schema
        );

        RowWrapper row = converter.toRow(executionContext, binaryRow, rowFactory);
        assertEquals("ABC", rowHandler.get(0, row));
        assertEquals(100, rowHandler.get(1, row));
    }

    /** Test conversion to storage places columns in the order expected by a physical schema. */
    @Test
    public void testToKeyValueRow() {
        SchemaDescriptor schema = newSchema(
                List.of(
                        Map.entry("c4", NativeTypes.STRING),
                        Map.entry("c2", NativeTypes.BOOLEAN),
                        Map.entry("c3", NativeTypes.INT32),
                        Map.entry("c1", NativeTypes.INT32)
                ),
                List.of("c1", "c2"),
                null
        );

        StructNativeType rowSchema = NativeTypes.rowBuilder()
                .addField("C1", NativeTypes.STRING, false)
                .addField("C2", NativeTypes.BOOLEAN, false)
                .addField("C3", NativeTypes.INT32, false)
                .addField("C4", NativeTypes.INT32, false)
                .build();

        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> rowFactory = rowHandler.create(rowSchema);

        when(executionContext.rowAccessor()).thenReturn(rowHandler);

        RowWrapper wrapper = rowFactory.create("654", true, (int) Short.MAX_VALUE, 5);

        TableRowConverterImpl converter = new TableRowConverterImpl(
                schemaRegistry,
                schema
        );
        BinaryRowEx convertedRow = converter.toFullRow(executionContext, wrapper);

        BinaryTupleReader reader = new BinaryTupleReader(schema.length(), convertedRow.tupleSlice());

        assertEquals("654", reader.stringValue(0));
        assertTrue(reader.booleanValue(1));
        assertEquals(Short.MAX_VALUE, reader.intValue(2));
        assertEquals(5, reader.intValue(3));
    }

    @ParameterizedTest
    @CsvSource({
            "0, 1, 0",
            "0, 1, 1",
            "1, 0, 0",
            "1, 0, 1",
    })
    public void testToKeyOnlyRow(int key1, int key2, int colocationColumn) {
        Object[] keyColumnValues = {Short.MAX_VALUE, true};
        String[] keyColumnNames = {"c1", "c2"};

        SchemaDescriptor schema = newSchema(
                List.of(
                        Map.entry("c1", NativeTypes.INT16),
                        Map.entry("c2", NativeTypes.BOOLEAN),
                        Map.entry("c3", NativeTypes.INT32),
                        Map.entry("c4", NativeTypes.STRING)
                        ),
                List.of(keyColumnNames[key1], keyColumnNames[key2]),
                List.of(keyColumnNames[colocationColumn])
        );

        StructNativeType rowSchema = NativeTypes.rowBuilder()
                .addField("C1", schema.keyColumns().get(0).type(), false)
                .addField("C2", schema.keyColumns().get(1).type(), false)
                .build();

        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> rowFactory = rowHandler.create(rowSchema);

        when(executionContext.rowAccessor()).thenReturn(rowHandler);

        RowWrapper wrapper = rowFactory.create(keyColumnValues[key1], keyColumnValues[key2]);

        TableRowConverter converter = new TableRowConverterFactoryImpl(
                Mockito.mock(TableDescriptor.class),
                schemaRegistry,
                schema
        ).create(null);

        BinaryRowEx convertedRow = converter.toKeyRow(executionContext, wrapper);

        List<Column> keyColumns = schema.keyColumns();
        BinaryTuple reader = new BinaryTuple(keyColumns.size(), convertedRow.tupleSlice());

        assertEquals(
                keyColumnValues[key1],
                readValue(reader, schema.keyColumns().get(0).type(), 0)
        );
        assertEquals(
                keyColumnValues[key2],
                readValue(reader, schema.keyColumns().get(1).type(), 1)
        );
        assertEquals(
                colocationHash(keyColumnValues[colocationColumn], schema.colocationColumns().get(0).type()),
                convertedRow.colocationHash()
        );
    }

    private static SchemaDescriptor newSchema(
            List<Map.Entry<String, NativeType>> columns,
            List<String> keyColumns,
            @Nullable List<String> colocationColumns
    ) {
        List<Column> columnList = columns.stream()
                .map(entry -> {
                    String name = entry.getKey();
                    NativeType type = entry.getValue();

                    return new Column(name, type, !keyColumns.contains(name));
                })
                .collect(Collectors.toList());
        return new SchemaDescriptor(
                1,
                columnList,
                keyColumns,
                colocationColumns
        );
    }

    private static int colocationHash(Object value, NativeType type) {
        HashCalculator calculator = new HashCalculator();

        ColocationUtils.append(calculator, value, type);

        return calculator.hash();
    }
}
