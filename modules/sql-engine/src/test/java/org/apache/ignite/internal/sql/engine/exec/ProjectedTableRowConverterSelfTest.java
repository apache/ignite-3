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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.util.BitSets;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link TableRowConverterImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class ProjectedTableRowConverterSelfTest extends BaseIgniteAbstractTest {

    @Mock
    private SchemaRegistry schemaRegistry;

    @Mock
    private ExecutionContext<RowWrapper> executionContext;

    /** Test checks conversion from storage row to execution engine row. */
    @Test
    public void testToEngineRowSameVersion() {
        SchemaDescriptor schema = newSchema(
                List.of(
                        Map.entry("c3", NativeTypes.UUID),
                        Map.entry("c2", NativeTypes.STRING),
                        Map.entry("c4", NativeTypes.BOOLEAN),
                        Map.entry("c1", NativeTypes.INT32)
                        ),
                List.of("c1"),
                null
        );

        RowSchema rowSchema = RowSchema.builder()
                .addField(NativeTypes.STRING)
                .addField(NativeTypes.INT32)
                .build();

        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> rowFactory = rowHandler.factory(rowSchema);

        ByteBuffer tupleBuf = new BinaryTupleBuilder(schema.length())
                .appendUuid(UUID.randomUUID())
                .appendString("ABC")
                .appendBoolean(true)
                .appendInt(100)
                .build();

        BinaryRow binaryRow = new BinaryRowImpl(schema.version(), tupleBuf);

        ProjectedTableRowConverterImpl converter = new ProjectedTableRowConverterImpl(
                schemaRegistry,
                BinaryTupleSchema.createRowSchema(schema),
                schema,
                BitSets.of(1, 3),
                List.of()
        );

        RowWrapper row = converter.toRow(executionContext, binaryRow, rowFactory);
        assertEquals("ABC", rowHandler.get(0, row));
        assertEquals(100, rowHandler.get(1, row));
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
}
