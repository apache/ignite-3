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

package org.apache.ignite.internal.client.sql;

import static org.apache.ignite.internal.type.NativeTypes.INT32;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Ensures that reading a {@code null} value as primitive from the
 * {@link ClientSqlRow} instance produces a {@link NullPointerException}.
 */
public class ClientSqlRowTest extends BaseIgniteAbstractTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("primitiveAccessors")
    @SuppressWarnings("ThrowableNotThrown")
    void nullPointerWhenReadingNullAsPrimitive(
            NativeType type,
            BiConsumer<Tuple, Object> fieldAccessor
    ) {
        String valueColumn = "VAL";
        Tuple row = createRow(valueColumn, type);

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> fieldAccessor.accept(row, 1),
                IgniteStringFormatter.format(IgniteUtils.NULL_TO_PRIMITIVE_ERROR_MESSAGE, 1)
        );

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> fieldAccessor.accept(row, valueColumn),
                IgniteStringFormatter.format(IgniteUtils.NULL_TO_PRIMITIVE_NAMED_ERROR_MESSAGE, valueColumn)
        );

        IgniteTestUtils.assertThrows(
                UnsupportedOperationException.class,
                () -> row.set("NEW", null),
                null
        );
    }

    private static Tuple createRow(String columnName, NativeType type) {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("ID", INT32, false)},
                new Column[]{new Column(columnName, type, true)}
        );

        BinaryRow binaryRow = SchemaTestUtils.binaryRow(schema, 1, null);
        BinaryTupleReader binaryTuple = new BinaryTupleReader(2, binaryRow.tupleSlice());

        ResultSetMetadata resultSetMetadata = new ResultSetMetadataImpl(List.of(
                new ColumnMetadataImpl("ID", ColumnType.INT32, 0, 0, false, null),
                new ColumnMetadataImpl(columnName, type.spec(), 0, 0, true, null)
        ));

        return new ClientSqlRow(binaryTuple, resultSetMetadata);
    }

    private static Stream<Arguments> primitiveAccessors() {
        return SchemaTestUtils.PRIMITIVE_ACCESSORS.entrySet().stream()
                .map(e -> Arguments.of(e.getKey(), e.getValue()));
    }
}
