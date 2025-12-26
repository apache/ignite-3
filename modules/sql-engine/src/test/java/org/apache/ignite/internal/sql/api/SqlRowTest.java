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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.IDENTITY_ROW_CONVERTER;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.api.AsyncResultSetImpl.SqlRowImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRowImpl;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Ensures that reading a {@code null} value as primitive from the
 * {@link SqlRow} instance produces a {@link NullPointerException}.
 */
public class SqlRowTest extends BaseIgniteAbstractTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("primitiveAccessors")
    @SuppressWarnings("ThrowableNotThrown")
    void nullPointerWhenReadingNullAsPrimitive(
            NativeType type,
            BiConsumer<Tuple, Object> fieldAccessor
    ) {
        SqlRow row = makeRow(type);

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> fieldAccessor.accept(row, 0),
                IgniteStringFormatter.format(IgniteUtils.NULL_TO_PRIMITIVE_ERROR_MESSAGE, 0)
        );

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> fieldAccessor.accept(row, "VAL"),
                IgniteStringFormatter.format(IgniteUtils.NULL_TO_PRIMITIVE_NAMED_ERROR_MESSAGE, "VAL")
        );
    }

    private static SqlRow makeRow(NativeType type) {
        SqlRowHandler handler = SqlRowHandler.INSTANCE;
        StructNativeType schema = NativeTypes.structBuilder()
                .addField("VAL", type, true)
                .build();

        RowFactory<RowWrapper> factory = handler.create(schema);
        RowWrapper binaryTupleRow = factory.create(handler.toBinaryTuple(factory.create(new Object[]{null})));

        InternalSqlRowImpl<RowWrapper> internalSqlRow =
                new InternalSqlRowImpl<>(binaryTupleRow, handler, IDENTITY_ROW_CONVERTER);

        return new SqlRowImpl(internalSqlRow, new ResultSetMetadataImpl(List.of(
                new ColumnMetadataImpl("VAL", type.spec(), 0, 0, true, null))));
    }

    private static Stream<Arguments> primitiveAccessors() {
        return SchemaTestUtils.PRIMITIVE_ACCESSORS.entrySet().stream()
                .map(e -> Arguments.of(e.getKey(), e.getValue()));
    }
}
