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

package org.apache.ignite.internal.sql.engine.exec.row;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory.RowBuilder;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.NativeTypes.StructTypeBuilder;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link SqlRowHandler}.
 */
public class SqlRowHandlerTest extends IgniteAbstractTest {
    private static final SqlRowHandler handler = SqlRowHandler.INSTANCE;

    private final long seed = ThreadLocalRandom.current().nextLong();

    private final Random rnd = new Random(seed);

    @BeforeEach
    void printSeed() {
        log.info("Using seed: " + seed);
    }

    @Test
    public void testBytebufferSerialization() {
        List<ColumnType> columnTypes = shuffledColumnTypes();
        Object[] sourceData = values(columnTypes);
        StructNativeType schema = rowSchema(columnTypes, sourceData);

        int elementsCount = schema.fields().size();

        RowFactory<RowWrapper> factory = handler.create(schema);
        RowWrapper src = factory.create(wrap(sourceData, schema));

        // Serialization to binary tuple representation.
        BinaryTuple tuple = handler.toBinaryTuple(src);
        RowWrapper dest = factory.create(tuple);

        for (int i = 0; i < elementsCount; i++) {
            String msg = schema.fields().get(i).toString();
            Object expected = convertToInternal(schema.fields().get(i).type(), sourceData[i]);

            assertThat(msg, handler.get(i, src), equalTo(expected));
            assertThat(msg, handler.get(i, dest), equalTo(expected));
        }
    }

    @Test
    public void testMap() {
        List<ColumnType> columnTypes = List.of(
                ColumnType.INT32,
                ColumnType.STRING,
                ColumnType.BOOLEAN,
                ColumnType.INT32,
                ColumnType.DOUBLE,
                ColumnType.STRING
        );

        int[] mapping = {3, 5};

        Object[] sourceData = values(columnTypes);
        StructNativeType schema = rowSchema(columnTypes, sourceData);

        RowFactory<RowWrapper> factory = handler.create(schema);

        RowWrapper srcRow = factory.create(sourceData);

        RowWrapper srcBinRow = factory.create(handler.toBinaryTuple(srcRow));

        StructNativeType mappedSchema = rowSchema(columnTypes.subList(0, mapping.length), Arrays.copyOf(sourceData, mapping.length));
        RowFactory<RowWrapper> mappedFactory = handler.create(mappedSchema);

        RowWrapper mappedRow = mappedFactory.map(srcRow, mapping);
        RowWrapper mappedFromBinRow = mappedFactory.map(srcBinRow, mapping);

        RowWrapper deserializedMappedBinRow = mappedFactory.create(handler.toBinaryTuple(mappedFromBinRow));

        assertThat(handler.columnsCount(mappedRow), equalTo(mapping.length));
        assertThat(handler.columnsCount(mappedFromBinRow), equalTo(mapping.length));

        for (int i = 0; i < mapping.length; i++) {
            Object expected = handler.get(mapping[i], srcRow);

            assertThat(handler.get(i, mappedRow), equalTo(expected));
            assertThat(handler.get(i, mappedFromBinRow), equalTo(expected));
            assertThat(handler.get(i, deserializedMappedBinRow), equalTo(expected));
        }
    }

    @Test
    public void testUpdateRowSchemaOnMapping() {
        SqlRowHandler handler = SqlRowHandler.INSTANCE;

        StructNativeType rowSchema = NativeTypes.structBuilder()
                .addField("C1", NativeTypes.INT32, false)
                .addField("C2", NativeTypes.STRING, false)
                .build();

        RowWrapper row1 = handler.create(rowSchema).rowBuilder()
                .addField(1).addField("2")
                .build();

        StructNativeType reverseRowSchema = NativeTypes.structBuilder()
                .addField("C1", NativeTypes.STRING, false)
                .addField("C2", NativeTypes.INT32, false)
                .build();

        RowFactory<RowWrapper> factory = handler.create(reverseRowSchema);

        RowWrapper reverseMapping = factory.map(row1, new int[]{1, 0});

        BinaryTuple mappedBinaryTuple = handler.toBinaryTuple(reverseMapping);
        assertEquals("2", mappedBinaryTuple.stringValue(0));
        assertEquals(1, mappedBinaryTuple.intValue(1));
    }

    private static Stream<Arguments> concatTestArguments() {
        return Stream.of(
                Arguments.of(Named.of("array", false), Named.of("array", false)),
                Arguments.of(Named.of("array", false), Named.of("tuple", true)),
                Arguments.of(Named.of("tuple", true), Named.of("array", false)),
                Arguments.of(Named.of("tuple", true), Named.of("tuple", true))
        );
    }

    @ParameterizedTest
    @MethodSource("columnTypes")
    public void testRowBuilder(ColumnType type) {
        Object value1 = SqlTestUtils.generateValueByTypeWithMaxScalePrecisionForSql(type);

        StructNativeType rowSchema = rowSchema(List.of(type), new Object[]{value1});
        RowFactory<RowWrapper> rowFactory = handler.create(rowSchema);
        RowBuilder<RowWrapper> builder = rowFactory.rowBuilder();

        RowWrapper row00 = builder.addField(value1).build();
        assertEquals(value1, handler.get(0, row00));

        RowWrapper row01 = builder.build();
        assertEquals(value1, handler.get(0, row01));

        builder.reset();

        Object value2 = SqlTestUtils.generateValueByTypeWithMaxScalePrecisionForSql(type);
        RowWrapper row2 = builder.addField(value2).build();
        assertEquals(value2, handler.get(0, row2));
    }

    @Test
    public void testRowBuilderRejectInvalidField() {
        StructNativeType rowSchema = rowSchema(List.of(ColumnType.INT32), new Object[]{1});
        RowFactory<RowWrapper> rowFactory = handler.create(rowSchema);

        RowBuilder<RowWrapper> builder = rowFactory.rowBuilder();
        builder.addField(1);

        IllegalStateException err = assertThrows(IllegalStateException.class, () -> builder.addField(1));
        assertThat(err.getMessage(), containsString("Field index is out of bounds"));
    }

    @Test
    public void testRowBuilderBuildReset() {
        StructNativeType rowSchema = rowSchema(List.of(ColumnType.INT32), new Object[]{1});
        RowFactory<RowWrapper> rowFactory = handler.create(rowSchema);

        RowBuilder<RowWrapper> builder = rowFactory.rowBuilder();

        RowWrapper row1 = builder.addField(1).build();
        assertEquals(1, handler.get(0, row1));

        builder.reset();

        String message = "Row has not been initialised";
        IllegalStateException err1 = assertThrows(IllegalStateException.class, builder::build);
        assertThat(err1.getMessage(), containsString(message));

        RowWrapper row2 = builder.addField(2).buildAndReset();
        assertEquals(2, handler.get(0, row2));

        IllegalStateException err2 = assertThrows(IllegalStateException.class, builder::build);
        assertThat(err2.getMessage(), containsString(message));

        RowWrapper row3 = builder.addField(3).build();
        assertEquals(3, handler.get(0, row3));
    }

    @Test
    public void testRowBuilderBuildingIncompleteRowIsNotAllowed() {
        StructNativeType rowSchema = rowSchema(List.of(ColumnType.INT32, ColumnType.INT32), new Object[]{1, 2});
        RowFactory<RowWrapper> rowFactory = handler.create(rowSchema);

        RowBuilder<RowWrapper> rowBuilder = rowFactory.rowBuilder();
        rowBuilder.addField(1);

        IllegalStateException err = assertThrows(IllegalStateException.class, rowBuilder::build);
        assertThat(err.getMessage(), containsString("Row has not been fully built"));
    }

    @Test
    public void testRowBuilderEmptyRow() {
        RowFactory<RowWrapper> rowFactory = handler.create(NativeTypes.structBuilder().build());
        RowBuilder<RowWrapper> rowBuilder = rowFactory.rowBuilder();
        assertNotNull(rowBuilder.build());
    }

    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-17373 Interval type support.
    @EnumSource(value = ColumnType.class, names = {"NULL", "PERIOD", "DURATION", "STRUCT"}, mode = EnumSource.Mode.EXCLUDE)
    public void testIsNull(ColumnType columnType) {
        NativeType nativeType = TypeUtils.columnType2NativeType(columnType, 3, 3, 0);

        StructNativeType rowSchema = NativeTypes.structBuilder()
                .addField("C1", nativeType, true)
                .build();

        RowFactory<RowWrapper> rowFactory = handler.create(rowSchema);

        {
            RowWrapper row = rowFactory.create(new Object[]{null});
            assertNull(handler.get(0, row));
            assertTrue(handler.isNull(0, row));
        }

        {
            RowWrapper row = rowFactory.rowBuilder().addField(null).build();
            assertNull(handler.get(0, row));
            assertTrue(handler.isNull(0, row));
        }
    }

    private StructNativeType rowSchema(List<ColumnType> columnTypes, Object[] values) {
        StructTypeBuilder schemaBuilder = NativeTypes.structBuilder();

        for (int i = 0; i < values.length; i++) {
            ColumnType type = columnTypes.get(i);

            NativeType nativeType = values[i] == null
                    ? TypeUtils.columnType2NativeType(type, 9, 3, 20)
                    : NativeTypes.fromObject(values[i]);

            assertNotNull(nativeType, "Unable to create nativeType for columnType=" 
                    + type + " and value={" + values[i] + "}");

            schemaBuilder.addField("F$" + i, nativeType, values[i] == null || rnd.nextBoolean());
        }

        return schemaBuilder.build();
    }

    private Object[] values(List<ColumnType> columnTypes) {
        Object[] values = new Object[columnTypes.size()];

        for (int i = 0; i < values.length; i++) {
            ColumnType type = columnTypes.get(i);

            values[i] = SqlTestUtils.generateValueByTypeWithMaxScalePrecisionForSql(type);
        }

        return values;
    }

    private static Object[] wrap(Object[] values, StructNativeType rowSchema) {
        Object[] newValues = new Object[values.length];

        for (int i = 0; i < values.length; i++) {
            newValues[i] = convertToInternal(rowSchema.fields().get(i).type(), values[i]);
        }

        return newValues;
    }

    private static Set<ColumnType> columnTypes() {
        // TODO Include ignored types to test after https://issues.apache.org/jira/browse/IGNITE-17373
        return EnumSet.complementOf(EnumSet.of(ColumnType.PERIOD, ColumnType.DURATION, ColumnType.STRUCT));
    }

    private List<ColumnType> shuffledColumnTypes() {
        List<ColumnType> columnTypes = new ArrayList<>(columnTypes());

        Collections.shuffle(columnTypes, rnd);

        return columnTypes;
    }

    private static @Nullable Object convertToInternal(NativeType type, Object value) {
        if (type.spec() == ColumnType.NULL) {
            return null;
        }

        return TypeUtils.toInternal(value, type.spec());
    }
}
