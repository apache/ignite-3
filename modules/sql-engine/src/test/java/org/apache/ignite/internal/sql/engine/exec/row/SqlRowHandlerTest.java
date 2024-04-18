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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.generateValueByType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowBuilder;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema.Builder;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link SqlRowHandler}.
 */
public class SqlRowHandlerTest extends IgniteAbstractTest {
    private static final RowHandler<RowWrapper> handler = SqlRowHandler.INSTANCE;

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
        RowSchema schema = rowSchema(columnTypes, sourceData);

        int elementsCount = schema.fields().size();

        RowFactory<RowWrapper> factory = handler.factory(schema);
        RowWrapper src = factory.create(wrap(sourceData, schema));

        // Serialization to binary tuple representation.
        BinaryTuple tuple = handler.toBinaryTuple(src);
        RowWrapper dest = factory.create(tuple);

        for (int i = 0; i < elementsCount; i++) {
            String msg = schema.fields().get(i).toString();
            TypeSpec typeSpec = schema.fields().get(i);
            Object expected = convertToInternal(typeSpec, sourceData[i]);

            assertThat(msg, handler.get(i, src), equalTo(expected));
            assertThat(msg, handler.get(i, dest), equalTo(expected));
        }
    }

    @ParameterizedTest(name = "{0} - {1}")
    @MethodSource("concatTestArguments")
    public void testConcat(boolean leftTupleRequired, boolean rightTupleRequired) {
        ConcatTestParameters params = new ConcatTestParameters(leftTupleRequired, rightTupleRequired);

        RowWrapper concatenated = handler.concat(params.left, params.right);

        int leftLen = params.leftData.length;
        int rightLen = params.rightData.length;
        int totalElementsCount = leftLen + rightLen;

        assertThat(handler.columnCount(concatenated), equalTo(totalElementsCount));

        // Build combined schema.
        Builder builder = RowSchema.builder();
        params.leftSchema.fields().forEach(builder::addField);
        params.rightSchema.fields().forEach(builder::addField);

        RowSchema concatenatedSchema = builder.build();

        // Serialize.
        BinaryTuple tuple = handler.toBinaryTuple(concatenated);

        // Wrap into row.
        RowWrapper result = handler.factory(concatenatedSchema).create(tuple);

        for (int i = 0; i < leftLen; i++) {
            TypeSpec typeSpec = params.leftSchema.fields().get(i);

            assertThat(handler.get(i, result), equalTo(convertToInternal(typeSpec, params.leftData[i])));
        }

        for (int i = 0; i < rightLen; i++) {
            TypeSpec typeSpec = params.rightSchema.fields().get(i);

            assertThat(handler.get(leftLen + i, result), equalTo(convertToInternal(typeSpec, params.rightData[i])));
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
        RowSchema schema = rowSchema(columnTypes, sourceData);

        RowFactory<RowWrapper> factory = handler.factory(schema);

        RowWrapper srcRow = factory.create(sourceData);
        RowWrapper srcBinRow = factory.create(handler.toBinaryTuple(srcRow));

        RowWrapper mappedRow = handler.map(srcRow, mapping);
        RowWrapper mappedFromBinRow = handler.map(srcBinRow, mapping);

        RowSchema mappedSchema = rowSchema(columnTypes.subList(0, mapping.length), Arrays.copyOf(sourceData, mapping.length));
        RowWrapper deserializedMappedBinRow = handler.factory(mappedSchema).create(handler.toBinaryTuple(mappedFromBinRow));

        assertThat(handler.columnCount(mappedRow), equalTo(mapping.length));
        assertThat(handler.columnCount(mappedFromBinRow), equalTo(mapping.length));

        for (int i = 0; i < mapping.length; i++) {
            Object expected = handler.get(mapping[i], srcRow);

            assertThat(handler.get(i, mappedRow), equalTo(expected));
            assertThat(handler.get(i, mappedFromBinRow), equalTo(expected));
            assertThat(handler.get(i, deserializedMappedBinRow), equalTo(expected));
        }
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
        Object value1 = generateValueByType(0, type);

        RowSchema rowSchema = rowSchema(List.of(type), new Object[]{value1});
        RowFactory<RowWrapper> rowFactory = handler.factory(rowSchema);
        RowBuilder<RowWrapper> builder = rowFactory.rowBuilder();

        RowWrapper row00 = builder.addField(value1).build();
        assertEquals(value1, handler.get(0, row00));

        RowWrapper row01 = builder.build();
        assertEquals(value1, handler.get(0, row01));

        builder.reset();

        Object value2 = generateValueByType(0, type);
        RowWrapper row2 = builder.addField(value2).build();
        assertEquals(value2, handler.get(0, row2));
    }

    @Test
    public void testRowBuilderRejectInvalidField() {
        RowSchema rowSchema = rowSchema(List.of(ColumnType.INT32), new Object[]{1});
        RowFactory<RowWrapper> rowFactory = handler.factory(rowSchema);

        RowBuilder<RowWrapper> builder = rowFactory.rowBuilder();
        builder.addField(1);

        IllegalStateException err = assertThrows(IllegalStateException.class, () -> builder.addField(1));
        assertThat(err.getMessage(), containsString("Field index is out of bounds"));
    }

    @Test
    public void testRowBuilderBuildReset() {
        RowSchema rowSchema = rowSchema(List.of(ColumnType.INT32), new Object[]{1});
        RowFactory<RowWrapper> rowFactory = handler.factory(rowSchema);

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
        RowSchema rowSchema = rowSchema(List.of(ColumnType.INT32, ColumnType.INT32), new Object[]{1, 2});
        RowFactory<RowWrapper> rowFactory = handler.factory(rowSchema);

        RowBuilder<RowWrapper> rowBuilder = rowFactory.rowBuilder();
        rowBuilder.addField(1);

        IllegalStateException err = assertThrows(IllegalStateException.class, rowBuilder::build);
        assertThat(err.getMessage(), containsString("Row has not been fully built"));
    }

    @Test
    public void testRowBuilderEmptyRow() {
        RowFactory<RowWrapper> rowFactory = handler.factory(RowSchema.builder().build());
        RowBuilder<RowWrapper> rowBuilder = rowFactory.rowBuilder();
        assertNotNull(rowBuilder.build());
    }

    private RowSchema rowSchema(List<ColumnType> columnTypes, Object[] values) {
        Builder schemaBuilder = RowSchema.builder();

        for (int i = 0; i < values.length; i++) {
            ColumnType type = columnTypes.get(i);

            if (type == ColumnType.NULL) {
                schemaBuilder.addField(new NullTypeSpec());

                continue;
            }

            NativeType nativeType = values[i] == null
                    ? TypeUtils.columnType2NativeType(type, 9, 3, 20)
                    : NativeTypes.fromObject(values[i]);

            schemaBuilder.addField(nativeType, values[i] == null || rnd.nextBoolean());
        }

        return schemaBuilder.build();
    }

    private Object[] values(List<ColumnType> columnTypes) {
        Object[] values = new Object[columnTypes.size()];
        int baseValue = rnd.nextInt();

        for (int i = 0; i < values.length; i++) {
            ColumnType type = columnTypes.get(i);

            values[i] = type == ColumnType.NULL ? null : generateValueByType(baseValue, type);
        }

        return values;
    }

    private static Object[] wrap(Object[] values, RowSchema rowSchema) {
        Object[] newValues = new Object[values.length];

        for (int i = 0; i < values.length; i++) {
            TypeSpec typeSpec = rowSchema.fields().get(i);
            newValues[i] = convertToInternal(typeSpec, values[i]);
        }

        return newValues;
    }

    private static Set<ColumnType> columnTypes() {
        // TODO Include ignored types to test after https://issues.apache.org/jira/browse/IGNITE-15200
        return EnumSet.complementOf(EnumSet.of(ColumnType.PERIOD, ColumnType.DURATION));
    }

    private List<ColumnType> shuffledColumnTypes() {
        List<ColumnType> columnTypes = new ArrayList<>(columnTypes());

        Collections.shuffle(columnTypes, rnd);

        return columnTypes;
    }

    private final class ConcatTestParameters {
        final RowSchema leftSchema;
        final RowSchema rightSchema;
        final Object[] leftData;
        final Object[] rightData;
        final RowWrapper left;
        final RowWrapper right;

        ConcatTestParameters(boolean leftTupleRequired, boolean rightTupleRequired) {
            List<ColumnType> columnTypes1 = shuffledColumnTypes();
            List<ColumnType> columnTypes2 = shuffledColumnTypes();

            leftData = values(columnTypes1);
            rightData = values(columnTypes2);
            leftSchema = rowSchema(columnTypes1, leftData);
            rightSchema = rowSchema(columnTypes2, rightData);

            RowFactory<RowWrapper> factory1 = handler.factory(leftSchema);
            RowFactory<RowWrapper> factory2 = handler.factory(rightSchema);

            RowWrapper left = factory1.create(wrap(leftData, leftSchema));
            RowWrapper right = factory2.create(wrap(rightData, rightSchema));

            this.left = leftTupleRequired ? factory1.create(handler.toBinaryTuple(left)) : left;
            this.right = rightTupleRequired ? factory2.create(handler.toBinaryTuple(right)) : right;
        }
    }

    private static @Nullable Object convertToInternal(NativeType nativeType, Object value) {
        return convertToInternal(RowSchemaTypes.nativeType(nativeType), value);
    }

    private static @Nullable Object convertToInternal(TypeSpec typeSpec, Object value) {
        if (typeSpec instanceof NullTypeSpec) {
            return null;
        } else {
            BaseTypeSpec baseTypeSpec = (BaseTypeSpec) typeSpec;
            NativeType nativeType = baseTypeSpec.nativeType();
            Class<?> type = Commons.nativeTypeToClass(nativeType);
            return TypeUtils.toInternal(value, type);
        }
    }
}
