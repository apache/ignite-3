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

package org.apache.ignite.internal.schema.serializer;

import static java.math.RoundingMode.HALF_UP;
import static org.apache.ignite.internal.schema.DefaultValueGenerator.GEN_RANDOM_UUID;
import static org.apache.ignite.internal.schema.DefaultValueProvider.constantProvider;
import static org.apache.ignite.internal.schema.DefaultValueProvider.forValueGenerator;
import static org.apache.ignite.internal.schema.SchemaTestUtils.specToType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DefaultValueProvider.Type;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.schema.marshaller.schema.AbstractSchemaSerializer;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.util.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * SchemaDescriptor (de)serializer test.
 */
public class AbstractSerializerTest {
    protected static final Map<NativeTypeSpec, List<Object>> DEFAULT_VALUES_TO_TEST;

    static {
        var tmp = new HashMap<NativeTypeSpec, List<Object>>();

        tmp.put(NativeTypeSpec.BOOLEAN, Arrays.asList(null, false, true));
        tmp.put(NativeTypeSpec.INT8, Arrays.asList(null, Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 14));
        tmp.put(NativeTypeSpec.INT16, Arrays.asList(null, Short.MIN_VALUE, Short.MAX_VALUE, (short) 14));
        tmp.put(NativeTypeSpec.INT32, Arrays.asList(null, Integer.MIN_VALUE, Integer.MAX_VALUE, 14));
        tmp.put(NativeTypeSpec.INT64, Arrays.asList(null, Long.MIN_VALUE, Long.MAX_VALUE, 14L));
        tmp.put(NativeTypeSpec.FLOAT, Arrays.asList(null, Float.MIN_VALUE, Float.MAX_VALUE, Float.NaN,
                Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, 14.14f));
        tmp.put(NativeTypeSpec.DOUBLE, Arrays.asList(null, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 14.14));
        tmp.put(NativeTypeSpec.DECIMAL, Arrays.asList(null, BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.valueOf(Long.MIN_VALUE),
                BigDecimal.valueOf(Long.MAX_VALUE), new BigDecimal("10000000000000000000000000000000000000")));
        tmp.put(NativeTypeSpec.DATE, Arrays.asList(null, LocalDate.MIN, LocalDate.MAX, LocalDate.EPOCH, LocalDate.now()));
        tmp.put(NativeTypeSpec.TIME, Arrays.asList(null, LocalTime.MIN, LocalTime.MAX, LocalTime.MIDNIGHT,
                LocalTime.NOON, LocalTime.now()));
        tmp.put(NativeTypeSpec.DATETIME, Arrays.asList(null, LocalDateTime.MIN, LocalDateTime.MAX, LocalDateTime.now()));
        tmp.put(NativeTypeSpec.TIMESTAMP, Arrays.asList(null, Instant.MIN, Instant.MAX, Instant.EPOCH, Instant.now()));
        tmp.put(NativeTypeSpec.UUID, Arrays.asList(null, UUID.randomUUID()));
        tmp.put(NativeTypeSpec.BITMASK, Arrays.asList(null, fromBinString(""), fromBinString("1"),
                fromBinString("10101010101010101010101")));
        tmp.put(NativeTypeSpec.STRING, Arrays.asList(null, "", UUID.randomUUID().toString()));
        tmp.put(NativeTypeSpec.BYTES, Arrays.asList(null, ArrayUtils.BYTE_EMPTY_ARRAY, UUID.randomUUID().toString().getBytes(
                StandardCharsets.UTF_8)));
        tmp.put(NativeTypeSpec.NUMBER, Arrays.asList(null, BigInteger.ONE, BigInteger.ZERO,
                new BigInteger("10000000000000000000000000000000000000")));

        var missedTypes = new HashSet<>(Arrays.asList(NativeTypeSpec.values()));

        missedTypes.removeAll(tmp.keySet());

        assertThat(missedTypes, empty());

        DEFAULT_VALUES_TO_TEST = Map.copyOf(tmp);
    }

    /**
     * (de)Serialize schema test.
     */
    @Test
    public void schemaSerializeTest() {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        SchemaDescriptor desc = new SchemaDescriptor(100500,
                new Column[]{
                        new Column("A", NativeTypes.INT8, false),
                        new Column("B", NativeTypes.INT16, false),
                        new Column("C", NativeTypes.INT32, false),
                        new Column("D", NativeTypes.INT64, false),
                        new Column("E", NativeTypes.UUID, false),
                        new Column("F", NativeTypes.FLOAT, false),
                        new Column("G", NativeTypes.DOUBLE, false),
                        new Column("H", NativeTypes.DATE, false),
                },
                new Column[]{
                        new Column("A1", NativeTypes.stringOf(128), false),
                        new Column("B1", NativeTypes.numberOf(255), false),
                        new Column("C1", NativeTypes.decimalOf(128, 64), false),
                        new Column("D1", NativeTypes.bitmaskOf(256), false),
                        new Column("E1", NativeTypes.datetime(8), false),
                        new Column("F1", NativeTypes.time(8), false),
                        new Column("G1", NativeTypes.timestamp(8), true)
                }
        );

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        assertEquals(desc.version(), deserialize.version());

        assertArrayEquals(desc.keyColumns().columns(), deserialize.keyColumns().columns());
        assertArrayEquals(desc.valueColumns().columns(), deserialize.valueColumns().columns());
        assertArrayEquals(desc.colocationColumns(), deserialize.colocationColumns());
    }

    /**
     * (de)Serialize schema test.
     */
    @Test
    public void columnOrderSerializeTest() {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        Column[] keyCols = {
            new Column(0, "A", NativeTypes.UUID, false),
            new Column(1, "B", NativeTypes.INT64, false),
            new Column(2, "C", NativeTypes.INT8, false),
        };

        Column[] valCols = {
            new Column(3, "A1", NativeTypes.stringOf(128), false),
            new Column(4, "B1", NativeTypes.INT64, false),
        };

        SchemaDescriptor desc = new SchemaDescriptor(100500, keyCols, valCols);

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        assertEquals(desc.version(), deserialize.version());

        ArrayList<Column> columns = new ArrayList<>();
        Collections.addAll(columns, keyCols);
        Collections.addAll(columns, valCols);

        for (int i = 0; i < columns.size(); i++) {
            Column col = deserialize.column(i);

            assertEquals(columns.get(col.columnOrder()), col);
        }

        assertArrayEquals(columns.stream().map(Column::name).toArray(String[]::new), desc.columnNames().toArray(String[]::new));
    }

    /**
     * (de)Serialize default value test.
     */
    @ParameterizedTest
    @MethodSource("generateTestArguments")
    public void defaultValueSerialization(DefaultValueArg arg) {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        var columnName = arg.type.spec().name();
        var nullable = arg.defaultValue == null;

        SchemaDescriptor desc = new SchemaDescriptor(100500,
                new Column[]{
                        new Column("ID", NativeTypes.INT8, false)
                },
                new Column[]{
                        new Column(columnName, arg.type, nullable, constantProvider(arg.defaultValue))
                }
        );

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        assertThat(deserialize.valueColumns().length(), equalTo(1));

        var column = deserialize.valueColumns().columns()[0];

        assertThat(column.name(), equalTo(columnName));
        assertThat(column.nullable(), equalTo(nullable));
        assertThat(column.defaultValue(), equalTo(arg.defaultValue));
    }

    /**
     * Validates functional default serialisation.
     */
    @Test
    public void functionalDefaultSerialization() {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        SchemaDescriptor desc = new SchemaDescriptor(100500,
                new Column[]{
                        new Column("ID", NativeTypes.stringOf(64), false, forValueGenerator(GEN_RANDOM_UUID))
                },
                new Column[]{
                        new Column("VAL", NativeTypes.INT8, true)
                }
        );

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        assertThat(deserialize.keyColumns().length(), equalTo(1));

        var column = deserialize.keyColumns().columns()[0];

        assertThat(column.name(), equalTo("ID"));
        assertThat(column.nullable(), equalTo(false));
        assertThat(column.defaultValueProvider().type(), equalTo(Type.FUNCTIONAL));

        var defaultVal = column.defaultValue();

        assertThat(defaultVal, notNullValue());
        assertThat(column.defaultValue(), not(equalTo(defaultVal))); // should generate next value
    }

    /**
     * (de)Serialize column mapping test.
     */
    @Test
    public void columnMappingSerializeTest() {
        final AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        SchemaDescriptor desc = new SchemaDescriptor(100500,
                new Column[]{
                        new Column("A", NativeTypes.INT8, false, constantProvider((byte) 1))
                },
                new Column[]{
                        new Column("A1", NativeTypes.stringOf(128), false, constantProvider("test")),
                        new Column("B1", NativeTypes.numberOf(255), false, constantProvider(BigInteger.TEN))
                }
        );

        ColumnMapper mapper = ColumnMapping.createMapper(desc);

        mapper.add(0, 1);

        Column c1 = new Column("C1", NativeTypes.stringOf(128), false, constantProvider("brandNewColumn")).copy(2);

        mapper.add(c1);

        desc.columnMapping(mapper);

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        ColumnMapper mapper1 = deserialize.columnMapping();

        assertEquals(1, mapper1.map(0));
        assertEquals(c1, mapper1.mappedColumn(2));
    }

    private static Iterable<DefaultValueArg> generateTestArguments() {
        var paramList = new ArrayList<DefaultValueArg>();

        for (var entry : DEFAULT_VALUES_TO_TEST.entrySet()) {
            for (var defaultValue : entry.getValue()) {
                paramList.add(
                        new DefaultValueArg(specToType(entry.getKey()), adjust(defaultValue))
                );
            }
        }
        return paramList;
    }

    /**
     * Adjust the given value.
     *
     * <p>Some values need to be adjusted before comparison. For example, decimal values should be adjusted
     * in order to have the same scale, because '1.0' not equals to '1.00'.
     *
     * @param val Value to adjust.
     * @param <T> Type of te value.
     * @return Adjusted value.
     */
    @SuppressWarnings("unchecked")
    protected static <T> T adjust(T val) {
        if (val instanceof BigDecimal) {
            return (T) ((BigDecimal) val).setScale(CatalogUtils.DEFAULT_SCALE, HALF_UP);
        }

        return val;
    }

    /** Creates a bit set from binary string. */
    private static BitSet fromBinString(String binString) {
        var bs = new BitSet();

        var idx = 0;
        for (var c : binString.toCharArray()) {
            if (c == '1') {
                bs.set(idx);
            }

            idx++;
        }

        return bs;
    }

    /**
     * Converts the given value to a string representation.
     *
     * <p>Convenient method to convert a value to a string. Some types don't override
     * {@link Object#toString()} method (any array, for instance), hence should be converted to a string manually.
     */
    private static String toString(Object val) {
        if (val instanceof byte[]) {
            return Arrays.toString((byte[]) val);
        }

        if (val == null) {
            return "null";
        }

        return val.toString();
    }

    /**
     * Class represents a default value of particular type.
     */
    private static class DefaultValueArg {
        final NativeType type;
        final Object defaultValue;

        /**
         * Constructor.
         *
         * @param type Type of the value
         * @param defaultValue value itself.
         */
        public DefaultValueArg(NativeType type, Object defaultValue) {
            this.type = type;
            this.defaultValue = defaultValue;
        }

        @Override
        public String toString() {
            return type.spec().name() + ": " + AbstractSerializerTest.toString(defaultValue);
        }
    }
}
