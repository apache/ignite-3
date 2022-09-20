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

package org.apache.ignite.internal.schema.configuration;

import static java.math.RoundingMode.HALF_UP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.ColumnType.ColumnTypeSpec;
import org.apache.ignite.schema.definition.ColumnType.DecimalColumnType;

/**
 * Abstract class for schema converter tests.
 */
public class AbstractSchemaConverterTest {
    protected static final Map<ColumnTypeSpec, List<Object>> DEFAULT_VALUES_TO_TEST;

    static {
        var tmp = new HashMap<ColumnTypeSpec, List<Object>>();

        tmp.put(ColumnTypeSpec.INT8, Arrays.asList(null, Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 14));
        tmp.put(ColumnTypeSpec.INT16, Arrays.asList(null, Short.MIN_VALUE, Short.MAX_VALUE, (short) 14));
        tmp.put(ColumnTypeSpec.INT32, Arrays.asList(null, Integer.MIN_VALUE, Integer.MAX_VALUE, 14));
        tmp.put(ColumnTypeSpec.INT64, Arrays.asList(null, Long.MIN_VALUE, Long.MAX_VALUE, 14L));
        tmp.put(ColumnTypeSpec.FLOAT, Arrays.asList(null, Float.MIN_VALUE, Float.MAX_VALUE, Float.NaN,
                Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, 14.14f));
        tmp.put(ColumnTypeSpec.DOUBLE, Arrays.asList(null, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 14.14));
        tmp.put(ColumnTypeSpec.DECIMAL, Arrays.asList(null, BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.valueOf(Long.MIN_VALUE),
                BigDecimal.valueOf(Long.MAX_VALUE), new BigDecimal("10000000000000000000000000000000000000")));
        tmp.put(ColumnTypeSpec.DATE, Arrays.asList(null, LocalDate.MIN, LocalDate.MAX, LocalDate.EPOCH, LocalDate.now()));
        tmp.put(ColumnTypeSpec.TIME, Arrays.asList(null, LocalTime.MIN, LocalTime.MAX, LocalTime.MIDNIGHT,
                LocalTime.NOON, LocalTime.now()));
        tmp.put(ColumnTypeSpec.DATETIME, Arrays.asList(null, LocalDateTime.MIN, LocalDateTime.MAX, LocalDateTime.now()));
        tmp.put(ColumnTypeSpec.TIMESTAMP, Arrays.asList(null, Instant.MIN, Instant.MAX, Instant.EPOCH, Instant.now()));
        tmp.put(ColumnTypeSpec.UUID, Arrays.asList(null, UUID.randomUUID()));
        tmp.put(ColumnTypeSpec.BITMASK, Arrays.asList(null, fromBinString(""), fromBinString("1"),
                fromBinString("10101010101010101010101")));
        tmp.put(ColumnTypeSpec.STRING, Arrays.asList(null, "", UUID.randomUUID().toString()));
        tmp.put(ColumnTypeSpec.BLOB, Arrays.asList(null, ArrayUtils.BYTE_EMPTY_ARRAY,
                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
        tmp.put(ColumnTypeSpec.NUMBER, Arrays.asList(null, BigInteger.ONE, BigInteger.ZERO,
                new BigInteger("10000000000000000000000000000000000000")));

        var missedTypes = new HashSet<>(Arrays.asList(ColumnTypeSpec.values()));

        missedTypes.removeAll(tmp.keySet());

        assertThat(missedTypes, empty());

        DEFAULT_VALUES_TO_TEST = Map.copyOf(tmp);
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
            return (T) ((BigDecimal) val).setScale(DecimalColumnType.DEFAULT_SCALE, HALF_UP);
        }

        return val;
    }

    /** Creates a column type from given type spec. */
    protected static ColumnType specToType(ColumnTypeSpec spec) {
        switch (spec) {
            case INT8:
                return ColumnType.INT8;
            case INT16:
                return ColumnType.INT16;
            case INT32:
                return ColumnType.INT32;
            case INT64:
                return ColumnType.INT64;
            case FLOAT:
                return ColumnType.FLOAT;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case DECIMAL:
                return ColumnType.decimal();
            case DATE:
                return ColumnType.DATE;
            case TIME:
                return ColumnType.time();
            case DATETIME:
                return ColumnType.datetime();
            case TIMESTAMP:
                return ColumnType.timestamp();
            case NUMBER:
                return ColumnType.number();
            case STRING:
                return ColumnType.string();
            case UUID:
                return ColumnType.UUID;
            case BLOB:
                return ColumnType.blob();
            case BITMASK:
                return ColumnType.bitmaskOf(10);
            default:
                throw new IllegalStateException("Unknown type spec [spec=" + spec + ']');
        }
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
    protected static class DefaultValueArg {
        final ColumnType type;
        final Object defaultValue;

        /**
         * Constructor.
         *
         * @param type Type of the value
         * @param defaultValue value itself.
         */
        public DefaultValueArg(ColumnType type, Object defaultValue) {
            this.type = type;
            this.defaultValue = defaultValue;
        }

        @Override
        public String toString() {
            return type.typeSpec() + ": " + AbstractSchemaConverterTest.toString(defaultValue);
        }
    }
}
