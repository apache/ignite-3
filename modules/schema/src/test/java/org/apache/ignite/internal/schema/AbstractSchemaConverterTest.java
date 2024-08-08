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

package org.apache.ignite.internal.schema;

import static java.math.RoundingMode.HALF_UP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

import java.math.BigDecimal;
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
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.util.ArrayUtils;

/**
 * Abstract class for schema converter tests.
 */
public class AbstractSchemaConverterTest extends BaseIgniteAbstractTest {
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
        tmp.put(NativeTypeSpec.STRING, Arrays.asList(null, "", UUID.randomUUID().toString()));
        tmp.put(NativeTypeSpec.BYTES, Arrays.asList(null, ArrayUtils.BYTE_EMPTY_ARRAY,
                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));

        var missedTypes = new HashSet<>(Arrays.asList(NativeTypeSpec.values()));

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
            return (T) ((BigDecimal) val).setScale(CatalogUtils.DEFAULT_SCALE, HALF_UP);
        }

        return val;
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
        public final NativeType type;
        public final Object defaultValue;

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
            return type.spec() + ": " + AbstractSchemaConverterTest.toString(defaultValue);
        }
    }
}
