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

import static org.apache.ignite.internal.util.TemporalTypeUtils.normalizeNanos;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.BitmaskNativeType;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.TemporalNativeType;

/**
 * Test utility class.
 */
public final class SchemaTestUtils {
    /** Min year boundary. */
    private static final int MIN_YEAR = -(1 << 14);

    /** Max year boundary. */
    private static final int MAX_YEAR = (1 << 14) - 1;

    /** All types for tests. */
    public static final List<NativeType> ALL_TYPES = List.of(
            NativeTypes.BOOLEAN,
            NativeTypes.INT8,
            NativeTypes.INT16,
            NativeTypes.INT32,
            NativeTypes.INT64,
            NativeTypes.FLOAT,
            NativeTypes.DOUBLE,
            NativeTypes.DATE,
            NativeTypes.UUID,
            NativeTypes.numberOf(20),
            NativeTypes.decimalOf(25, 5),
            NativeTypes.bitmaskOf(22),
            NativeTypes.time(0),
            NativeTypes.datetime(3),
            NativeTypes.timestamp(3),
            NativeTypes.BYTES,
            NativeTypes.STRING);

    /**
     * Generates random value of given type.
     *
     * @param rnd  Random generator.
     * @param type Type.
     * @return Random object of asked type.
     */
    public static Object generateRandomValue(Random rnd, NativeType type) {
        switch (type.spec()) {
            case BOOLEAN:
                return rnd.nextBoolean();

            case INT8:
                return (byte) rnd.nextInt(255);

            case INT16:
                return (short) rnd.nextInt(65535);

            case INT32:
                return rnd.nextInt();

            case INT64:
                return rnd.nextLong();

            case FLOAT:
                return rnd.nextFloat();

            case DOUBLE:
                return rnd.nextDouble();

            case UUID:
                return new UUID(rnd.nextLong(), rnd.nextLong());

            case STRING:
                return IgniteTestUtils.randomString(rnd, rnd.nextInt(255));

            case BYTES:
                return IgniteTestUtils.randomBytes(rnd, rnd.nextInt(255));

            case NUMBER:
                return BigInteger.probablePrime(12, rnd);

            case DECIMAL:
                DecimalNativeType decimalType = (DecimalNativeType) type;

                return BigDecimal.valueOf(rnd.nextInt(), decimalType.scale());

            case BITMASK: {
                BitmaskNativeType maskType = (BitmaskNativeType) type;

                return IgniteTestUtils.randomBitSet(rnd, maskType.bits());
            }

            case DATE: {
                Year year = Year.of(rnd.nextInt(MAX_YEAR - MIN_YEAR) + MIN_YEAR);

                return LocalDate.ofYearDay(year.getValue(), rnd.nextInt(year.length()) + 1);
            }

            case TIME:
                return LocalTime.of(rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60),
                        normalizeNanos(rnd.nextInt(1_000_000_000), ((TemporalNativeType) type).precision()));

            case DATETIME: {
                Year year = Year.of(rnd.nextInt(MAX_YEAR - MIN_YEAR) + MIN_YEAR);

                LocalDate date = LocalDate.ofYearDay(year.getValue(), rnd.nextInt(year.length()) + 1);
                LocalTime time = LocalTime.of(rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60),
                        normalizeNanos(rnd.nextInt(1_000_000_000), ((TemporalNativeType) type).precision()));

                return LocalDateTime.of(date, time);
            }

            case TIMESTAMP:
                return Instant.ofEpochMilli(rnd.nextLong()).truncatedTo(ChronoUnit.SECONDS)
                        .plusNanos(normalizeNanos(rnd.nextInt(1_000_000_000), ((TemporalNativeType) type).precision()));

            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    /** Creates a native type from given type spec. */
    public static NativeType specToType(NativeTypeSpec spec) {
        switch (spec) {
            case BOOLEAN:
                return NativeTypes.BOOLEAN;
            case INT8:
                return NativeTypes.INT8;
            case INT16:
                return NativeTypes.INT16;
            case INT32:
                return NativeTypes.INT32;
            case INT64:
                return NativeTypes.INT64;
            case FLOAT:
                return NativeTypes.FLOAT;
            case DOUBLE:
                return NativeTypes.DOUBLE;
            case DECIMAL:
                return NativeTypes.decimalOf(10, 3);
            case DATE:
                return NativeTypes.DATE;
            case TIME:
                return NativeTypes.time(0);
            case DATETIME:
                return NativeTypes.datetime(3);
            case TIMESTAMP:
                return NativeTypes.timestamp(3);
            case NUMBER:
                return NativeTypes.numberOf(10);
            case STRING:
                return NativeTypes.stringOf(8);
            case UUID:
                return NativeTypes.UUID;
            case BYTES:
                return NativeTypes.blobOf(8);
            case BITMASK:
                return NativeTypes.bitmaskOf(16);
            default:
                throw new IllegalStateException("Unknown type spec [spec=" + spec + ']');
        }
    }

    /**
     * Ensure specified columns contains all type spec, presented in NativeTypeSpec.
     *
     * @param allColumns Columns to test.
     */
    public static void ensureAllTypesChecked(Stream<Column> allColumns) {
        Set<NativeTypeSpec> testedTypes = allColumns.map(c -> c.type().spec())
                .collect(Collectors.toSet());

        Set<NativeTypeSpec> missedTypes = Arrays.stream(NativeTypeSpec.values())
                .filter(t -> !testedTypes.contains(t)).collect(Collectors.toSet());

        assertEquals(Collections.emptySet(), missedTypes);
    }

    /**
     * Creates a {@link BinaryRow} according to the scheme.
     *
     * @param schema Schema.
     * @param values Values.
     */
    public static BinaryRow binaryRow(SchemaDescriptor schema, Object... values) {
        var rowAssembler = new RowAssembler(schema, -1);

        for (Object value : values) {
            rowAssembler.appendValue(value);
        }

        return new BinaryRowImpl(schema.version(), rowAssembler.build().tupleSlice());
    }
}
