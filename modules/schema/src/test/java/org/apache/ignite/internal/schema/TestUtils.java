/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.temporal.ChronoUnit;
import java.util.BitSet;
import java.util.Random;

import static org.apache.ignite.internal.schema.row.TemporalTypesHelper.MAX_YEAR;
import static org.apache.ignite.internal.schema.row.TemporalTypesHelper.MIN_YEAR;
import static org.apache.ignite.internal.schema.row.TemporalTypesHelper.normalizeNanos;

/**
 * Test utility class.
 */
public final class TestUtils {
    /**
     * Generates random value of given type.
     *
     * @param rnd Random generator.
     * @param type Type.
     * @return Random object of asked type.
     */
    public static Object generateRandomValue(Random rnd, NativeType type) {
        switch (type.spec()) {
            case INT8:
                return (byte)rnd.nextInt(255);

            case INT16:
                return (short)rnd.nextInt(65535);

            case INT32:
                return rnd.nextInt();

            case INT64:
                return rnd.nextLong();

            case FLOAT:
                return rnd.nextFloat();

            case DOUBLE:
                return rnd.nextDouble();

            case UUID:
                return new java.util.UUID(rnd.nextLong(), rnd.nextLong());

            case STRING:
                return randomString(rnd, rnd.nextInt(255));

            case BYTES:
                return randomBytes(rnd, rnd.nextInt(255));

            case NUMBER:
                return BigInteger.probablePrime(12, rnd);

            case DECIMAL:
                return BigDecimal.valueOf(rnd.nextInt(), 3);

            case BITMASK: {
                BitmaskNativeType maskType = (BitmaskNativeType)type;

                return randomBitSet(rnd, maskType.bits());
            }

            case DATE: {
                Year year = Year.of(rnd.nextInt(MAX_YEAR - MIN_YEAR) + MIN_YEAR);

                return LocalDate.ofYearDay(year.getValue(), rnd.nextInt(year.length()) + 1);
            }

            case TIME:
                return LocalTime.of(rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60),
                    normalizeNanos(rnd.nextInt(1_000_000_000), ((TemporalNativeType)type).precision()));

            case DATETIME: {
                Year year = Year.of(rnd.nextInt(MAX_YEAR - MIN_YEAR) + MIN_YEAR);

                LocalDate date = LocalDate.ofYearDay(year.getValue(), rnd.nextInt(year.length()) + 1);
                LocalTime time = LocalTime.of(rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60),
                    normalizeNanos(rnd.nextInt(1_000_000_000), ((TemporalNativeType)type).precision()));

                return LocalDateTime.of(date, time);
            }

            case TIMESTAMP:
                return Instant.ofEpochMilli(rnd.nextLong()).truncatedTo(ChronoUnit.SECONDS)
                    .plusNanos(normalizeNanos(rnd.nextInt(1_000_000_000), ((TemporalNativeType)type).precision()));

            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    /**
     * @param rnd Random generator.
     * @param bits Amount of bits in bitset.
     * @return Random BitSet.
     */
    public static BitSet randomBitSet(Random rnd, int bits) {
        BitSet set = new BitSet();

        for (int i = 0; i < bits; i++) {
            if (rnd.nextBoolean())
                set.set(i);
        }

        return set;
    }

    /**
     * @param rnd Random generator.
     * @param len Byte array length.
     * @return Random byte array.
     */
    public static byte[] randomBytes(Random rnd, int len) {
        byte[] data = new byte[len];
        rnd.nextBytes(data);

        return data;
    }

    /**
     * @param rnd Random generator.
     * @param len String length.
     * @return Random string.
     */
    public static String randomString(Random rnd, int len) {
        StringBuilder sb = new StringBuilder();

        while (sb.length() < len) {
            char pt = (char)rnd.nextInt(Character.MAX_VALUE + 1);

            if (Character.isDefined(pt) &&
                Character.getType(pt) != Character.PRIVATE_USE &&
                !Character.isSurrogate(pt))
                sb.append(pt);
        }

        return sb.toString();
    }

    /**
     * Stub.
     */
    private TestUtils() {
    }
}
