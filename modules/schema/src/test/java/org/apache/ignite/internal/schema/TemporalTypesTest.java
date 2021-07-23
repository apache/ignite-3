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

import java.time.LocalDate;
import java.time.LocalTime;
import org.apache.ignite.internal.schema.row.TemporalTypesHelper;
import org.apache.ignite.schema.ColumnType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test temporal type compaction.
 */
public class TemporalTypesTest {
    /**
     * Check date boundaries.
     */
    @Test
    void testDate() {
        checkDate(LocalDate.of(0, 1, 1));
        checkDate(LocalDate.of(-1, 1, 11));

        LocalDate maxDate = LocalDate.of(TemporalTypesHelper.MAX_YEAR, 12, 31);
        LocalDate minDate = LocalDate.of(TemporalTypesHelper.MIN_YEAR, 1, 1);

        checkDate(maxDate);
        checkDate(minDate);

        assertThrows(AssertionError.class, () -> checkTime(TemporalNativeType.time(ColumnType.TemporalColumnType.DEFAULT_PRECISION), LocalTime.MAX));
        assertThrows(AssertionError.class, () -> checkDate(maxDate.plusDays(1)));
        assertThrows(AssertionError.class, () -> checkDate(minDate.minusDays(1)));
    }

    /**
     * Check time boundaries.
     */
    @Test
    void testTime() {
        TemporalNativeType type = TemporalNativeType.time(ColumnType.TemporalColumnType.DEFAULT_PRECISION);

        checkTime(type, LocalTime.MAX.truncatedTo(TemporalTypesHelper.TIME_PRECISION));
        checkTime(type, LocalTime.MIN);

        checkTime(TemporalNativeType.time(0), LocalTime.MAX.truncatedTo(TestUtils.chronoUnitForPrecision(0))); // Seconds precision.
        checkTime(TemporalNativeType.time(0), LocalTime.MIN);

        checkTime(TemporalNativeType.time(3), LocalTime.MAX.truncatedTo(TestUtils.chronoUnitForPrecision(3))); // Millis precision.
        checkTime(TemporalNativeType.time(3), LocalTime.MIN);

        checkTime(TemporalNativeType.time(6), LocalTime.MAX.truncatedTo(TestUtils.chronoUnitForPrecision(6))); // Micros precision.
        checkTime(TemporalNativeType.time(6), LocalTime.MIN);

        checkTime(TemporalNativeType.time(9), LocalTime.MAX.truncatedTo(TestUtils.chronoUnitForPrecision(9))); // Nanos precision.
        checkTime(TemporalNativeType.time(9), LocalTime.MIN);

        assertThrows(AssertionError.class, () -> checkTime(type, LocalTime.MAX));
        assertThrows(AssertionError.class, () -> checkTime(TemporalNativeType.time(3), LocalTime.MAX));
        assertThrows(AssertionError.class, () -> checkTime(TemporalNativeType.time(6), LocalTime.MAX));
        checkTime(TemporalNativeType.time(9), LocalTime.MAX);

        assertThrows(IllegalArgumentException.class, () -> checkTime(TemporalNativeType.time(10), LocalTime.MAX));
    }

    /**
     * @param date Date.
     */
    private void checkDate(LocalDate date) {
        assertEquals(date, TemporalTypesHelper.decodeDate(TemporalTypesHelper.encodeDate(date)));
    }

    /**
     * @param type Type to validate against.
     * @param time Time value.
     */
    private void checkTime(TemporalNativeType type, LocalTime time) {
        assertEquals(time, TemporalTypesHelper.decodeTime(TemporalTypesHelper.encodeTime(type, time)));
    }
}
