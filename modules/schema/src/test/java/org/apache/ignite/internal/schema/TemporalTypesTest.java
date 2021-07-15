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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

        assertThrows(AssertionError.class, () -> checkTime(LocalTime.MAX));
        assertThrows(AssertionError.class, () -> checkDate(maxDate.plusDays(1)));
        assertThrows(AssertionError.class, () -> checkDate(minDate.minusDays(1)));
    }

    /**
     * Check time boundaries.
     */
    @Test
    void testTime() {
        checkTime(LocalTime.MAX.truncatedTo(TemporalTypesHelper.TIME_PRECISION)); // Millis precision.
        checkTime(LocalTime.MIN);

        assertThrows(AssertionError.class, () -> checkTime(LocalTime.MAX));
    }

    /**
     * @param date Date.
     */
    private void checkDate(LocalDate date) {
        assertEquals(date, TemporalTypesHelper.decodeDate(TemporalTypesHelper.encodeDate(date)));
    }

    /**
     * @param time Time.
     */
    private void checkTime(LocalTime time) {
        assertEquals(time, TemporalTypesHelper.decodeTime(TemporalTypesHelper.compactTime(time)));
    }
}
