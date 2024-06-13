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

package org.apache.ignite.internal.type;

import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.DATE;
import static org.apache.ignite.internal.type.NativeTypes.FLOAT;
import static org.apache.ignite.internal.type.NativeTypes.INT16;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.UUID;
import static org.apache.ignite.internal.type.NativeTypes.datetime;
import static org.apache.ignite.internal.type.NativeTypes.time;
import static org.apache.ignite.internal.type.NativeTypes.timestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * NativeTypeTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class NativeTypeTest {
    /**
     * CompareFixlenTypesVsVarlenTypes.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Test
    public void compareFixlenTypesVsVarlenTypes() {
        assertTrue(INT8.compareTo(STRING) < 0);
        assertTrue(INT8.compareTo(BYTES) < 0);

        assertTrue(INT32.compareTo(STRING) < 0);
        assertTrue(INT32.compareTo(BYTES) < 0);

        assertTrue(INT64.compareTo(STRING) < 0);
        assertTrue(INT64.compareTo(BYTES) < 0);

        assertTrue(UUID.compareTo(STRING) < 0);
        assertTrue(UUID.compareTo(BYTES) < 0);
    }

    /**
     * CompareFixlenTypesBySize.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Test
    public void compareFixlenTypesBySize() {
        assertTrue(INT16.compareTo(INT32) < 0);
        assertTrue(INT32.compareTo(INT64) < 0);
        assertTrue(INT64.compareTo(UUID) < 0);

        assertTrue(INT16.compareTo(DATE) < 0);
        assertTrue(DATE.compareTo(INT32) < 0);

        assertTrue(DATE.compareTo(time(0)) < 0);
        assertTrue(INT32.compareTo(time(3)) < 0);
        assertFalse(time(2).compareTo(time(3)) < 0);
        assertTrue(time(3).compareTo(datetime(0)) < 0);

        assertTrue(datetime(3).compareTo(INT64) < 0);
        assertFalse(INT64.compareTo(datetime(3)) < 0);

        assertTrue(INT64.compareTo(timestamp(1)) < 0);
    }

    /**
     * CompareFixlenTypesByDesc.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Test
    public void compareFixlenTypesByDesc() {
        assertTrue(FLOAT.compareTo(INT32) < 0);
        assertTrue(datetime(0).compareTo(INT64) < 0);
        assertTrue(INT32.compareTo(time(0)) < 0);
        assertTrue(INT32.compareTo(time(3)) < 0);
        assertTrue(INT64.compareTo(timestamp(0)) < 0);
    }

    /**
     * Validate data size of datetime types.
     */
    @Test
    public void validateTemporalTypesLength() {
        assertEquals(3, DATE.sizeInBytes());

        assertEquals(4, time(0).sizeInBytes());
        assertEquals(4, time(3).sizeInBytes());
        //assertEquals(6, time(4).sizeInBytes());
        //assertEquals(6, time(9).sizeInBytes());

        assertEquals(7, datetime(0).sizeInBytes());
        assertEquals(7, datetime(3).sizeInBytes());
        //assertEquals(9, datetime(4).sizeInBytes());
        //assertEquals(9, datetime(9).sizeInBytes());

        assertEquals(8, timestamp(0).sizeInBytes());
        assertEquals(12, timestamp(1).sizeInBytes());
        //assertEquals(12, timestamp(9).sizeInBytes());
    }

    /**
     * InvalidTemporalTypes.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Test
    public void invalidTemporalTypes() {
        assertThrows(IllegalArgumentException.class, () -> time(-1));
        assertThrows(IllegalArgumentException.class, () -> timestamp(-1));
        assertThrows(IllegalArgumentException.class, () -> datetime(-1));

        assertThrows(IllegalArgumentException.class, () -> time(10));
        assertThrows(IllegalArgumentException.class, () -> timestamp(10));
        assertThrows(IllegalArgumentException.class, () -> datetime(10));
    }

    /**
     * CompareVarlenTypesByDesc.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Test
    public void compareVarlenTypesByDesc() {
        assertTrue(BYTES.compareTo(STRING) < 0);
    }

    /**
     * Validate data size of bitmap type.
     */
    @Test
    public void bitmaskSizeTest() {
        assertEquals(1, new BitmaskNativeType(0).sizeInBytes());

        assertEquals(1, new BitmaskNativeType(8).sizeInBytes());

        assertEquals(2, new BitmaskNativeType(9).sizeInBytes());
    }
}
