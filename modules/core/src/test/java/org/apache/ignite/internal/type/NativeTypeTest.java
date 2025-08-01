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

import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.DATE;
import static org.apache.ignite.internal.type.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.type.NativeTypes.FLOAT;
import static org.apache.ignite.internal.type.NativeTypes.INT16;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.UUID;
import static org.apache.ignite.internal.type.NativeTypes.blobOf;
import static org.apache.ignite.internal.type.NativeTypes.datetime;
import static org.apache.ignite.internal.type.NativeTypes.decimalOf;
import static org.apache.ignite.internal.type.NativeTypes.stringOf;
import static org.apache.ignite.internal.type.NativeTypes.time;
import static org.apache.ignite.internal.type.NativeTypes.timestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.sql.ColumnType;
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
        assertTrue(INT32.compareTo(time(4)) < 0);
        assertTrue(time(3).compareTo(time(4)) < 0);
        assertTrue(time(9).compareTo(datetime(0)) < 0);

        assertTrue(datetime(3).compareTo(INT64) < 0);
        assertTrue(INT64.compareTo(datetime(4)) < 0);

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
        assertEquals(6, time(4).sizeInBytes());
        assertEquals(6, time(9).sizeInBytes());

        assertEquals(7, datetime(0).sizeInBytes());
        assertEquals(7, datetime(3).sizeInBytes());
        assertEquals(9, datetime(4).sizeInBytes());
        assertEquals(9, datetime(9).sizeInBytes());

        assertEquals(8, timestamp(0).sizeInBytes());
        assertEquals(12, timestamp(1).sizeInBytes());
        assertEquals(12, timestamp(9).sizeInBytes());
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

    @Test
    public void basicTypeEquality() {
        assertEquals(BOOLEAN, new NativeType(ColumnType.BOOLEAN, 1));

        assertEquals(INT8, new NativeType(ColumnType.INT8, 1));
        assertEquals(INT16, new NativeType(ColumnType.INT16, 2));
        assertEquals(INT32, new NativeType(ColumnType.INT32, 4));
        assertEquals(INT64, new NativeType(ColumnType.INT64, 8));

        assertEquals(FLOAT, new NativeType(ColumnType.FLOAT, 4));
        assertEquals(DOUBLE, new NativeType(ColumnType.DOUBLE, 8));

        assertEquals(DATE, new NativeType(ColumnType.DATE, 3));
        assertEquals(UUID, new NativeType(ColumnType.UUID, 16));
    }

    @Test
    public void temporalTypeEquality() {
        assertEquals(time(0), time(0));
        assertNotEquals(time(1), time(0));
        assertNotEquals(time(0), time(1));

        assertEquals(timestamp(1), timestamp(1));
        assertEquals(timestamp(0), timestamp(0));
        assertNotEquals(timestamp(1), timestamp(0));
        assertNotEquals(timestamp(0), timestamp(1));

        assertEquals(datetime(1), datetime(1));
        assertEquals(datetime(0), datetime(0));
        assertNotEquals(datetime(1), datetime(0));
        assertNotEquals(datetime(0), datetime(1));
    }

    @Test
    public void decimalTypeEquality() {
        assertEquals(decimalOf(10, 2), decimalOf(10, 2));
        assertNotEquals(decimalOf(10, 2), decimalOf(10, 3));
    }

    @Test
    public void stringTypeEquality() {
        assertEquals(stringOf(10), stringOf(10));
        assertEquals(stringOf(2), stringOf(2));

        assertNotEquals(stringOf(10), stringOf(5));
        assertNotEquals(stringOf(5), stringOf(10));
        assertNotEquals(STRING, stringOf(10));
        assertNotEquals(stringOf(10), STRING);
    }

    @Test
    public void blobTypeEquality() {
        assertEquals(blobOf(10), blobOf(10));
        assertEquals(blobOf(2), blobOf(2));

        assertNotEquals(blobOf(10), blobOf(5));
        assertNotEquals(blobOf(5), blobOf(10));
        assertNotEquals(BYTES, blobOf(10));
        assertNotEquals(blobOf(10), BYTES);
    }

    @Test
    public void stringTypeDefaultLength() {
        VarlenNativeType nativeType = (VarlenNativeType) STRING;
        assertEquals(65536, nativeType.length());
    }

    @Test
    public void blobTypeDefaultLength() {
        VarlenNativeType nativeType = (VarlenNativeType) BYTES;
        assertEquals(65536, nativeType.length());
    }
}
