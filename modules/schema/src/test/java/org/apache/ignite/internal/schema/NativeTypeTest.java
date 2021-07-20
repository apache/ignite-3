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

import org.apache.ignite.schema.ColumnType;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DATE;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.apache.ignite.internal.schema.NativeTypes.bitmaskOf;
import static org.apache.ignite.internal.schema.NativeTypes.blobOf;
import static org.apache.ignite.internal.schema.NativeTypes.datetime;
import static org.apache.ignite.internal.schema.NativeTypes.from;
import static org.apache.ignite.internal.schema.NativeTypes.stringOf;
import static org.apache.ignite.internal.schema.NativeTypes.time;
import static org.apache.ignite.internal.schema.NativeTypes.timestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class NativeTypeTest {
    /**
     *
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
     *
     */
    @Test
    public void compareFixlenTypesBySize() {
        assertTrue(INT16.compareTo(INT32) < 0);
        assertTrue(INT32.compareTo(INT64) < 0);
        assertTrue(INT64.compareTo(UUID) < 0);

        assertTrue(INT16.compareTo(DATE) < 0);
        assertTrue(DATE.compareTo(INT32) < 0);

        assertTrue(DATE.compareTo(NativeTypes.time(3)) < 0);
        assertTrue(NativeTypes.time(3).compareTo(NativeTypes.time(6)) < 0);
        assertTrue(NativeTypes.time(6).compareTo(NativeTypes.time(9)) < 0);
        assertTrue(NativeTypes.time(9).compareTo(NativeTypes.datetime(3)) < 0);

        assertTrue(NativeTypes.datetime(3).compareTo(NativeTypes.datetime(6)) < 0);
        assertTrue(NativeTypes.datetime(6).compareTo(NativeTypes.datetime(9)) < 0);
        assertTrue(INT64.compareTo(NativeTypes.datetime(9)) < 0);

        assertTrue(INT64.compareTo(NativeTypes.timestamp(3)) < 0);
        assertTrue(NativeTypes.timestamp(3).compareTo(NativeTypes.timestamp(6)) < 0);
        assertTrue(NativeTypes.timestamp(6).compareTo(NativeTypes.timestamp(9)) < 0);

        assertEquals(0, NativeTypes.datetime().compareTo(NativeTypes.datetime(6)));
        assertEquals(0, NativeTypes.time().compareTo(NativeTypes.time(6)));
        assertEquals(0, NativeTypes.timestamp().compareTo(NativeTypes.timestamp(6)));
    }

    /**
     *
     */
    @Test
    public void compareFixlenTypesByDesc() {
        assertTrue(FLOAT.compareTo(INT32) < 0);
        assertTrue(datetime(6).compareTo(INT64) < 0);
        assertTrue(INT32.compareTo(NativeTypes.time(3)) < 0);
    }

    /**
     *
     */
    @Test
    public void validateTemporalTypesLength() {
        assertEquals(3, DATE.sizeInBytes());

        assertEquals(5, time().sizeInBytes());
        assertEquals(4, time(3).sizeInBytes());
        assertEquals(5, time(6).sizeInBytes());
        assertEquals(6, time(9).sizeInBytes());

        assertEquals(8, datetime().sizeInBytes());
        assertEquals(7, datetime(3).sizeInBytes());
        assertEquals(8, datetime(6).sizeInBytes());
        assertEquals(9, datetime(9).sizeInBytes());

        assertEquals(11, timestamp().sizeInBytes());
        assertEquals(10, timestamp(3).sizeInBytes());
        assertEquals(11, timestamp(6).sizeInBytes());
        assertEquals(12, timestamp(9).sizeInBytes());
    }

    /**
     *
     */
    @Test
    public void invalidTemporalTypes() {
        assertThrows(IllegalArgumentException.class, () -> time(0));
        assertThrows(IllegalArgumentException.class, () -> timestamp(0));
        assertThrows(IllegalArgumentException.class, () -> datetime(0));

        assertThrows(IllegalArgumentException.class, () -> time(-3));
        assertThrows(IllegalArgumentException.class, () -> timestamp(-3));
        assertThrows(IllegalArgumentException.class, () -> datetime(-3));

        assertThrows(IllegalArgumentException.class, () -> time(2));
        assertThrows(IllegalArgumentException.class, () -> timestamp(2));
        assertThrows(IllegalArgumentException.class, () -> datetime(2));
    }

    /**
     *
     */
    @Test
    public void compareVarlenTypesByDesc() {
        assertTrue(BYTES.compareTo(STRING) < 0);
    }

    /**
     *
     */
    @Test
    public void createNativeTypeFromColumnType() {
        assertEquals(INT8, from(ColumnType.INT8));
        assertEquals(INT16, from(ColumnType.INT16));
        assertEquals(INT32, from(ColumnType.INT32));
        assertEquals(INT64, from(ColumnType.INT64));
        assertEquals(FLOAT, from(ColumnType.FLOAT));
        assertEquals(DOUBLE, from(ColumnType.DOUBLE));
        assertEquals(DATE, from(ColumnType.DATE));
        assertEquals(BYTES, from(ColumnType.blobOf()));
        assertEquals(STRING, from(ColumnType.string()));

        assertEquals(time(), from(ColumnType.time(ColumnType.TemporalColumnType.DEFAULT_PRECISION)));
        assertEquals(datetime(), from(ColumnType.datetime(ColumnType.TemporalColumnType.DEFAULT_PRECISION)));
        assertEquals(timestamp(), from(ColumnType.timestamp(ColumnType.TemporalColumnType.DEFAULT_PRECISION)));

        for (int i = 1; i < 800; i += 100) {
            assertEquals(blobOf(i), from(ColumnType.blobOf(i)));
            assertEquals(stringOf(i), from(ColumnType.stringOf(i)));
            assertEquals(bitmaskOf(i), from(ColumnType.bitmaskOf(i)));
        }

        for (int i = 3; i <= 9; i += 3) {
            assertEquals(time(i), from(ColumnType.time(i)));
            assertEquals(datetime(i), from(ColumnType.datetime(i)));
            assertEquals(timestamp(i), from(ColumnType.timestamp(i)));
        }
    }
}
