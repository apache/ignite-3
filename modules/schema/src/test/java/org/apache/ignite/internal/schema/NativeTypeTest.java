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
import static org.apache.ignite.internal.schema.NativeTypes.DATETIME;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.TIME;
import static org.apache.ignite.internal.schema.NativeTypes.TIMESTAMP;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.apache.ignite.internal.schema.NativeTypes.bitmaskOf;
import static org.apache.ignite.internal.schema.NativeTypes.blobOf;
import static org.apache.ignite.internal.schema.NativeTypes.from;
import static org.apache.ignite.internal.schema.NativeTypes.stringOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        assertTrue(DATE.compareTo(TIME) < 0);
        assertTrue(DATE.compareTo(INT32) < 0);
        assertTrue(TIME.compareTo(DATETIME) < 0);
        assertTrue(DATETIME.compareTo(INT64) < 0);
    }

    /**
     *
     */
    @Test
    public void compareFixlenTypesByDesc() {
        assertTrue(FLOAT.compareTo(INT32) < 0);
        assertTrue(INT64.compareTo(TIMESTAMP) < 0);
        assertTrue(INT32.compareTo(TIME) < 0);
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
        assertEquals(TIME, from(ColumnType.TIME));
        assertEquals(DATE, from(ColumnType.DATE));
        assertEquals(DATETIME, from(ColumnType.DATETIME));
        assertEquals(TIMESTAMP, from(ColumnType.TIMESTAMP));
        assertEquals(BYTES, from(ColumnType.blobOf()));
        assertEquals(STRING, from(ColumnType.string()));

        for (int i = 1; i < 800; i += 100) {
            assertEquals(blobOf(i), from(ColumnType.blobOf(i)));
            assertEquals(stringOf(i), from(ColumnType.stringOf(i)));
            assertEquals(bitmaskOf(i), from(ColumnType.bitmaskOf(i)));
        }
    }
}
