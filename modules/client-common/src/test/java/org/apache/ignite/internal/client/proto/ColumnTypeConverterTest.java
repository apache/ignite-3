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

package org.apache.ignite.internal.client.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

/**
 * Column type converter tests.
 */
public class ColumnTypeConverterTest {
    /**
     * Client protocol relies on this order of column types. Do not change.
     * If a change to ColumnType enum is required, fix ColumnTypeConverter.
     */
    private static final ColumnType[] EXPECTED_VALS = {
            ColumnType.BOOLEAN,
            ColumnType.INT8,
            ColumnType.INT16,
            ColumnType.INT32,
            ColumnType.INT64,
            ColumnType.FLOAT,
            ColumnType.DOUBLE,
            ColumnType.DECIMAL,
            ColumnType.DATE,
            ColumnType.TIME,
            ColumnType.DATETIME,
            ColumnType.TIMESTAMP,
            ColumnType.UUID,
            ColumnType.BITMASK,
            ColumnType.STRING,
            ColumnType.BYTE_ARRAY,
            ColumnType.PERIOD,
            ColumnType.DURATION,
            ColumnType.NUMBER
    };

    @Test
    public void testColumnTypeConverter() {
        for (int i = 0; i < EXPECTED_VALS.length; i++) {
            ColumnType expectedVal = EXPECTED_VALS[i];
            ColumnType actualVal = ColumnTypeConverter.fromOrdinalOrThrow(i);

            assertEquals(expectedVal, actualVal);
        }
    }
}
