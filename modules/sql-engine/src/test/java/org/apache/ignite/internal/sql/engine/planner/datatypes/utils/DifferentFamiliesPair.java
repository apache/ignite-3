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

package org.apache.ignite.internal.sql.engine.planner.datatypes.utils;

import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;

/**
 * Enumerates possible pairs of types belonging to a different type families for test purposes.
 */
public enum DifferentFamiliesPair implements TypePair {
    TINYINT_VARCHAR_128(NativeTypes.INT8, Types.VARCHAR_128),
    TINYINT_DATE(NativeTypes.INT8, NativeTypes.DATE),
    TINYINT_TIME_9(NativeTypes.INT8, Types.TIME_9),
    TINYINT_TIMESTAMP_9(NativeTypes.INT8, Types.TIMESTAMP_9),
    TINYINT_TIMESTAMP_WLTZ_9(NativeTypes.INT8, Types.TIMESTAMP_WLTZ_9),
    TINYINT_BOOLEAN(NativeTypes.INT8, NativeTypes.BOOLEAN),
    TINYINT_UUID(NativeTypes.INT8, NativeTypes.UUID),
    TINYINT_VARBINARY_128(NativeTypes.INT8, Types.VARBINARY_128),

    SMALLINT_VARCHAR_128(NativeTypes.INT16, Types.VARCHAR_128),
    SMALLINT_DATE(NativeTypes.INT16, NativeTypes.DATE),
    SMALLINT_TIME_9(NativeTypes.INT16, Types.TIME_9),
    SMALLINT_TIMESTAMP_9(NativeTypes.INT16, Types.TIMESTAMP_9),
    SMALLINT_TIMESTAMP_WLTZ_9(NativeTypes.INT16, Types.TIMESTAMP_WLTZ_9),
    SMALLINT_BOOLEAN(NativeTypes.INT16, NativeTypes.BOOLEAN),
    SMALLINT_UUID(NativeTypes.INT16, NativeTypes.UUID),
    SMALLINT_VARBINARY_128(NativeTypes.INT16, Types.VARBINARY_128),

    INT_VARCHAR_128(NativeTypes.INT32, Types.VARCHAR_128),
    INT_DATE(NativeTypes.INT32, NativeTypes.DATE),
    INT_TIME_9(NativeTypes.INT32, Types.TIME_9),
    INT_TIMESTAMP_9(NativeTypes.INT32, Types.TIMESTAMP_9),
    INT_TIMESTAMP_WLTZ_9(NativeTypes.INT32, Types.TIMESTAMP_WLTZ_9),
    INT_BOOLEAN(NativeTypes.INT32, NativeTypes.BOOLEAN),
    INT_UUID(NativeTypes.INT32, NativeTypes.UUID),
    INT_VARBINARY_128(NativeTypes.INT32, Types.VARBINARY_128),

    BIGINT_VARCHAR_128(NativeTypes.INT64, Types.VARCHAR_128),
    BIGINT_DATE(NativeTypes.INT64, NativeTypes.DATE),
    BIGINT_TIME_9(NativeTypes.INT64, Types.TIME_9),
    BIGINT_TIMESTAMP_9(NativeTypes.INT64, Types.TIMESTAMP_9),
    BIGINT_TIMESTAMP_WLTZ_9(NativeTypes.INT64, Types.TIMESTAMP_WLTZ_9),
    BIGINT_BOOLEAN(NativeTypes.INT64, NativeTypes.BOOLEAN),
    BIGINT_UUID(NativeTypes.INT64, NativeTypes.UUID),
    BIGINT_VARBINARY_128(NativeTypes.INT64, Types.VARBINARY_128),

    DECIMAL_4_2_VARCHAR_128(Types.DECIMAL_4_2, Types.VARCHAR_128),
    DECIMAL_4_2_DATE(Types.DECIMAL_4_2, NativeTypes.DATE),
    DECIMAL_4_2_TIME_9(Types.DECIMAL_4_2, Types.TIME_9),
    DECIMAL_4_2_TIMESTAMP_9(Types.DECIMAL_4_2, Types.TIMESTAMP_9),
    DECIMAL_4_2_TIMESTAMP_WLTZ_9(Types.DECIMAL_4_2, Types.TIMESTAMP_WLTZ_9),
    DECIMAL_4_2_BOOLEAN(Types.DECIMAL_4_2, NativeTypes.BOOLEAN),
    DECIMAL_4_2_UUID(Types.DECIMAL_4_2, NativeTypes.UUID),
    DECIMAL_4_2_VARBINARY_128(Types.DECIMAL_4_2, Types.VARBINARY_128),

    REAL_VARCHAR_128(NativeTypes.FLOAT, Types.VARCHAR_128),
    REAL_DATE(NativeTypes.FLOAT, NativeTypes.DATE),
    REAL_TIME_9(NativeTypes.FLOAT, Types.TIME_9),
    REAL_TIMESTAMP_9(NativeTypes.FLOAT, Types.TIMESTAMP_9),
    REAL_TIMESTAMP_WLTZ_9(NativeTypes.FLOAT, Types.TIMESTAMP_WLTZ_9),
    REAL_BOOLEAN(NativeTypes.FLOAT, NativeTypes.BOOLEAN),
    REAL_UUID(NativeTypes.FLOAT, NativeTypes.UUID),
    REAL_VARBINARY_128(NativeTypes.FLOAT, Types.VARBINARY_128),

    DOUBLE_VARCHAR_128(NativeTypes.DOUBLE, Types.VARCHAR_128),
    DOUBLE_DATE(NativeTypes.DOUBLE, NativeTypes.DATE),
    DOUBLE_TIME_9(NativeTypes.DOUBLE, Types.TIME_9),
    DOUBLE_TIMESTAMP_9(NativeTypes.DOUBLE, Types.TIMESTAMP_9),
    DOUBLE_TIMESTAMP_WLTZ_9(NativeTypes.DOUBLE, Types.TIMESTAMP_WLTZ_9),
    DOUBLE_BOOLEAN(NativeTypes.DOUBLE, NativeTypes.BOOLEAN),
    DOUBLE_UUID(NativeTypes.DOUBLE, NativeTypes.UUID),
    DOUBLE_VARBINARY_128(NativeTypes.DOUBLE, Types.VARBINARY_128),

    VARCHAR_9_DATE(Types.VARCHAR_128, NativeTypes.DATE),
    VARCHAR_9_TIME_9(Types.VARCHAR_128, Types.TIME_9),
    VARCHAR_128_TIMESTAMP_9(Types.VARCHAR_128, Types.TIMESTAMP_9),
    VARCHAR_9_TIMESTAMP_WLTZ_9(Types.VARCHAR_128, Types.TIMESTAMP_WLTZ_9),
    VARCHAR_128_BOOLEAN(Types.VARCHAR_128, NativeTypes.BOOLEAN),
    VARCHAR_128_UUID(Types.VARCHAR_128, NativeTypes.UUID),
    VARCHAR_128_VARBINARY_128(Types.VARCHAR_128, Types.VARBINARY_128),

    DATE_TIME_9(NativeTypes.DATE, Types.TIME_9),
    DATE_TIMESTAMP_9(NativeTypes.DATE, Types.TIMESTAMP_9),
    DATE_TIMESTAMP_WLTZ_9(NativeTypes.DATE, Types.TIMESTAMP_WLTZ_9),
    DATE_BOOLEAN(NativeTypes.DATE, NativeTypes.BOOLEAN),
    DATE_UUID(NativeTypes.DATE, NativeTypes.UUID),
    DATE_VARBINARY_128(NativeTypes.DATE, Types.VARBINARY_128),

    TIME_9_TIMESTAMP_9(Types.TIME_9, Types.TIMESTAMP_9),
    TIME_9_TIMESTAMP_WLTZ_9(Types.TIME_9, Types.TIMESTAMP_WLTZ_9),
    TIME_9_BOOLEAN(Types.TIME_9, NativeTypes.BOOLEAN),
    TIME_9_UUID(Types.TIME_9, NativeTypes.UUID),
    TIME_9_VARBINARY_128(Types.TIME_9, Types.VARBINARY_128),

    TIMESTAMP_9_BOOLEAN(Types.TIMESTAMP_9, NativeTypes.BOOLEAN),
    TIMESTAMP_9_UUID(Types.TIMESTAMP_9, NativeTypes.UUID),
    TIMESTAMP_9_VARBINARY_9(Types.TIMESTAMP_9, Types.VARBINARY_128),

    TIMESTAMP_WLTZ_9_BOOLEAN(Types.TIMESTAMP_WLTZ_9, NativeTypes.BOOLEAN),
    TIMESTAMP_WLTZ_9_UUID(Types.TIMESTAMP_WLTZ_9, NativeTypes.UUID),
    TIMESTAMP_WLTZ_9_VARBINARY_128(Types.TIMESTAMP_WLTZ_9, Types.VARBINARY_128),

    BOOLEAN_UUID(NativeTypes.BOOLEAN, NativeTypes.UUID),
    BOOLEAN_VARBINARY_128(NativeTypes.BOOLEAN, Types.VARBINARY_128),

    UUID_VARBINARY_128(NativeTypes.UUID, Types.VARBINARY_128),

    ;

    private final NativeType first;
    private final NativeType second;

    DifferentFamiliesPair(NativeType first, NativeType second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public NativeType first() {
        return first;
    }

    @Override
    public NativeType second() {
        return second;
    }
}
