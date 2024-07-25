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
 * Enumerates possible numeric type pairs for test purposes.
 *
 * <p>This is an upper triangular matrix of numeric, where types are enumerated from
 * lesser (INT8) to greater (DOUBLE). That is, this enumeration contains
 * pair (INT8, INT16), but not (INT16. INT8).
 */
public enum NumericPair implements TypePair {
    TINYINT_TINYINT(NativeTypes.INT8, NativeTypes.INT8),
    TINYINT_SMALLINT(NativeTypes.INT8, NativeTypes.INT16),
    TINYINT_INT(NativeTypes.INT8, NativeTypes.INT32),
    TINYINT_BIGINT(NativeTypes.INT8, NativeTypes.INT64),
    TINYINT_DECIMAL_1_0(NativeTypes.INT8, Types.DECIMAL_1_0),
    TINYINT_DECIMAL_2_1(NativeTypes.INT8, Types.DECIMAL_2_1),
    TINYINT_DECIMAL_4_3(NativeTypes.INT8, Types.DECIMAL_4_3),
    TINYINT_DECIMAL_2_0(NativeTypes.INT8, Types.DECIMAL_2_0),
    TINYINT_DECIMAL_3_1(NativeTypes.INT8, Types.DECIMAL_3_1),
    TINYINT_DECIMAL_5_3(NativeTypes.INT8, Types.DECIMAL_5_3),
    TINYINT_DECIMAL_5_0(NativeTypes.INT8, Types.DECIMAL_5_0),
    TINYINT_DECIMAL_6_1(NativeTypes.INT8, Types.DECIMAL_6_1),
    TINYINT_DECIMAL_8_3(NativeTypes.INT8, Types.DECIMAL_8_3),
    TINYINT_REAL(NativeTypes.INT8, NativeTypes.FLOAT),
    TINYINT_DOUBLE(NativeTypes.INT8, NativeTypes.DOUBLE),

    SMALLINT_SMALLINT(NativeTypes.INT16, NativeTypes.INT16),
    SMALLINT_INT(NativeTypes.INT16, NativeTypes.INT32),
    SMALLINT_BIGINT(NativeTypes.INT16, NativeTypes.INT64),
    SMALLINT_DECIMAL_1_0(NativeTypes.INT16, Types.DECIMAL_1_0),
    SMALLINT_DECIMAL_2_1(NativeTypes.INT16, Types.DECIMAL_2_1),
    SMALLINT_DECIMAL_4_3(NativeTypes.INT16, Types.DECIMAL_4_3),
    SMALLINT_DECIMAL_2_0(NativeTypes.INT16, Types.DECIMAL_2_0),
    SMALLINT_DECIMAL_3_1(NativeTypes.INT16, Types.DECIMAL_3_1),
    SMALLINT_DECIMAL_5_3(NativeTypes.INT16, Types.DECIMAL_5_3),
    SMALLINT_DECIMAL_5_0(NativeTypes.INT16, Types.DECIMAL_5_0),
    SMALLINT_DECIMAL_6_1(NativeTypes.INT16, Types.DECIMAL_6_1),
    SMALLINT_DECIMAL_8_3(NativeTypes.INT16, Types.DECIMAL_8_3),
    SMALLINT_REAL(NativeTypes.INT16, NativeTypes.FLOAT),
    SMALLINT_DOUBLE(NativeTypes.INT16, NativeTypes.DOUBLE),

    INT_INT(NativeTypes.INT32, NativeTypes.INT32),
    INT_BIGINT(NativeTypes.INT32, NativeTypes.INT64),
    INT_DECIMAL_1_0(NativeTypes.INT32, Types.DECIMAL_1_0),
    INT_DECIMAL_2_1(NativeTypes.INT32, Types.DECIMAL_2_1),
    INT_DECIMAL_4_3(NativeTypes.INT32, Types.DECIMAL_4_3),
    INT_DECIMAL_2_0(NativeTypes.INT32, Types.DECIMAL_2_0),
    INT_DECIMAL_3_1(NativeTypes.INT32, Types.DECIMAL_3_1),
    INT_DECIMAL_5_3(NativeTypes.INT32, Types.DECIMAL_5_3),
    INT_DECIMAL_5_0(NativeTypes.INT32, Types.DECIMAL_5_0),
    INT_DECIMAL_6_1(NativeTypes.INT32, Types.DECIMAL_6_1),
    INT_DECIMAL_8_3(NativeTypes.INT32, Types.DECIMAL_8_3),
    INT_REAL(NativeTypes.INT32, NativeTypes.FLOAT),
    INT_DOUBLE(NativeTypes.INT32, NativeTypes.DOUBLE),

    BIGINT_BIGINT(NativeTypes.INT64, NativeTypes.INT64),
    BIGINT_DECIMAL_1_0(NativeTypes.INT64, Types.DECIMAL_1_0),
    BIGINT_DECIMAL_2_1(NativeTypes.INT64, Types.DECIMAL_2_1),
    BIGINT_DECIMAL_4_3(NativeTypes.INT64, Types.DECIMAL_4_3),
    BIGINT_DECIMAL_2_0(NativeTypes.INT64, Types.DECIMAL_2_0),
    BIGINT_DECIMAL_3_1(NativeTypes.INT64, Types.DECIMAL_3_1),
    BIGINT_DECIMAL_5_3(NativeTypes.INT64, Types.DECIMAL_5_3),
    BIGINT_DECIMAL_5_0(NativeTypes.INT64, Types.DECIMAL_5_0),
    BIGINT_DECIMAL_6_1(NativeTypes.INT64, Types.DECIMAL_6_1),
    BIGINT_DECIMAL_8_3(NativeTypes.INT64, Types.DECIMAL_8_3),
    BIGINT_REAL(NativeTypes.INT64, NativeTypes.FLOAT),
    BIGINT_DOUBLE(NativeTypes.INT64, NativeTypes.DOUBLE),

    DECIMAL_1_0_DECIMAL_1_0(Types.DECIMAL_1_0, Types.DECIMAL_1_0),
    DECIMAL_1_0_DECIMAL_2_1(Types.DECIMAL_1_0, Types.DECIMAL_2_1),
    DECIMAL_1_0_DECIMAL_4_3(Types.DECIMAL_1_0, Types.DECIMAL_4_3),
    DECIMAL_1_0_DECIMAL_2_0(Types.DECIMAL_1_0, Types.DECIMAL_2_0),
    DECIMAL_1_0_DECIMAL_3_1(Types.DECIMAL_1_0, Types.DECIMAL_3_1),
    DECIMAL_1_0_DECIMAL_5_3(Types.DECIMAL_1_0, Types.DECIMAL_5_3),
    DECIMAL_1_0_DECIMAL_5_0(Types.DECIMAL_1_0, Types.DECIMAL_5_0),
    DECIMAL_1_0_DECIMAL_6_1(Types.DECIMAL_1_0, Types.DECIMAL_6_1),
    DECIMAL_1_0_DECIMAL_8_3(Types.DECIMAL_1_0, Types.DECIMAL_8_3),
    DECIMAL_1_0_REAL(Types.DECIMAL_1_0, NativeTypes.FLOAT),
    DECIMAL_1_0_DOUBLE(Types.DECIMAL_1_0, NativeTypes.DOUBLE),

    DECIMAL_2_1_DECIMAL_2_1(Types.DECIMAL_2_1, Types.DECIMAL_2_1),
    DECIMAL_2_1_DECIMAL_4_3(Types.DECIMAL_2_1, Types.DECIMAL_4_3),
    DECIMAL_2_1_DECIMAL_2_0(Types.DECIMAL_2_1, Types.DECIMAL_2_0),
    DECIMAL_2_1_DECIMAL_3_1(Types.DECIMAL_2_1, Types.DECIMAL_3_1),
    DECIMAL_2_1_DECIMAL_5_3(Types.DECIMAL_2_1, Types.DECIMAL_5_3),
    DECIMAL_2_1_DECIMAL_5_0(Types.DECIMAL_2_1, Types.DECIMAL_5_0),
    DECIMAL_2_1_DECIMAL_6_1(Types.DECIMAL_2_1, Types.DECIMAL_6_1),
    DECIMAL_2_1_DECIMAL_8_3(Types.DECIMAL_2_1, Types.DECIMAL_8_3),
    DECIMAL_2_1_REAL(Types.DECIMAL_2_1, NativeTypes.FLOAT),
    DECIMAL_2_1_DOUBLE(Types.DECIMAL_2_1, NativeTypes.DOUBLE),

    DECIMAL_4_3_DECIMAL_4_3(Types.DECIMAL_4_3, Types.DECIMAL_4_3),
    DECIMAL_4_3_DECIMAL_2_0(Types.DECIMAL_4_3, Types.DECIMAL_2_0),
    DECIMAL_4_3_DECIMAL_3_1(Types.DECIMAL_4_3, Types.DECIMAL_3_1),
    DECIMAL_4_3_DECIMAL_5_3(Types.DECIMAL_4_3, Types.DECIMAL_5_3),
    DECIMAL_4_3_DECIMAL_5_0(Types.DECIMAL_4_3, Types.DECIMAL_5_0),
    DECIMAL_4_3_DECIMAL_6_1(Types.DECIMAL_4_3, Types.DECIMAL_6_1),
    DECIMAL_4_3_DECIMAL_8_3(Types.DECIMAL_4_3, Types.DECIMAL_8_3),
    DECIMAL_4_3_REAL(Types.DECIMAL_4_3, NativeTypes.FLOAT),
    DECIMAL_4_3_DOUBLE(Types.DECIMAL_4_3, NativeTypes.DOUBLE),

    DECIMAL_2_0_DECIMAL_2_0(Types.DECIMAL_2_0, Types.DECIMAL_2_0),
    DECIMAL_2_0_DECIMAL_3_1(Types.DECIMAL_2_0, Types.DECIMAL_3_1),
    DECIMAL_2_0_DECIMAL_5_3(Types.DECIMAL_2_0, Types.DECIMAL_5_3),
    DECIMAL_2_0_DECIMAL_5_0(Types.DECIMAL_2_0, Types.DECIMAL_5_0),
    DECIMAL_2_0_DECIMAL_6_1(Types.DECIMAL_2_0, Types.DECIMAL_6_1),
    DECIMAL_2_0_DECIMAL_8_3(Types.DECIMAL_2_0, Types.DECIMAL_8_3),
    DECIMAL_2_0_REAL(Types.DECIMAL_2_0, NativeTypes.FLOAT),
    DECIMAL_2_0_DOUBLE(Types.DECIMAL_2_0, NativeTypes.DOUBLE),

    DECIMAL_3_1_DECIMAL_3_1(Types.DECIMAL_3_1, Types.DECIMAL_3_1),
    DECIMAL_3_1_DECIMAL_5_3(Types.DECIMAL_3_1, Types.DECIMAL_5_3),
    DECIMAL_3_1_DECIMAL_5_0(Types.DECIMAL_3_1, Types.DECIMAL_5_0),
    DECIMAL_3_1_DECIMAL_6_1(Types.DECIMAL_3_1, Types.DECIMAL_6_1),
    DECIMAL_3_1_DECIMAL_8_3(Types.DECIMAL_3_1, Types.DECIMAL_8_3),
    DECIMAL_3_1_REAL(Types.DECIMAL_3_1, NativeTypes.FLOAT),
    DECIMAL_3_1_DOUBLE(Types.DECIMAL_3_1, NativeTypes.DOUBLE),

    DECIMAL_5_3_DECIMAL_5_3(Types.DECIMAL_5_3, Types.DECIMAL_5_3),
    DECIMAL_5_3_DECIMAL_5_0(Types.DECIMAL_5_3, Types.DECIMAL_5_0),
    DECIMAL_5_3_DECIMAL_6_1(Types.DECIMAL_5_3, Types.DECIMAL_6_1),
    DECIMAL_5_3_DECIMAL_8_3(Types.DECIMAL_5_3, Types.DECIMAL_8_3),
    DECIMAL_5_3_REAL(Types.DECIMAL_5_3, NativeTypes.FLOAT),
    DECIMAL_5_3_DOUBLE(Types.DECIMAL_5_3, NativeTypes.DOUBLE),

    DECIMAL_5_0_DECIMAL_5_0(Types.DECIMAL_5_0, Types.DECIMAL_5_0),
    DECIMAL_5_0_DECIMAL_6_1(Types.DECIMAL_5_0, Types.DECIMAL_6_1),
    DECIMAL_5_0_DECIMAL_8_3(Types.DECIMAL_5_0, Types.DECIMAL_8_3),
    DECIMAL_5_0_REAL(Types.DECIMAL_5_0, NativeTypes.FLOAT),
    DECIMAL_5_0_DOUBLE(Types.DECIMAL_5_0, NativeTypes.DOUBLE),

    DECIMAL_6_1_DECIMAL_6_1(Types.DECIMAL_6_1, Types.DECIMAL_6_1),
    DECIMAL_6_1_DECIMAL_8_3(Types.DECIMAL_6_1, Types.DECIMAL_8_3),
    DECIMAL_6_1_REAL(Types.DECIMAL_6_1, NativeTypes.FLOAT),
    DECIMAL_6_1_DOUBLE(Types.DECIMAL_6_1, NativeTypes.DOUBLE),

    DECIMAL_8_3_DECIMAL_8_3(Types.DECIMAL_8_3, Types.DECIMAL_8_3),
    DECIMAL_8_3_REAL(Types.DECIMAL_8_3, NativeTypes.FLOAT),
    DECIMAL_8_3_DOUBLE(Types.DECIMAL_8_3, NativeTypes.DOUBLE),

    REAL_REAL(NativeTypes.FLOAT, NativeTypes.FLOAT),
    REAL_DOUBLE(NativeTypes.FLOAT, NativeTypes.DOUBLE),

    DOUBLE_DOUBLE(NativeTypes.DOUBLE, NativeTypes.DOUBLE),
    ;

    private final NativeType first;
    private final NativeType second;

    NumericPair(NativeType first, NativeType second) {
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
