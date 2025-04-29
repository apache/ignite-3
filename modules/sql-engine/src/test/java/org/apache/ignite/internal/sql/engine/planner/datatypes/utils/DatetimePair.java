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

/**
 * Enumerates possible datetime type pairs for test purposes.
 */
public enum DatetimePair implements TypePair {
    DATE_DATE(Types.DATE, Types.DATE),

    TIME_0_TIME_0(Types.TIME_0, Types.TIME_0),
    TIME_0_TIME_1(Types.TIME_0, Types.TIME_3),
    TIME_0_TIME_9(Types.TIME_0, Types.TIME_9),

    TIME_1_TIME_1(Types.TIME_3, Types.TIME_3),
    TIME_1_TIME_0(Types.TIME_3, Types.TIME_0),
    TIME_1_TIME_9(Types.TIME_3, Types.TIME_9),

    TIME_9_TIME_9(Types.TIME_9, Types.TIME_9),
    TIME_9_TIME_0(Types.TIME_9, Types.TIME_0),
    TIME_9_TIME_1(Types.TIME_9, Types.TIME_3),

    TIMESTAMP_0_TIMESTAMP_0(Types.TIMESTAMP_0, Types.TIMESTAMP_0),
    TIMESTAMP_0_TIMESTAMP_1(Types.TIMESTAMP_0, Types.TIMESTAMP_3),
    TIMESTAMP_0_TIMESTAMP_9(Types.TIMESTAMP_0, Types.TIMESTAMP_9),
    TIMESTAMP_0_TIMESTAMP_WLTZ_0(Types.TIMESTAMP_0, Types.TIMESTAMP_WLTZ_0),
    TIMESTAMP_0_TIMESTAMP_WLTZ_1(Types.TIMESTAMP_0, Types.TIMESTAMP_WLTZ_3),
    TIMESTAMP_0_TIMESTAMP_WLTZ_9(Types.TIMESTAMP_0, Types.TIMESTAMP_WLTZ_9),

    TIMESTAMP_1_TIMESTAMP_1(Types.TIMESTAMP_3, Types.TIMESTAMP_3),
    TIMESTAMP_1_TIMESTAMP_0(Types.TIMESTAMP_3, Types.TIMESTAMP_0),
    TIMESTAMP_1_TIMESTAMP_9(Types.TIMESTAMP_3, Types.TIMESTAMP_9),
    TIMESTAMP_1_TIMESTAMP_WLTZ_0(Types.TIMESTAMP_3, Types.TIMESTAMP_WLTZ_0),
    TIMESTAMP_1_TIMESTAMP_WLTZ_1(Types.TIMESTAMP_3, Types.TIMESTAMP_WLTZ_3),
    TIMESTAMP_1_TIMESTAMP_WLTZ_9(Types.TIMESTAMP_3, Types.TIMESTAMP_WLTZ_9),

    TIMESTAMP_9_TIMESTAMP_9(Types.TIMESTAMP_9, Types.TIMESTAMP_9),
    TIMESTAMP_9_TIMESTAMP_0(Types.TIMESTAMP_9, Types.TIMESTAMP_0),
    TIMESTAMP_9_TIMESTAMP_1(Types.TIMESTAMP_9, Types.TIMESTAMP_3),
    TIMESTAMP_9_TIMESTAMP_WLTZ_0(Types.TIMESTAMP_9, Types.TIMESTAMP_WLTZ_0),
    TIMESTAMP_9_TIMESTAMP_WLTZ_1(Types.TIMESTAMP_9, Types.TIMESTAMP_WLTZ_3),
    TIMESTAMP_9_TIMESTAMP_WLTZ_9(Types.TIMESTAMP_9, Types.TIMESTAMP_WLTZ_9),


    TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0(Types.TIMESTAMP_WLTZ_0, Types.TIMESTAMP_WLTZ_0),
    TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1(Types.TIMESTAMP_WLTZ_0, Types.TIMESTAMP_WLTZ_3),
    TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9(Types.TIMESTAMP_WLTZ_0, Types.TIMESTAMP_WLTZ_9),
    TIMESTAMP_WLTZ_0_TIMESTAMP_0(Types.TIMESTAMP_WLTZ_0, Types.TIMESTAMP_0),
    TIMESTAMP_WLTZ_0_TIMESTAMP_1(Types.TIMESTAMP_WLTZ_0, Types.TIMESTAMP_3),
    TIMESTAMP_WLTZ_0_TIMESTAMP_9(Types.TIMESTAMP_WLTZ_0, Types.TIMESTAMP_9),

    TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1(Types.TIMESTAMP_WLTZ_3, Types.TIMESTAMP_WLTZ_3),
    TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0(Types.TIMESTAMP_WLTZ_3, Types.TIMESTAMP_WLTZ_0),
    TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9(Types.TIMESTAMP_WLTZ_3, Types.TIMESTAMP_WLTZ_9),
    TIMESTAMP_WLTZ_1_TIMESTAMP_0(Types.TIMESTAMP_WLTZ_3, Types.TIMESTAMP_0),
    TIMESTAMP_WLTZ_1_TIMESTAMP_1(Types.TIMESTAMP_WLTZ_3, Types.TIMESTAMP_3),
    TIMESTAMP_WLTZ_1_TIMESTAMP_9(Types.TIMESTAMP_WLTZ_3, Types.TIMESTAMP_9),

    TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9(Types.TIMESTAMP_WLTZ_9, Types.TIMESTAMP_WLTZ_9),
    TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0(Types.TIMESTAMP_WLTZ_9, Types.TIMESTAMP_WLTZ_0),
    TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1(Types.TIMESTAMP_WLTZ_9, Types.TIMESTAMP_WLTZ_3),
    TIMESTAMP_WLTZ_9_TIMESTAMP_0(Types.TIMESTAMP_WLTZ_9, Types.TIMESTAMP_0),
    TIMESTAMP_WLTZ_9_TIMESTAMP_1(Types.TIMESTAMP_WLTZ_9, Types.TIMESTAMP_3),
    TIMESTAMP_WLTZ_9_TIMESTAMP_9(Types.TIMESTAMP_WLTZ_9, Types.TIMESTAMP_9),

    ;

    private final NativeType first;

    private final NativeType second;

    DatetimePair(NativeType first, NativeType second) {
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
