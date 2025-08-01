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

import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;

/**
 * Utility class that defines type constant to use as {@link TypePair type pair}, for instance.
 *
 * <p>These constants is just shortcuts for variable length types to use in tests.
 */
@SuppressWarnings("WeakerAccess")
public final class Types {

    public static final NativeType DECIMAL_1_0 = NativeTypes.decimalOf(1, 0);

    public static final NativeType DECIMAL_2_0 = NativeTypes.decimalOf(2, 0);
    public static final NativeType DECIMAL_2_1 = NativeTypes.decimalOf(2, 1);

    public static final NativeType DECIMAL_3_0 = NativeTypes.decimalOf(3, 0);
    public static final NativeType DECIMAL_3_1 = NativeTypes.decimalOf(3, 1);

    public static final NativeType DECIMAL_4_0 = NativeTypes.decimalOf(4, 0);
    public static final NativeType DECIMAL_4_1 = NativeTypes.decimalOf(4, 1);
    public static final NativeType DECIMAL_4_2 = NativeTypes.decimalOf(4, 2);
    public static final NativeType DECIMAL_4_3 = NativeTypes.decimalOf(4, 3);

    public static final NativeType DECIMAL_5_0 = NativeTypes.decimalOf(5, 0);
    public static final NativeType DECIMAL_5_1 = NativeTypes.decimalOf(5, 1);
    public static final NativeType DECIMAL_5_2 = NativeTypes.decimalOf(5, 2);
    public static final NativeType DECIMAL_5_3 = NativeTypes.decimalOf(5, 3);

    public static final NativeType DECIMAL_6_0 = NativeTypes.decimalOf(6, 0);
    public static final NativeType DECIMAL_6_1 = NativeTypes.decimalOf(6, 1);
    public static final NativeType DECIMAL_6_2 = NativeTypes.decimalOf(6, 2);
    public static final NativeType DECIMAL_6_3 = NativeTypes.decimalOf(6, 3);
    public static final NativeType DECIMAL_6_4 = NativeTypes.decimalOf(6, 4);

    public static final NativeType DECIMAL_7_0 = NativeTypes.decimalOf(7, 0);
    public static final NativeType DECIMAL_7_1 = NativeTypes.decimalOf(7, 1);
    public static final NativeType DECIMAL_7_3 = NativeTypes.decimalOf(7, 3);
    public static final NativeType DECIMAL_7_4 = NativeTypes.decimalOf(7, 4);
    public static final NativeType DECIMAL_7_6 = NativeTypes.decimalOf(7, 6);

    public static final NativeType DECIMAL_8_0 = NativeTypes.decimalOf(8, 0);
    public static final NativeType DECIMAL_8_1 = NativeTypes.decimalOf(8, 1);
    public static final NativeType DECIMAL_8_2 = NativeTypes.decimalOf(8, 2);
    public static final NativeType DECIMAL_8_3 = NativeTypes.decimalOf(8, 3);
    public static final NativeType DECIMAL_8_4 = NativeTypes.decimalOf(8, 4);
    public static final NativeType DECIMAL_8_6 = NativeTypes.decimalOf(8, 6);
    public static final NativeType DECIMAL_8_7 = NativeTypes.decimalOf(8, 7);

    public static final NativeType DECIMAL_9_1 = NativeTypes.decimalOf(9, 1);
    public static final NativeType DECIMAL_9_2 = NativeTypes.decimalOf(9, 2);
    public static final NativeType DECIMAL_9_3 = NativeTypes.decimalOf(9, 3);
    public static final NativeType DECIMAL_9_6 = NativeTypes.decimalOf(9, 6);
    public static final NativeType DECIMAL_9_7 = NativeTypes.decimalOf(9, 7);

    public static final NativeType DECIMAL_10_0 = NativeTypes.decimalOf(10, 0);
    public static final NativeType DECIMAL_10_1 = NativeTypes.decimalOf(10, 1);
    public static final NativeType DECIMAL_10_3 = NativeTypes.decimalOf(10, 3);
    public static final NativeType DECIMAL_10_4 = NativeTypes.decimalOf(10, 4);
    public static final NativeType DECIMAL_10_6 = NativeTypes.decimalOf(10, 6);
    public static final NativeType DECIMAL_10_7 = NativeTypes.decimalOf(10, 7);
    public static final NativeType DECIMAL_10_8 = NativeTypes.decimalOf(10, 8);
    public static final NativeType DECIMAL_10_9 = NativeTypes.decimalOf(10, 9);

    public static final NativeType DECIMAL_11_0 = NativeTypes.decimalOf(11, 0);
    public static final NativeType DECIMAL_11_1 = NativeTypes.decimalOf(11, 1);
    public static final NativeType DECIMAL_11_3 = NativeTypes.decimalOf(11, 3);
    public static final NativeType DECIMAL_11_4 = NativeTypes.decimalOf(11, 4);
    public static final NativeType DECIMAL_11_6 = NativeTypes.decimalOf(11, 6);
    public static final NativeType DECIMAL_11_7 = NativeTypes.decimalOf(11, 7);
    public static final NativeType DECIMAL_11_8 = NativeTypes.decimalOf(11, 8);
    public static final NativeType DECIMAL_11_9 = NativeTypes.decimalOf(11, 9);

    public static final NativeType DECIMAL_12_0 = NativeTypes.decimalOf(12, 0);
    public static final NativeType DECIMAL_12_1 = NativeTypes.decimalOf(12, 1);
    public static final NativeType DECIMAL_12_2 = NativeTypes.decimalOf(12, 2);
    public static final NativeType DECIMAL_12_6 = NativeTypes.decimalOf(12, 6);
    public static final NativeType DECIMAL_12_7 = NativeTypes.decimalOf(12, 7);
    public static final NativeType DECIMAL_12_8 = NativeTypes.decimalOf(12, 8);
    public static final NativeType DECIMAL_12_10 = NativeTypes.decimalOf(12, 10);

    public static final NativeType DECIMAL_13_1 = NativeTypes.decimalOf(13, 1);
    public static final NativeType DECIMAL_13_3 = NativeTypes.decimalOf(13, 3);
    public static final NativeType DECIMAL_13_6 = NativeTypes.decimalOf(13, 6);
    public static final NativeType DECIMAL_13_7 = NativeTypes.decimalOf(13, 7);
    public static final NativeType DECIMAL_13_9 = NativeTypes.decimalOf(13, 9);
    public static final NativeType DECIMAL_13_10 = NativeTypes.decimalOf(13, 10);

    public static final NativeType DECIMAL_14_3 = NativeTypes.decimalOf(14, 3);
    public static final NativeType DECIMAL_14_4 = NativeTypes.decimalOf(14, 4);
    public static final NativeType DECIMAL_14_6 = NativeTypes.decimalOf(14, 6);
    public static final NativeType DECIMAL_14_7 = NativeTypes.decimalOf(14, 7);
    public static final NativeType DECIMAL_14_8 = NativeTypes.decimalOf(14, 8);
    public static final NativeType DECIMAL_14_9 = NativeTypes.decimalOf(14, 9);
    public static final NativeType DECIMAL_14_10 = NativeTypes.decimalOf(14, 10);

    public static final NativeType DECIMAL_15_0 = NativeTypes.decimalOf(15, 0);
    public static final NativeType DECIMAL_15_3 = NativeTypes.decimalOf(15, 3);
    public static final NativeType DECIMAL_15_9 = NativeTypes.decimalOf(15, 9);
    public static final NativeType DECIMAL_15_10 = NativeTypes.decimalOf(15, 10);

    public static final NativeType DECIMAL_16_1 = NativeTypes.decimalOf(16, 1);
    public static final NativeType DECIMAL_16_6 = NativeTypes.decimalOf(16, 6);
    public static final NativeType DECIMAL_16_12 = NativeTypes.decimalOf(16, 12);
    public static final NativeType DECIMAL_16_15 = NativeTypes.decimalOf(16, 15);

    public static final NativeType DECIMAL_17_6 = NativeTypes.decimalOf(17, 6);
    public static final NativeType DECIMAL_17_9 = NativeTypes.decimalOf(17, 9);
    public static final NativeType DECIMAL_17_12 = NativeTypes.decimalOf(17, 12);
    public static final NativeType DECIMAL_17_15 = NativeTypes.decimalOf(17, 15);

    public static final NativeType DECIMAL_18_3 = NativeTypes.decimalOf(18, 3);
    public static final NativeType DECIMAL_18_7 = NativeTypes.decimalOf(18, 7);
    public static final NativeType DECIMAL_18_10 = NativeTypes.decimalOf(18, 10);
    public static final NativeType DECIMAL_18_15 = NativeTypes.decimalOf(18, 15);
    public static final NativeType DECIMAL_18_16 = NativeTypes.decimalOf(18, 16);

    public static final NativeType DECIMAL_19_0 = NativeTypes.decimalOf(19, 0);
    public static final NativeType DECIMAL_19_1 = NativeTypes.decimalOf(19, 1);
    public static final NativeType DECIMAL_19_3 = NativeTypes.decimalOf(19, 3);
    public static final NativeType DECIMAL_19_6 = NativeTypes.decimalOf(19, 6);
    public static final NativeType DECIMAL_19_16 = NativeTypes.decimalOf(19, 16);

    public static final NativeType DECIMAL_20_0 = NativeTypes.decimalOf(20, 0);
    public static final NativeType DECIMAL_20_1 = NativeTypes.decimalOf(20, 1);
    public static final NativeType DECIMAL_20_12 = NativeTypes.decimalOf(20, 12);
    public static final NativeType DECIMAL_20_15 = NativeTypes.decimalOf(20, 15);
    public static final NativeType DECIMAL_20_16 = NativeTypes.decimalOf(20, 16);
    public static final NativeType DECIMAL_20_18 = NativeTypes.decimalOf(20, 18);

    public static final NativeType DECIMAL_21_0 = NativeTypes.decimalOf(21, 0);
    public static final NativeType DECIMAL_21_1 = NativeTypes.decimalOf(21, 1);
    public static final NativeType DECIMAL_21_16 = NativeTypes.decimalOf(21, 16);

    public static final NativeType DECIMAL_22_1 = NativeTypes.decimalOf(22, 1);
    public static final NativeType DECIMAL_22_3 = NativeTypes.decimalOf(22, 3);
    public static final NativeType DECIMAL_22_9 = NativeTypes.decimalOf(22, 9);
    public static final NativeType DECIMAL_22_15 = NativeTypes.decimalOf(22, 15);

    public static final NativeType DECIMAL_23_3 = NativeTypes.decimalOf(23, 3);

    public static final NativeType DECIMAL_24_0 = NativeTypes.decimalOf(24, 0);
    public static final NativeType DECIMAL_24_3 = NativeTypes.decimalOf(24, 3);

    public static final NativeType DECIMAL_25_1 = NativeTypes.decimalOf(25, 1);
    public static final NativeType DECIMAL_25_6 = NativeTypes.decimalOf(25, 6);
    public static final NativeType DECIMAL_25_15 = NativeTypes.decimalOf(25, 15);

    public static final NativeType DECIMAL_26_6 = NativeTypes.decimalOf(26, 6);
    public static final NativeType DECIMAL_26_16 = NativeTypes.decimalOf(26, 16);

    public static final NativeType DECIMAL_27_3 = NativeTypes.decimalOf(27, 3);
    public static final NativeType DECIMAL_27_7 = NativeTypes.decimalOf(27, 7);

    public static final NativeType DECIMAL_28_6 = NativeTypes.decimalOf(28, 6);

    public static final NativeType DECIMAL_30_15 = NativeTypes.decimalOf(30, 15);

    public static final NativeType DECIMAL_31_9 = NativeTypes.decimalOf(31, 9);

    public static final NativeType DECIMAL_35_16 = NativeTypes.decimalOf(35, 16);

    public static final NativeType DECIMAL_MAX_18 = NativeTypes.decimalOf(CatalogUtils.MAX_DECIMAL_PRECISION, 18);

    public static final NativeType DECIMAL_MAX_2 = NativeTypes.decimalOf(CatalogUtils.MAX_DECIMAL_PRECISION, 2);

    public static final NativeType DECIMAL_MAX_0 = NativeTypes.decimalOf(CatalogUtils.MAX_DECIMAL_PRECISION, 0);

    public static final NativeType TIME_0 = NativeTypes.time(0);
    public static final NativeType TIME_3 = NativeTypes.time(3);
    public static final NativeType TIME_6 = NativeTypes.time(6);
    public static final NativeType TIME_9 = NativeTypes.time(9);
    public static final NativeType TIME_DYN_PARAM = NativeTypes.time(IgniteSqlValidator.TEMPORAL_DYNAMIC_PARAM_PRECISION);

    public static final NativeType DATE = NativeTypes.DATE;

    public static final NativeType TIMESTAMP_0 = NativeTypes.datetime(0);
    public static final NativeType TIMESTAMP_3 = NativeTypes.datetime(3);
    public static final NativeType TIMESTAMP_6 = NativeTypes.datetime(6);
    public static final NativeType TIMESTAMP_9 = NativeTypes.datetime(9);
    public static final NativeType TIMESTAMP_DYN_PARAM = NativeTypes.datetime(IgniteSqlValidator.TEMPORAL_DYNAMIC_PARAM_PRECISION);

    public static final NativeType TIMESTAMP_WLTZ_0 = NativeTypes.timestamp(0);
    public static final NativeType TIMESTAMP_WLTZ_3 = NativeTypes.timestamp(3);
    public static final NativeType TIMESTAMP_WLTZ_6 = NativeTypes.timestamp(6);
    public static final NativeType TIMESTAMP_WLTZ_9 = NativeTypes.timestamp(9);
    public static final NativeType TIMESTAMP_WLTZ_DYN_PARAM = NativeTypes.timestamp(IgniteSqlValidator.TEMPORAL_DYNAMIC_PARAM_PRECISION);

    public static final NativeType VARCHAR_1 = NativeTypes.stringOf(1);
    public static final NativeType VARCHAR_3 = NativeTypes.stringOf(3);
    public static final NativeType VARCHAR_5 = NativeTypes.stringOf(5);
    public static final NativeType VARCHAR_128 = NativeTypes.stringOf(128);
    public static final NativeType VARCHAR_DEFAULT = NativeTypes.stringOf(-1);

    public static final NativeType VARBINARY_1 = NativeTypes.blobOf(1);
    public static final NativeType VARBINARY_2 = NativeTypes.blobOf(2);

    public static final NativeType VARBINARY_128 = NativeTypes.blobOf(128);

    private Types() {
        throw new AssertionError("Should not be called");
    }
}
