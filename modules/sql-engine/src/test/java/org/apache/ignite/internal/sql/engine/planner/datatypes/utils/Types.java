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
 * Utility class that defines type constant to use as {@link TypePair type pair}, for instance.
 *
 * <p>These constants is just shortcuts for variable length types to use in tests.
 */
public final class Types {
    public static final NativeType NUMBER_1 = NativeTypes.numberOf(1);
    public static final NativeType NUMBER_2 = NativeTypes.numberOf(2);
    public static final NativeType NUMBER_3 = NativeTypes.numberOf(3);
    public static final NativeType NUMBER_5 = NativeTypes.numberOf(5);
    public static final NativeType NUMBER_10 = NativeTypes.numberOf(10);
    public static final NativeType NUMBER_19 = NativeTypes.numberOf(19);

    public static final NativeType DECIMAL_1_0 = NativeTypes.decimalOf(1, 0);
    public static final NativeType DECIMAL_2_1 = NativeTypes.decimalOf(2, 1);
    public static final NativeType DECIMAL_4_1 = NativeTypes.decimalOf(4, 1);
    public static final NativeType DECIMAL_4_3 = NativeTypes.decimalOf(4, 3);
    public static final NativeType DECIMAL_10_0 = NativeTypes.decimalOf(10, 0);
    public static final NativeType DECIMAL_11_1 = NativeTypes.decimalOf(11, 1);
    public static final NativeType DECIMAL_13_3 = NativeTypes.decimalOf(13, 3);
    public static final NativeType DECIMAL_19_0 = NativeTypes.decimalOf(19, 0);
    public static final NativeType DECIMAL_20_1 = NativeTypes.decimalOf(20, 1);
    public static final NativeType DECIMAL_22_3 = NativeTypes.decimalOf(22, 3);

    public static final NativeType DECIMAL_2_0 = NativeTypes.decimalOf(2, 0);
    public static final NativeType DECIMAL_3_1 = NativeTypes.decimalOf(3, 1);
    public static final NativeType DECIMAL_5_3 = NativeTypes.decimalOf(5, 3);
    public static final NativeType DECIMAL_6_3 = NativeTypes.decimalOf(6, 3);

    public static final NativeType DECIMAL_3_0 = NativeTypes.decimalOf(3, 0);
    public static final NativeType DECIMAL_5_0 = NativeTypes.decimalOf(5, 0);
    public static final NativeType DECIMAL_6_1 = NativeTypes.decimalOf(6, 1);
    public static final NativeType DECIMAL_8_3 = NativeTypes.decimalOf(8, 3);

    public static final NativeType DECIMAL_14_7 = NativeTypes.decimalOf(14, 7);
    public static final NativeType DECIMAL_30_15 = NativeTypes.decimalOf(30, 15);

    private Types() {
        throw new AssertionError("Should not be called");
    }
}
