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

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.ignite.schema.ColumnType;

/**
 * Numeric types utility class.
 * */
public class NumericTypeUtils {
    /**
     * check precision of BigInteger value.
     */
    public static boolean precisionDoesNotFit(BigInteger val, int precision) {
        if (precision == ColumnType.NumberColumnType.UNLIMITED_PRECISION)
            return false;
        return calculatePrecision(val) > precision;
    }

    /**
     * Calculates precision for BigInteger value.
     */
    public static int calculatePrecision(BigInteger val) {
        return new BigDecimal(val).precision();
    }
}
