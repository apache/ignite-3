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

import java.math.BigInteger;

/**
 * Number class utility class.
 * */
public class NumericTypeUtils {
    /**
     * Calculates byte size by precision. May be too large but can never be too small. Typically exact.
     * */
    public static int byteSizeByPrecision(int precision) {
        long numBits = ((precision * 3402L) >>> 10) + 1;
        return (int)(numBits / 8 + 1);
    }

    /**
     * Calculates byte size for a BigInteger value.
     * */
    public static int sizeInBytes(BigInteger val) {
        return val.bitLength() / 8 + 1;
    }
}
