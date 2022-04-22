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

package org.apache.ignite.internal.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.NativeTypeSpec;

/**
 * Colocation hash utilities.
 */
public class ColocationUtils {
    /**
     * Disallow to construct instance.
     */
    private ColocationUtils() {
        // No-op.
    }

    /**
     * Append value to the hash calculation.
     *
     * @param calc Hash calculator.
     * @param v Value to update hash.
     * @param typeSpec Value type.
     */
    public static void append(HashCalculator calc, Object v, NativeTypeSpec typeSpec) {
        if (v == null) {
            calc.appendNull();
            return;
        }

        switch (typeSpec) {
            case INT8:
                calc.appendByte((byte) v);
                return;

            case INT16:
                calc.appendShort((short) v);
                return;

            case INT32:
                calc.appendInt((int) v);
                return;

            case INT64:
                calc.appendLong((long) v);
                return;

            case FLOAT:
                calc.appendFloat((float) v);
                return;

            case DOUBLE:
                calc.appendDouble((double) v);
                return;

            case DECIMAL:
                calc.appendDecimal((BigDecimal) v);
                return;

            case UUID:
                calc.appendUuid((UUID) v);
                return;

            case STRING:
                calc.appendString((String) v);
                return;

            case BYTES:
                calc.appendBytes((byte[]) v);
                return;

            case BITMASK:
                calc.appendBitmask((BitSet) v);
                return;

            case NUMBER:
                calc.appendNumber((BigInteger) v);
                return;

            case DATE:
                calc.appendDate((LocalDate) v);
                return;

            case TIME:
                calc.appendTime((LocalTime) v);
                return;

            case DATETIME:
                calc.appendDateTime((LocalDateTime) v);
                return;

            case TIMESTAMP:
                calc.appendTimestamp((Instant) v);
                return;

            default:
                throw new IllegalStateException("Unexpected type: " + typeSpec);
        }
    }
}
