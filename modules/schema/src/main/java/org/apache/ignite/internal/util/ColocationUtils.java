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

package org.apache.ignite.internal.util;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.jetbrains.annotations.Nullable;

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
     * @param type Value type.
     */
    public static void append(HashCalculator calc, @Nullable Object v, NativeType type) {
        if (v == null) {
            calc.appendNull();
            return;
        }

        switch (type.spec()) {
            case BOOLEAN:
                calc.appendBoolean((boolean) v);
                return;

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
                calc.appendDecimal((BigDecimal) v, ((DecimalNativeType) type).scale());
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

            case DATE:
                calc.appendDate((LocalDate) v);
                return;

            case TIME:
                calc.appendTime((LocalTime) v, ((TemporalNativeType) type).precision());
                return;

            case DATETIME:
                calc.appendDateTime((LocalDateTime) v, ((TemporalNativeType) type).precision());
                return;

            case TIMESTAMP:
                calc.appendTimestamp((Instant) v, ((TemporalNativeType) type).precision());
                return;

            default:
                throw new IllegalStateException("Unexpected type: " + type.spec());
        }
    }
}
