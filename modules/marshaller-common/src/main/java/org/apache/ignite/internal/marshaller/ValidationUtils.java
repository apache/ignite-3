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

package org.apache.ignite.internal.marshaller;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

/**
 * Marshaller utility class for validation methods.
 */
public class ValidationUtils {
    /**
     * Checks that the given column description is <i>compatible</i> with an actual object field type.
     *
     * @param col Column description.
     * @param cls Object field type.
     * @throws ClassCastException If the field type is not compatible with the column description.
     */
    public static void validateColumnType(MarshallerColumn col, Class<?> cls) {
        if (!isColumnCompatible(col.type(), cls)) {
            // Exception message is similar to embedded mode - see o.a.i.i.schema.Column#validate
            throw new ClassCastException("Column's type mismatch ["
                    + "column=" + col.name()
                    + ", expectedType=" + col.type()
                    + ", actualType=" + cls + ']');
        }
    }

    private static boolean isColumnCompatible(BinaryMode colType, Class<?> cls) {
        switch (colType) {
            case P_BOOLEAN:
            case BOOLEAN:
                return cls == boolean.class || cls == Boolean.class;
            case P_BYTE:
            case BYTE:
                return cls == byte.class || cls == Byte.class;
            case P_SHORT:
            case SHORT:
                return cls == short.class || cls == Short.class;
            case P_INT:
            case INT:
                return cls == int.class || cls == Integer.class;
            case P_LONG:
            case LONG:
                return cls == long.class || cls == Long.class;
            case P_FLOAT:
            case FLOAT:
                return cls == float.class || cls == Float.class;
            case P_DOUBLE:
            case DOUBLE:
                return cls == double.class || cls == Double.class;
            case STRING:
                return cls == String.class;
            case UUID:
                return cls == UUID.class;
            case BYTE_ARR:
                return cls == byte[].class;
            case BITSET:
                return cls == BitSet.class;
            case NUMBER:
                return cls == BigInteger.class;
            case DECIMAL:
                return cls == BigDecimal.class;
            case DATE:
                return cls == LocalDate.class;
            case TIME:
                return cls == LocalTime.class;
            case DATETIME:
                return cls == LocalDateTime.class;
            case TIMESTAMP:
                return cls == Instant.class;
            default:
                return false;
        }
    }

    private ValidationUtils() {}
}
