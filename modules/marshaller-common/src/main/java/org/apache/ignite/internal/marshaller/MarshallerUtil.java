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
 * Marshaller utility class.
 */
public class MarshallerUtil {
    /**
     * Gets binary read/write mode for given class.
     *
     * @param cls Type.
     * @return Binary mode.
     */
    public static BinaryMode mode(Class<?> cls) {
        assert cls != null;

        // Primitives.
        if (cls == boolean.class) {
            return BinaryMode.P_BOOLEAN;
        } else if (cls == byte.class) {
            return BinaryMode.P_BYTE;
        } else if (cls == short.class) {
            return BinaryMode.P_SHORT;
        } else if (cls == int.class) {
            return BinaryMode.P_INT;
        } else if (cls == long.class) {
            return BinaryMode.P_LONG;
        } else if (cls == float.class) {
            return BinaryMode.P_FLOAT;
        } else if (cls == double.class) {
            return BinaryMode.P_DOUBLE;
        } else if (cls == Boolean.class) { // Boxed primitives.
            return BinaryMode.BOOLEAN;
        } else if (cls == Byte.class) { // Boxed primitives.
            return BinaryMode.BYTE;
        } else if (cls == Short.class) {
            return BinaryMode.SHORT;
        } else if (cls == Integer.class) {
            return BinaryMode.INT;
        } else if (cls == Long.class) {
            return BinaryMode.LONG;
        } else if (cls == Float.class) {
            return BinaryMode.FLOAT;
        } else if (cls == Double.class) {
            return BinaryMode.DOUBLE;
        } else if (cls == LocalDate.class) { // Temporal types
            return BinaryMode.DATE;
        } else if (cls == LocalTime.class) {
            return BinaryMode.TIME;
        } else if (cls == LocalDateTime.class) {
            return BinaryMode.DATETIME;
        } else if (cls == Instant.class) {
            return BinaryMode.TIMESTAMP;
        } else if (cls == byte[].class) { // Other types
            return BinaryMode.BYTE_ARR;
        } else if (cls == String.class) {
            return BinaryMode.STRING;
        } else if (cls == UUID.class) {
            return BinaryMode.UUID;
        } else if (cls == BitSet.class) {
            return BinaryMode.BITSET;
        } else if (cls == BigInteger.class) {
            return BinaryMode.NUMBER;
        } else if (cls == BigDecimal.class) {
            return BinaryMode.DECIMAL;
        }

        return null;
    }

    static void validateColumnType(MarshallerColumn col, Class<?> cls) {
        if (!isColumnCompatible(col.type(), cls)) {
            throw new ClassCastException("Incorrect value type for column '" + col.name() + "': class " + cls +
                    " cannot be cast to column type " + col.type());
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
        }

        return false;
    }

    /**
     * Stub.
     */
    private MarshallerUtil() {
    }
}
