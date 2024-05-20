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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

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

    /**
     * Checks whether {@code null} is allowed for the given value type in a {@code KeyValueView} operation.
     * If the value type is not natively supported, throws an exception.
     *
     * @param val Value to check.
     * @param valueType Value type.
     * @throws NullPointerException If value is null and valueType is not natively supported.
     */
    public static void validateNullableValue(@Nullable Object val, Class<?> valueType) {
        if (val == null && !Mapper.nativelySupported(valueType)) {
            String message = "null value cannot be used when a value is not mapped to a simple type";

            throw new NullPointerException(message);
        }
    }

    /**
     * Checks whether {@code getNullable*} operation on a {@code KeyValueView} is allowed for the given value type.
     * If the value type is not natively supported, throws an exception.
     *
     * @param valueType Value type.
     */
    public static void validateNullableOperation(Class<?> valueType, String methodName) {
        if (!Mapper.nativelySupported(valueType)) {
            String message = format("{} cannot be used when a value is not mapped to a simple type", methodName);

            throw new UnsupportedOperationException(message);
        }
    }

    private ValidationUtils() {}
}
