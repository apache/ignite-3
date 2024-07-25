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

package org.apache.ignite.internal.schema.configuration;

import static java.math.RoundingMode.HALF_UP;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.StringUtils;

/**
 * Utility class to convert object to and from human readable string representation.
 */
public final class ValueSerializationHelper {
    /**
     * Converts value to a string representation.
     *
     * @param defaultValue Value to convert.
     * @param type Type of the value.
     * @return String representation of given value.
     * @throws NullPointerException If given value or type is null.
     */
    public static String toString(Object defaultValue, NativeType type) {
        Objects.requireNonNull(defaultValue, "defaultValue");
        Objects.requireNonNull(type, "type");

        switch (type.spec()) {
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case STRING:
            case UUID:
                return defaultValue.toString();
            case BYTES:
                return StringUtils.toHexString((byte[]) defaultValue);
            default:
                throw new IllegalStateException("Unknown type [type=" + type + ']');
        }
    }

    /**
     * Converts value from a string representation.
     *
     * @param defaultValue String representation of a value.
     * @param type Type of the value.
     * @return Restored value.
     * @throws NullPointerException If given value or type is null.
     */
    public static Object fromString(String defaultValue, NativeType type) {
        Objects.requireNonNull(defaultValue, "defaultValue");
        Objects.requireNonNull(type, "type");

        switch (type.spec()) {
            case BOOLEAN:
                return Boolean.parseBoolean(defaultValue);
            case INT8:
                return Byte.parseByte(defaultValue);
            case INT16:
                return Short.parseShort(defaultValue);
            case INT32:
                return Integer.parseInt(defaultValue);
            case INT64:
                return Long.parseLong(defaultValue);
            case FLOAT:
                return Float.parseFloat(defaultValue);
            case DOUBLE:
                return Double.parseDouble(defaultValue);
            case DECIMAL:
                assert type instanceof DecimalNativeType;

                return new BigDecimal(defaultValue).setScale(((DecimalNativeType) type).scale(), HALF_UP);
            case DATE:
                return LocalDate.parse(defaultValue);
            case TIME:
                return LocalTime.parse(defaultValue);
            case DATETIME:
                return LocalDateTime.parse(defaultValue);
            case TIMESTAMP:
                return Instant.parse(defaultValue);
            case STRING:
                return defaultValue;
            case UUID:
                return UUID.fromString(defaultValue);
            case BYTES:
                return StringUtils.fromHexString(defaultValue);
            default:
                throw new IllegalStateException("Unknown type [type=" + type + ']');
        }
    }

    private ValueSerializationHelper() { }
}
