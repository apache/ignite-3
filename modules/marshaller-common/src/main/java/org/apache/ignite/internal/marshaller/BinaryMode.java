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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Various read/write modes for binary objects that maps Java types to binary types.
 */
public enum BinaryMode {
    /** Primitive boolean. */
    P_BOOLEAN,

    /** Primitive byte. */
    P_BYTE,

    /** Primitive short. */
    P_SHORT,

    /** Primitive int. */
    P_INT,

    /** Primitive long. */
    P_LONG,

    /** Primitive float. */
    P_FLOAT,

    /** Primitive int. */
    P_DOUBLE,

    /** Boxed boolean. */
    BOOLEAN,

    /** Boxed byte. */
    BYTE,

    /** Boxed short. */
    SHORT,

    /** Boxed int. */
    INT,

    /** Boxed long. */
    LONG,

    /** Boxed float. */
    FLOAT,

    /** Boxed double. */
    DOUBLE,

    /** String. */
    STRING,

    /** Uuid. */
    UUID,

    /** Raw byte array. */
    BYTE_ARR,

    /** BigDecimal. */
    DECIMAL,

    /** Date. */
    DATE,

    /** Time. */
    TIME,

    /** Datetime. */
    DATETIME,

    /** Timestamp. */
    TIMESTAMP,

    /** User object. */
    POJO;

    /**
     * Gets binary read/write mode for given class.
     *
     * @param cls Type.
     * @return Binary mode.
     */
    public static BinaryMode forClass(Class<?> cls) {
        // Primitives.
        if (cls == boolean.class) {
            return P_BOOLEAN;
        } else if (cls == byte.class) {
            return P_BYTE;
        } else if (cls == short.class) {
            return P_SHORT;
        } else if (cls == int.class) {
            return P_INT;
        } else if (cls == long.class) {
            return P_LONG;
        } else if (cls == float.class) {
            return P_FLOAT;
        } else if (cls == double.class) {
            return P_DOUBLE;
        } else if (cls == Boolean.class) { // Boxed primitives.
            return BOOLEAN;
        } else if (cls == Byte.class) {
            return BYTE;
        } else if (cls == Short.class) {
            return SHORT;
        } else if (cls == Integer.class) {
            return INT;
        } else if (cls == Long.class) {
            return LONG;
        } else if (cls == Float.class) {
            return FLOAT;
        } else if (cls == Double.class) {
            return DOUBLE;
        } else if (cls == LocalDate.class) { // Temporal types
            return DATE;
        } else if (cls == LocalTime.class) {
            return TIME;
        } else if (cls == LocalDateTime.class) {
            return DATETIME;
        } else if (cls == Instant.class) {
            return TIMESTAMP;
        } else if (cls == byte[].class) { // Other types
            return BYTE_ARR;
        } else if (cls == String.class) {
            return STRING;
        } else if (cls == java.util.UUID.class) {
            return UUID;
        } else if (cls == BigDecimal.class) {
            return DECIMAL;
        }

        return POJO;
    }
}
