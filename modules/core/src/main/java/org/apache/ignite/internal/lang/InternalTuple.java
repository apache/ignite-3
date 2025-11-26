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

package org.apache.ignite.internal.lang;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;

/**
 * General interface to describe tuples outside of their data layout and column schemas.
 */
public interface InternalTuple {
    /**
     * Returns a number of values in the tuple.
     */
    int elementCount();

    /**
     * Checks whether the given column contains a null value.
     *
     * @param col Column index.
     * @return {@code true} if this column contains a null value, {@code false} otherwise.
     */
    boolean hasNullValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    boolean booleanValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    Boolean booleanValueBoxed(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    byte byteValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    Byte byteValueBoxed(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    short shortValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    Short shortValueBoxed(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    int intValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    Integer intValueBoxed(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    long longValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    Long longValueBoxed(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    float floatValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    Float floatValueBoxed(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    double doubleValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    Double doubleValueBoxed(int col);

    /**
     * Reads value from specified column.
     *
     * @param col Column index.
     * @param decimalScale Decimal scale. If equal to {@link Integer#MIN_VALUE}, then the value will be returned with whatever scale
     *         it is stored in.
     * @return Column value.
     */
    BigDecimal decimalValue(int col, int decimalScale);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    String stringValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    byte[] bytesValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    UUID uuidValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    LocalDate dateValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    LocalTime timeValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    LocalDateTime dateTimeValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    Instant timestampValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    Period periodValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    Duration durationValue(int col);

    /**
     * Returns the representation of this tuple as a Byte Buffer.
     */
    ByteBuffer byteBuffer();
}
