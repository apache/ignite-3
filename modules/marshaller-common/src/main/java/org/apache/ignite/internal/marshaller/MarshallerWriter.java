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
import java.util.UUID;

/**
 * Binary writer.
 */
public interface MarshallerWriter {
    /**
     * Writes null.
     */
    void writeNull();

    /**
     * Writes absent value indicator.
     *
     * <p>Absent value means that the user has not specified any value, or the column is not mapped. This is different from null.
     */
    void writeAbsentValue();

    /**
     * Writes boolean.
     *
     * @param val Value.
     */
    void writeBoolean(boolean val);

    /**
     * Writes byte.
     *
     * @param val Value.
     */
    void writeByte(byte val);

    /**
     * Writes short.
     *
     * @param val Value.
     */
    void writeShort(short val);

    /**
     * Writes int.
     *
     * @param val Value.
     */
    void writeInt(int val);

    /**
     * Writes long.
     *
     * @param val Value.
     */
    void writeLong(long val);

    /**
     * Writes float.
     *
     * @param val Value.
     */
    void writeFloat(float val);

    /**
     * Writes double.
     *
     * @param val Value.
     */
    void writeDouble(double val);

    /**
     * Writes string.
     *
     * @param val Value.
     */
    void writeString(String val);

    /**
     * Writes UUI.
     *
     * @param val Value.
     */
    void writeUuid(UUID val);

    /**
     * Writes bytes.
     *
     * @param val Value.
     */
    void writeBytes(byte[] val);

    /**
     * Writes big decimal.
     *
     * @param val Value.
     */
    void writeBigDecimal(BigDecimal val, int scale);

    /**
     * Writes date.
     *
     * @param val Value.
     */
    void writeDate(LocalDate val);

    /**
     * Writes time.
     *
     * @param val Value.
     */
    void writeTime(LocalTime val);

    /**
     * Writes timestamp.
     *
     * @param val Value.
     */
    void writeTimestamp(Instant val);

    /**
     * Writes date and time.
     *
     * @param val Value.
     */
    void writeDateTime(LocalDateTime val);
}
