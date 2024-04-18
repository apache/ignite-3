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

package org.apache.ignite.internal.util.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.UUID;

/**
 * Extended data output.
 */
public interface IgniteDataOutput extends DataOutput {
    /**
     * Sets the {@link OutputStream} to push data to.
     *
     * @param out Underlying stream.
     */
    void outputStream(OutputStream out);

    /**
     * Returns a copy of the internal array.
     *
     * @return Copy of internal array shrunk to offset.
     */
    byte[] array();

    /**
     * Returns the internal array.
     *
     * @return Internal array.
     */
    byte[] internalArray();

    /**
     * Returns the current offset in the internal array.
     *
     * @return Offset.
     */
    int offset();

    /**
     * Adjusts the offset to the internal array.
     *
     * @param off Offset.
     */
    void offset(int off);

    /**
     * Makes the data output ready for reuse.
     */
    void cleanup();

    /**
     * Writes array of {@code byte}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeByteArray(byte[] arr) throws IOException;

    /**
     * Writes array of {@code short}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeShortArray(short[] arr) throws IOException;

    /**
     * Writes array of {@code int}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeIntArray(int[] arr) throws IOException;

    /**
     * Writes array of {@code long}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeLongArray(long[] arr) throws IOException;

    /**
     * Writes array of {@code float}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeFloatArray(float[] arr) throws IOException;

    /**
     * Writes array of {@code double}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeDoubleArray(double[] arr) throws IOException;

    /**
     * Writes array of {@code boolean}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeBooleanArray(boolean[] arr) throws IOException;

    /**
     * Writes array of {@code char}s.
     *
     * @param arr Array.
     * @throws IOException In case of error.
     */
    void writeCharArray(char[] arr) throws IOException;

    /**
     * Writes big integer.
     *
     * @param val Big integer.
     * @throws IOException In case of error.
     */
    void writeBigInteger(BigInteger val) throws IOException;

    /**
     * Writes decimal.
     *
     * @param val Decimal.
     * @throws IOException In case of error.
     */
    void writeBigDecimal(BigDecimal val) throws IOException;

    /**
     * Writes local time.
     *
     * @param val Local time.
     * @throws IOException In case of error.
     */
    void writeLocalTime(LocalTime val) throws IOException;

    /**
     * Writes local date.
     *
     * @param date Local date.
     * @throws IOException In case of error.
     */
    void writeLocalDate(LocalDate date) throws IOException;

    /**
     * Writes local date time.
     *
     * @param val Local date time.
     * @throws IOException In case of error.
     */
    void writeLocalDateTime(LocalDateTime val) throws IOException;

    /**
     * Writes instant.
     *
     * @param val Instant.
     * @throws IOException In case of error.
     */
    void writeInstant(Instant val) throws IOException;

    /**
     * Writes period.
     *
     * @param val Period.
     * @throws IOException In case of error.
     */
    void writePeriod(Period val) throws IOException;

    /**
     * Writes duration.
     *
     * @param val Duration.
     * @throws IOException In case of error.
     */
    void writeDuration(Duration val) throws IOException;

    /**
     * Writes uuid.
     *
     * @param val UUID.
     * @throws IOException In case of error.
     */
    void writeUuid(UUID val) throws IOException;

    /**
     * Writes bit set.
     *
     * @param val Bit set.
     * @throws IOException In case of error.
     */
    void writeBitSet(BitSet val) throws IOException;

    /**
     * Flushes the output. This flushes the interlying {@link OutputStream} (if exists).
     *
     * @throws IOException  if something went wrong
     */
    void flush() throws IOException;
}
