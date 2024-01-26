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
import java.util.Collection;
import java.util.function.Supplier;

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
     * Flushes the output. This flushes the interlying {@link OutputStream} (if exists).
     *
     * @throws IOException  if something went wrong
     */
    void flush() throws IOException;
}
