/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.marshaller.schema;

/**
 * ExtendedByteBuffer implementation that does not store any data but counts the size of the data that passed through it.
 */
public class CalcSizeByteBuffer implements ExtendedByteBuffer {
    /** String array length. */
    private static final int STRING_HEADER = 4;

    /** Array length. */
    private static final int ARRAY_HEADER_LENGTH = 4;

    /** Byte. */
    private static final int BYTE = 1;

    /** Short. */
    private static final int SHORT = 2;

    /** Int. */
    private static final int INT = 4;

    /** Size. */
    private int size;

    /**
     * Default constructor.
     */
    protected CalcSizeByteBuffer() {
        size = 0;
    }

    /** {@inheritDoc} */
    @Override public void put(byte val) {
        size += BYTE;
    }

    /** {@inheritDoc} */
    @Override public void put(byte[] values) {
        size += ARRAY_HEADER_LENGTH;
        size += values.length;
    }

    /** {@inheritDoc} */
    @Override public void putShort(short val) {
        size += SHORT;
    }

    /** {@inheritDoc} */
    @Override public void putInt(int val) {
        size += INT;
    }

    /** {@inheritDoc} */
    @Override public void putString(String val) {
        size += STRING_HEADER + val.getBytes().length;
    }

    /** {@inheritDoc} */
    @Override public byte get() {
        throw new UnsupportedOperationException("Append only byte buffer.");
    }

    /** {@inheritDoc} */
    @Override public short getShort() {
        throw new UnsupportedOperationException("Append only byte buffer.");
    }

    /** {@inheritDoc} */
    @Override public int getInt() {
        throw new UnsupportedOperationException("Append only byte buffer.");
    }

    /** {@inheritDoc} */
    @Override public String getString() {
        throw new UnsupportedOperationException("Append only byte buffer.");
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        throw new UnsupportedOperationException("Append only byte buffer.");
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }
}
