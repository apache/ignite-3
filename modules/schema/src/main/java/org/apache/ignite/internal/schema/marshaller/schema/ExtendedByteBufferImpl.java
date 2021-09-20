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

import java.nio.ByteBuffer;

/**
 * ExtendedByteBuffer implementation which wraps a ByteBuffer object and adds several useful functions.
 *
 */
public class ExtendedByteBufferImpl implements ExtendedByteBuffer {
    /** Buffer. */
    private final ByteBuffer buffer;

    /**
     * @param buf Buffer.
     */
    protected ExtendedByteBufferImpl(ByteBuffer buf) {
        this.buffer = buf;
    }

    /** {@inheritDoc} */
    @Override public void put(byte val) {
        buffer.put(val);
    }

    /** {@inheritDoc} */
    @Override public void put(byte[] values) {
        buffer.put(values);
    }

    /** {@inheritDoc} */
    @Override public void putShort(short val) {
        buffer.putShort(val);
    }

    /** {@inheritDoc} */
    @Override public void putInt(int val) {
        buffer.putInt(val);
    }

    /** {@inheritDoc} */
    @Override public void putString(String val) {
        byte[] bytes = val.getBytes();

        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    /** {@inheritDoc} */
    @Override public byte get() {
        return buffer.get();
    }

    /** {@inheritDoc} */
    @Override public short getShort() {
        return buffer.getShort();
    }

    /** {@inheritDoc} */
    @Override public int getInt() {
        return buffer.getInt();
    }

    /** {@inheritDoc} */
    @Override public String getString() {
        int len = buffer.getInt();
        byte[] arr = new byte[len];

        for (int i = 0; i < len; i++)
            arr[i] = buffer.get();

        return new String(arr);
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return buffer.array();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        throw new UnsupportedOperationException();
    }
}
