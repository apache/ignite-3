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
package org.apache.ignite.raft.jraft.util;

import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Bits util.
 */
public class Bits {
    public static int getInt(final byte[] b, final int off) {
        return HeapByteBufUtil.getInt(b, off);
    }

    public static long getLong(final byte[] b, final int off) {
        return HeapByteBufUtil.getLong(b, off);
    }

    public static void putInt(final byte[] b, final int off, final int val) {
        HeapByteBufUtil.setInt(b, off, val);
    }

    public static void putShort(final byte[] b, final int off, final short val) {
        HeapByteBufUtil.setShort(b, off, val);
    }

    public static short getShort(final byte[] b, final int off) {
        return HeapByteBufUtil.getShort(b, off);
    }

    public static void putLong(final byte[] b, final int off, final long val) {
        HeapByteBufUtil.setLong(b, off, val);
    }

    public static void putShortLittleEndian(long addr, short value) {
        if (GridUnsafe.IS_BIG_ENDIAN) {
            GridUnsafe.putShort(addr, Short.reverseBytes(value));
        } else {
            GridUnsafe.putShort(addr, value);
        }
    }

    public static void putShortLittleEndian(byte[] b, int off, short value) {
        if (GridUnsafe.IS_BIG_ENDIAN) {
            GridUnsafe.putShort(b, GridUnsafe.BYTE_ARR_OFF + off, Short.reverseBytes(value));
        } else {
            GridUnsafe.putShort(b, GridUnsafe.BYTE_ARR_OFF + off, value);
        }
    }

    public static short getShortLittleEndian(byte[] b, int off) {
        short value = GridUnsafe.getShort(b, GridUnsafe.BYTE_ARR_OFF + off);

        return GridUnsafe.IS_BIG_ENDIAN ? Short.reverseBytes(value) : value;
    }

    public static void putIntLittleEndian(long addr, int value) {
        if (GridUnsafe.IS_BIG_ENDIAN) {
            GridUnsafe.putInt(addr, Integer.reverseBytes(value));
        } else {
            GridUnsafe.putInt(addr, value);
        }
    }

    public static void putLongLittleEndian(long addr, long value) {
        if (GridUnsafe.IS_BIG_ENDIAN) {
            GridUnsafe.putLong(addr, Long.reverseBytes(value));
        } else {
            GridUnsafe.putLong(addr, value);
        }
    }

    public static void putLongLittleEndian(byte[] b, int off, long value) {
        if (GridUnsafe.IS_BIG_ENDIAN) {
            GridUnsafe.putLong(b, GridUnsafe.BYTE_ARR_OFF + off, Long.reverseBytes(value));
        } else {
            GridUnsafe.putLong(b, GridUnsafe.BYTE_ARR_OFF + off, value);
        }
    }

    public static long getLongLittleEndian(byte[] b, int off) {
        long value = GridUnsafe.getLong(b, GridUnsafe.BYTE_ARR_OFF + off);

        return GridUnsafe.IS_BIG_ENDIAN ? Long.reverseBytes(value) : value;
    }
}
