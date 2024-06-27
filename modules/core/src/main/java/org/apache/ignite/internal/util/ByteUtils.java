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

package org.apache.ignite.internal.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class provides various method for manipulating with bytes.
 */
public class ByteUtils {
    /**
     * Constructs {@code long} from byte array in Big Endian order.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Long value.
     */
    public static long bytesToLong(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Math.min(Long.BYTES, bytes.length - off);

        long res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            res = (res << 8) | (0xffL & bytes[off + i]);
        }

        return res;
    }

    /**
     * Constructs {@code long} from byte array in Big Endian order with offset equal to 0.
     *
     * @param bytes Array of bytes.
     * @return Long value.
     * @see #bytesToLong(byte[], int)
     */
    public static long bytesToLong(byte[] bytes) {
        return bytesToLong(bytes, 0);
    }

    /**
     * Constructs {@code long} from byte array created with {@link #longToBytesKeepingOrder(long)}.
     *
     * @param bytes Array of bytes.
     * @return Long value.
     */
    public static long bytesToLongKeepingOrder(byte[] bytes) {
        return bytesToLong(bytes) ^ 0x0080808080808080L;
    }

    /**
     * Converts a primitive {@code long} value to a byte array in Big Endian order.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytes(long l) {
        return putLongToBytes(l, new byte[8], 0);
    }

    /**
     * Converts a primitive {@code long} value to a byte array than can be compared with {@link ByteBuffer#compareTo(ByteBuffer)} with the
     * same order as the original {@code long}.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytesKeepingOrder(long l) {
        return longToBytes(l ^ 0x0080808080808080L);
    }

    /**
     * Converts a primitive {@code long} value to a byte array in Big Endian order and stores it in the specified byte array.
     *
     * @param l Unsigned long value.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static byte[] putLongToBytes(long l, byte[] bytes, int off) {
        assert bytes != null;
        assert bytes.length >= off + Long.BYTES;

        for (int i = Long.BYTES - 1; i >= 0; i--) {
            bytes[off + i] = (byte) l;
            l >>>= 8;
        }

        return bytes;
    }

    /**
     * Constructs {@code int} from byte array in Big Endian order.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Integer value.
     */
    public static int bytesToInt(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Math.min(Integer.BYTES, bytes.length - off);

        int res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            res = (res << 8) | (0xff & bytes[off + i]);
        }

        return res;
    }

    /**
     * Constructs {@code int} from byte array in Big Endian order with offset equal to 0.
     *
     * @param bytes Array of bytes.
     * @return Integer value.
     * @see #bytesToInt(byte[], int)
     */
    public static int bytesToInt(byte[] bytes) {
        return bytesToInt(bytes, 0);
    }

    /**
     * Constructs {@code int} from byte array created with {@link #intToBytesKeepingOrder(int)}.
     *
     * @param bytes Array of bytes.
     * @return Integer value.
     */
    public static int bytesToIntKeepingOrder(byte[] bytes) {
        return bytesToInt(bytes) ^ 0x00808080;
    }


    /**
     * Converts a primitive {@code int} value to a byte array in Big Endian order.
     *
     * @param i Integer value.
     * @return Array of bytes.
     */
    public static byte[] intToBytes(int i) {
        return putIntToBytes(i, new byte[4], 0);
    }

    /**
     * Converts a primitive {@code int} value to a byte array than can be compared with {@link ByteBuffer#compareTo(ByteBuffer)} with the
     * same order as the original {@code int}.
     *
     * @param i Integer value.
     * @return Array of bytes.
     */
    public static byte[] intToBytesKeepingOrder(int i) {
        return intToBytes(i ^ 0x00808080);
    }

    /**
     * Converts a primitive {@code int} value to a byte array in Big Endian order and stores it in the specified byte array.
     *
     * @param i Unsigned int value.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @return The array.
     */
    public static byte[] putIntToBytes(int i, byte[] bytes, int off) {
        assert bytes != null;
        assert bytes.length >= off + Integer.BYTES;

        for (int k = Integer.BYTES - 1; k >= 0; k--) {
            bytes[off + k] = (byte) i;
            i >>>= 8;
        }

        return bytes;
    }

    /**
     * Converts a {@link Boolean} value to a single byte.
     *
     * @param val Boolean value.
     * @return Byte representation of a boolean value.
     */
    public static byte booleanToByte(boolean val) {
        return (byte) (val ? 1 : 0);
    }

    /**
     * Converts a {@link Byte} value to a boolean.
     *
     * @param val Byte value.
     * @return Boolean representation of a byte value.
     */
    public static boolean byteToBoolean(byte val) {
        if (val == 1) {
            return true;
        } else {
            assert val == 0 : "Unexpected " + val;

            return false;
        }
    }

    /**
     * Serializes an object to byte array using native java serialization mechanism.
     *
     * @param obj Object to serialize.
     * @return Byte array.
     */
    public static byte[] toBytes(Object obj) {
        try (
                var bos = new ByteArrayOutputStream();
                var out = new ObjectOutputStream(bos)
        ) {
            out.writeObject(obj);

            out.flush();

            return bos.toByteArray();
        } catch (IOException e) {
            throw new IgniteInternalException(String.format("Could not serialize a class [cls=%s]", obj.getClass()), e);
        }
    }

    /**
     * Deserializes an object from byte array using native java serialization mechanism.
     *
     * @param bytes Byte array.
     * @return Object.
     */
    public static <T> T fromBytes(byte[] bytes) {
        try (
                var bis = new ByteArrayInputStream(bytes);
                var in = new ObjectInputStream(bis)
        ) {
            return (T) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IgniteInternalException("Could not deserialize an object", e);
        }
    }

    /**
     * Converts a string to a byte array using {@link StandardCharsets#UTF_8}, {@code null} if {@code s} is {@code null}.
     *
     * @param s String to convert.
     * @see #stringFromBytes(byte[])
     */
    public static byte @Nullable [] stringToBytes(@Nullable String s) {
        return s == null ? null : s.getBytes(UTF_8);
    }

    /**
     * Converts a byte array to a string using {@link StandardCharsets#UTF_8}, {@code null} if {@code bytes} is {@code null}.
     *
     * @param bytes String bytes.
     */
    public static @Nullable String stringFromBytes(byte @Nullable [] bytes) {
        return bytes == null ? null : new String(bytes, UTF_8);
    }

    /**
     * Converts a byte buffer to a byte array.
     *
     * @param buffer Byte buffer from which we copy bytes.
     */
    public static byte[] toByteArray(ByteBuffer buffer) {
        var bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        buffer.flip();
        return bytes;
    }

    /**
     * Converts a byte array list to a byte buffer list.
     *
     * @param byteBufferList List of byte buffers.
     */
    public static List<byte[]> toByteArrayList(List<ByteBuffer> byteBufferList) {
        var result = new ArrayList<byte[]>(byteBufferList.size());

        for (int i = 0; i < byteBufferList.size(); i++) {
            result.add(toByteArray(byteBufferList.get(i)));
        }

        return result;
    }
}
