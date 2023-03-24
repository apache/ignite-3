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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;
import org.apache.ignite.lang.IgniteInternalException;

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
     * Converts a primitive {@code long} value to a byte array in Big Endian order.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytes(long l) {
        return putLongToBytes(l, new byte[8], 0);
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
     * Converts a primitive {@code int} value to a byte array in Big Endian order.
     *
     * @param i Integer value.
     * @return Array of bytes.
     */
    public static byte[] intToBytes(int i) {
        return putIntToBytes(i, new byte[4], 0);
    }

    /**
     * Converts a primitive {@code int} value to a byte array in Big Endian order and stores it in the specified byte array.
     *
     * @param i Unsigned int value.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @return Number of bytes overwritten in {@code bytes} array.
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
     * Converts a {@link UUID} value to a byte array in Big Endian order and stores it in the specified byte array.
     *
     * @param uuid UUID.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static byte[] putUuidToBytes(UUID uuid, byte[] bytes, int off) {
        putLongToBytes(uuid.getMostSignificantBits(), bytes, off);
        putLongToBytes(uuid.getLeastSignificantBits(), bytes, off + Long.BYTES);

        return bytes;
    }

    /**
     * Constructs {@link UUID} from byte array in Big Endian order.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return UUID value.
     */
    public static UUID bytesToUuid(byte[] bytes, int off) {
        long msb = bytesToLong(bytes, off);
        long lsb = bytesToLong(bytes, off + Long.BYTES);

        return new UUID(msb, lsb);
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
}
