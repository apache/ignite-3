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

package org.apache.ignite.internal.binarytuple;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for class {@link ExpandableByteBuffer}.
 */
public class ExpandableByteBufferTest {
    @Test
    void minimumCapacity() {
        {
            ExpandableByteBuffer buf = new ExpandableByteBuffer(0);

            assertEquals(0, buf.position());
            assertEquals(ExpandableByteBuffer.MINIMUM_CAPACITY, buf.capacity());
            assertEquals(ExpandableByteBuffer.MINIMUM_CAPACITY, buf.remaining());
        }

        {
            ExpandableByteBuffer buf = new ExpandableByteBuffer(-1);

            assertEquals(0, buf.position());
            assertEquals(ExpandableByteBuffer.MINIMUM_CAPACITY, buf.capacity());
            assertEquals(ExpandableByteBuffer.MINIMUM_CAPACITY, buf.remaining());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 7})
    public void correctUnwrap(int arrayLen) {
        for (NativeTypeSpec type : NativeTypeSpec.values()) {
            ExpandableByteBuffer buf = new ExpandableByteBuffer(ThreadLocalRandom.current().nextInt(10));

            switch (type) {
                case INT8:
                    buf.put(0, (byte) 1);
                    break;

                case INT16:
                    buf.putShort(0, (short) 1);
                    break;

                case INT32:
                    buf.putInt(0, 1);
                    break;

                case INT64:
                    buf.putLong(0, 1L);
                    break;

                case FLOAT:
                    buf.putFloat(0, 1.0f);
                    break;

                case DOUBLE:
                    buf.putDouble(0, 1.0d);
                    break;

                case BYTES:
                    buf.put(0, new byte[arrayLen]);

//                    String targetStr = String.valueOf();
//                    try {
//                        buf.putString(0, targetStr, StandardCharsets.UTF_8.newEncoder());
//                    } catch (CharacterCodingException e) {
//                        fail(e);
//                    }
                    break;

                default:
                    // no op.
            }

            checkBufState(buf);
        }
    }

    private static void checkBufState(ExpandableByteBuffer buf) {
        ByteBuffer bb = buf.unwrap();
        assertEquals(0, bb.position());
    }

    @Test
    public void allTypesDirectOrder() {
        ExpandableByteBuffer buf = new ExpandableByteBuffer(5);

        byte[] targetBytes = {1, 2, 3, 4, 5, 6, 7};
//        String targetStr = "abcdefg";

        buf.put(0, (byte) 1);
        buf.putShort(1, (short) 2);
        buf.putInt(3, 3);
        buf.putLong(7, 4L);
        buf.putFloat(15, 5.f);
        buf.putDouble(19, 6.d);
        buf.put(27, targetBytes);
        //buf.putString(34, targetStr, StandardCharsets.UTF_8.newEncoder());

        byte[] arr = buf.unwrap().array();
        assertEquals(34, arr.length);

        ByteBuffer b = ByteBuffer.wrap(arr);
        b.order(ByteOrder.LITTLE_ENDIAN);

        assertEquals((byte) 1, b.get(0));
        assertEquals((short) 2, b.getShort(1));
        assertEquals(3, b.getInt(3));
        assertEquals(4L, b.getLong(7));
        assertEquals(5.f, b.getFloat(15));
        assertEquals(6.d, b.getDouble(19));

        byte[] bytes = new byte[7];
        b.position(27);
        b.get(bytes);

        assertArrayEquals(targetBytes, bytes);
    }

    @Test
    public void allTypesReverseOrder() {
        ExpandableByteBuffer buf = new ExpandableByteBuffer(5);

        byte[] targetBytes = {1, 2, 3, 4, 5, 6, 7};
        String targetStr = "abcdefg";

        buf.put(27, targetBytes);
        buf.putDouble(19, 6.d);
        buf.putFloat(15, 5.f);
        buf.putLong(7, 4L);
        buf.putInt(3, 3);
        buf.putShort(1, (short) 2);
        buf.put(0, (byte) 1);

        byte[] arr = buf.unwrap().array();
        assertEquals(41, arr.length);

        ByteBuffer b = ByteBuffer.wrap(arr);
        b.order(ByteOrder.LITTLE_ENDIAN);

        assertEquals((byte) 1, b.get(0));
        assertEquals((short) 2, b.getShort(1));
        assertEquals(3, b.getInt(3));
        assertEquals(4L, b.getLong(7));
        assertEquals(5.f, b.getFloat(15));
        assertEquals(6.d, b.getDouble(19));

        byte[] bytes = new byte[7];
        b.position(27);
        b.get(bytes);

        assertArrayEquals(targetBytes, bytes);

        b.position(34);
        b.get(bytes);

        assertEquals(targetStr, new String(bytes, StandardCharsets.UTF_8));
    }
}
