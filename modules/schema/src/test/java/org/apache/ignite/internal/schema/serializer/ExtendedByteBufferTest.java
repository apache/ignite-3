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

package org.apache.ignite.internal.schema.serializer;

import org.apache.ignite.internal.schema.marshaller.schema.ExtendedByteBuffer;
import org.apache.ignite.internal.schema.marshaller.schema.ExtendedByteBufferFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 *
 */
public class ExtendedByteBufferTest {
    /**
     *
     */
    @Test
    public void calcSizeByteBufferTest() {
        ExtendedByteBuffer buf = ExtendedByteBufferFactory.calcSize();

        buf.put((byte)1); // 1
        buf.putShort((short)1); //2
        buf.putInt(1); //4
        buf.put(new byte[]{1,2,3,4,5,6,7,8}); //4 + 8
        buf.putString("test"); // 4 + 4

        assertEquals(27, buf.size());

        assertThrows(UnsupportedOperationException.class, buf::get);
        assertThrows(UnsupportedOperationException.class, buf::getInt);
        assertThrows(UnsupportedOperationException.class, buf::getShort);
        assertThrows(UnsupportedOperationException.class, buf::getString);
        assertThrows(UnsupportedOperationException.class, buf::array);
    }

    /**
     *
     */
    @Test
    public void extendedByteBufferImplTest() {
        ExtendedByteBuffer calcSizeBuf = ExtendedByteBufferFactory.calcSize();

        calcSizeBuf.put((byte)1); // 1
        calcSizeBuf.putShort((short)1); //2
        calcSizeBuf.putInt(1); //4
        calcSizeBuf.put(new byte[]{1,2,3,4,5,6,7,8}); //4 + 8
        calcSizeBuf.putString("test"); // 4 + 4

        ExtendedByteBuffer buf = ExtendedByteBufferFactory.allocate(calcSizeBuf.size());

        assertThrows(UnsupportedOperationException.class, buf::size);

        buf.put((byte)1); // 1
        buf.putShort((short)1); //2
        buf.putInt(1); //4
        buf.putInt(8); //4
        buf.put(new byte[]{1,2,3,4,5,6,7,8}); //4 + 8
        buf.putString("test"); // 4 + 4

        byte[] array = buf.array();

        assertEquals(calcSizeBuf.size(), array.length);

        ExtendedByteBuffer wrapBuf = ExtendedByteBufferFactory.wrap(array);

        assertThrows(UnsupportedOperationException.class, wrapBuf::size);

        assertEquals(1, wrapBuf.get());
        assertEquals(1, wrapBuf.getShort());
        assertEquals(1, wrapBuf.getInt());

        assertEquals(8, wrapBuf.getInt());

        for (int i = 1; i < 9; i++)
            assertEquals(i, wrapBuf.get());

        assertEquals("test", wrapBuf.getString());
    }
}
