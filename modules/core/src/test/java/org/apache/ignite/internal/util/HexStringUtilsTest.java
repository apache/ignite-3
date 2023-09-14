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

import static org.apache.ignite.internal.util.HexStringUtils.toHexString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class HexStringUtilsTest {
    @Test
    void testToHexStringByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(8);

        assertEquals("00000000ffffaaaa", toHexString(buffer.rewind().putLong(0xffffaaaaL).rewind()));
        assertEquals("00000000aaaabbbb", toHexString(buffer.rewind().putLong(0xaaaabbbbL).rewind()));

        assertEquals("", toHexString(buffer.rewind().putLong(0xffffaaaaL)));
        assertEquals("", toHexString(buffer.rewind().putLong(0xaaaabbbbL)));

        assertEquals("ffffaaaa", toHexString(buffer.rewind().putLong(0xffffaaaaL).position(4)));
        assertEquals("aaaabbbb", toHexString(buffer.rewind().putLong(0xaaaabbbbL).position(4)));

        assertEquals("00001111", toHexString(buffer.rewind().limit(8).putLong(0x1111ffffaaaaL).rewind().limit(4)));
        assertEquals("00002222", toHexString(buffer.rewind().limit(8).putLong(0x2222aaaabbbbL).rewind().limit(4)));

        buffer.rewind().limit(8);

        // Checks slice.

        assertEquals("ffffaaaa", toHexString(buffer.rewind().putLong(0xffffaaaaL).position(4).slice()));
        assertEquals("aaaabbbb", toHexString(buffer.rewind().putLong(0xaaaabbbbL).position(4).slice()));
    }
}
