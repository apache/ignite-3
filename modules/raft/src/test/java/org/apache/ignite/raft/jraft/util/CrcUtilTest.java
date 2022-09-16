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

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CrcUtilTest {

    @Test
    public void testCrc64() {
        byte[] bs = "hello world".getBytes(UTF_8);
        long c = CrcUtil.crc64(bs);
        assertEquals(c, CrcUtil.crc64(bs));
        assertEquals(c, CrcUtil.crc64(bs));
        assertEquals(c, CrcUtil.crc64(bs, 0, bs.length));

        ByteBuffer buf = ByteBuffer.wrap(bs);
        assertEquals(c, CrcUtil.crc64(buf));

        buf = ByteBuffer.allocateDirect(bs.length);
        buf.put(bs);
        buf.flip();
        assertEquals(c, CrcUtil.crc64(buf));
    }
}
