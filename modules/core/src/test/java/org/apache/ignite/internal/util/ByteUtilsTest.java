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
import static org.apache.ignite.internal.util.ByteUtils.stringFromBytes;
import static org.apache.ignite.internal.util.ByteUtils.stringToBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** For {@link ByteUtils} testing. */
public class ByteUtilsTest {
    @Test
    void testStringToBytes() {
        assertNull(stringToBytes(null));

        assertArrayEquals("".getBytes(UTF_8), stringToBytes(""));
        assertArrayEquals("abc".getBytes(UTF_8), stringToBytes("abc"));
    }

    @Test
    void testStringFromBytes() {
        assertNull(stringFromBytes(null));

        assertEquals("", stringFromBytes(stringToBytes("")));
        assertEquals("abc", stringFromBytes(stringToBytes("abc")));
    }
}
