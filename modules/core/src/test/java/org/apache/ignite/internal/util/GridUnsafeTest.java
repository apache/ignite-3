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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/** Tests for {@link GridUnsafe}. */
class GridUnsafeTest {
    @Test
    void getBytes() {
        // We must use manual memory allocation, because an instance created by "allocateDirect" might be garbage-collected in the middle
        // of the test.
        int length = 4;
        ByteBuffer buffer = GridUnsafe.allocateBuffer(length);

        try {
            long address = GridUnsafe.bufferAddress(buffer);
            for (int i = 0; i < length; i++) {
                GridUnsafe.putByte(address + i, (byte) i);
            }

            assertArrayEquals(new byte[]{0, 1, 2, 3}, GridUnsafe.getBytes(address, 0, 4));
            assertArrayEquals(new byte[]{0, 1, 2}, GridUnsafe.getBytes(address, 0, 3));
            assertArrayEquals(new byte[]{1, 2, 3}, GridUnsafe.getBytes(address, 1, 3));
        } finally {
            GridUnsafe.freeBuffer(buffer);
        }
    }
}
