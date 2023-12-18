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

package org.apache.ignite.internal.raft.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DefaultByteBuffersPool}.
 */
class DefaultByteBuffersPoolTest {
    @Test
    public void testPoolOfOne() {
        var pool = new DefaultByteBuffersPool(1);

        ByteBuffer buffer = pool.borrow();

        assertNotNull(buffer);

        assertEquals(0, buffer.position());
        assertEquals(OptimizedMarshaller.DEFAULT_BUFFER_SIZE, buffer.capacity());

        assertNull(pool.borrow());

        ByteBuffer newBuffer = ByteBuffer.allocate(OptimizedMarshaller.MAX_CACHED_BUFFER_BYTES / 2);

        pool.release(newBuffer);

        assertSame(newBuffer, pool.borrow());

        assertNull(pool.borrow());
    }

    @Test
    public void testPoolOfN() {
        int capacity = 10;

        var pool = new DefaultByteBuffersPool(capacity);

        for (int i = 0; i < capacity; i++) {
            assertNotNull(pool.borrow());
        }

        assertNull(pool.borrow());
    }
}
