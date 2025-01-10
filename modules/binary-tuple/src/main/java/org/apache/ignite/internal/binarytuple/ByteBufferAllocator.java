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

import java.nio.ByteBuffer;
import org.jetbrains.annotations.Nullable;

/**
 * Byte buffer allocator.
 */
public class ByteBufferAllocator {
    /** Allocated buffer. */
    private ByteBuffer buffer;

    /**
     * Allocates a new buffer if the current buffer's capacity is not sufficient.
     *
     * @param capacity Required capacity.
     * @return Buffer with required capacity.
     */
    public ByteBuffer allocateIfNeeded(int capacity) {
        if (buffer == null || buffer.capacity() < capacity) {
            return allocate(capacity);
        }

        return buffer;
    }

    /**
     * Allocates new buffer with required capacity.
     *
     * @param capacity Required capacity.
     * @return Buffer with required capacity.
     */
    public ByteBuffer allocate(int capacity) {
        buffer = ByteBuffer.allocate(capacity);

        return buffer;
    }

    /**
     * Returns the allocated buffer.
     *
     * @return Allocated buffer, or {@code null} if no buffer was allocated.
     */
    public @Nullable ByteBuffer buffer() {
        return buffer;
    }
}
