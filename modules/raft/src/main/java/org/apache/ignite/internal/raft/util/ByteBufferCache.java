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

import java.nio.ByteBuffer;
import org.jetbrains.annotations.Nullable;

/**
 * Byte buffer cache for {@link org.apache.ignite.internal.raft.util.OptimizedMarshaller}. Helps re-using old buffers,
 * saving some time on allocations.
 */
public interface ByteBufferCache {
    /**
     * Default "no cache" instance for always-empty cache.
     */
    ByteBufferCache NO_CACHE = new EmptyByteBufferCache();

    /**
     * Removes one buffer from cache and returns it, if possible. Returns {@code null} otherwise.
     */
    @Nullable ByteBuffer borrow();

    /**
     * Adds a buffer to the cache. Might not modify the state of the cache, it it's already full.
     */
    void offer(ByteBuffer buffer);
}
