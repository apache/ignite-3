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

package org.apache.ignite.internal.pagememory.mem;

import org.jetbrains.annotations.Nullable;

/**
 * Direct memory provider interface. Not thread-safe.
 */
public interface DirectMemoryProvider {
    /**
     * Initializes provider with the chunk sizes.
     *
     * @param chunkSizes Chunk sizes.
     */
    // TODO: IGNITE-16350 Consider deleting a method
    void initialize(long[] chunkSizes);

    /**
     * Shuts down the provider.
     *
     * @param deallocate {@code True} to deallocate memory, {@code false} to allow memory reuse.
     */
    void shutdown(boolean deallocate);

    /**
     * Attempts to allocate next memory region. Will return {@code null} if no more regions are available.
     *
     * @return Next memory region.
     */
    @Nullable
    // TODO: IGNITE-16350 Consider adding a region size argument
    DirectMemoryRegion nextRegion();
}
