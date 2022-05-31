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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Useful class for testing a checkpoint.
 */
class CheckpointTestUtils {
    /**
     * Returns new instance of {@link CheckpointReadWriteLock}.
     *
     * @param log Logger.
     */
    static CheckpointReadWriteLock newReadWriteLock(IgniteLogger log) {
        return new CheckpointReadWriteLock(new ReentrantReadWriteLockWithTracking(log, 5_000));
    }

    /**
     * Returns mocked instance of {@link PageMemoryDataRegion}.
     *
     * @param persistent Persistent data region.
     * @param pageMemory Persistent page memory.
     */
    static PageMemoryDataRegion newDataRegion(boolean persistent, PageMemoryImpl pageMemory) {
        PageMemoryDataRegion mock = mock(PageMemoryDataRegion.class);

        when(mock.persistent()).thenReturn(persistent);

        when(mock.pageMemory()).thenReturn(pageMemory);

        return mock;
    }
}
