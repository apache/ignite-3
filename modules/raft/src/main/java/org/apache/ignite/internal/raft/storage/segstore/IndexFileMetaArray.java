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

package org.apache.ignite.internal.raft.storage.segstore;

import java.util.Arrays;
import org.jetbrains.annotations.Nullable;

/**
 * An array of {@link IndexFileMeta}.
 *
 * <p>Reads from multiple threads are thread-safe, but writes are expected to be done from a single thread only.
 */
class IndexFileMetaArray {
    static final int INITIAL_CAPACITY = 10;

    private final IndexFileMeta[] array;

    private final int size;

    IndexFileMetaArray(IndexFileMeta initialMeta) {
        this.array = new IndexFileMeta[INITIAL_CAPACITY];
        this.array[0] = initialMeta;

        this.size = 1;
    }

    private IndexFileMetaArray(IndexFileMeta[] array, int size) {
        this.array = array;
        this.size = size;
    }

    IndexFileMetaArray add(IndexFileMeta indexFileMeta) {
        assert indexFileMeta.firstLogIndexInclusive() == array[size - 1].lastLogIndexExclusive() :
                String.format("Index File Metas must be contiguous. Expected log index: %d, actual log index: %d",
                        array[size - 1].lastLogIndexExclusive() + 1,
                        indexFileMeta.firstLogIndexInclusive()
                );

        // The array can be shared between multiple instances, but since it always grows and we read at most "size" elements,
        // we don't need to copy it every time.
        IndexFileMeta[] array = this.array;

        if (size == array.length) {
            array = Arrays.copyOf(array, array.length * 2);
        }

        array[size] = indexFileMeta;

        return new IndexFileMetaArray(array, size + 1);
    }

    IndexFileMeta get(int arrayIndex) {
        return array[arrayIndex];
    }

    int size() {
        return size;
    }

    long firstLogIndexInclusive() {
        return array[0].firstLogIndexInclusive();
    }

    long lastLogIndexExclusive() {
        return array[size - 1].lastLogIndexExclusive();
    }

    /**
     * Returns the {@link IndexFileMeta} containing the given Raft log index or {@code null} if no such meta exists.
     */
    @Nullable
    IndexFileMeta find(long logIndex) {
        int lowArrayIndex = 0;
        int highArrayIndex = size - 1;

        while (lowArrayIndex <= highArrayIndex) {
            int middleArrayIndex = (lowArrayIndex + highArrayIndex) >>> 1;

            IndexFileMeta midValue = array[middleArrayIndex];

            if (logIndex < midValue.firstLogIndexInclusive()) {
                highArrayIndex = middleArrayIndex - 1;
            } else if (logIndex >= midValue.lastLogIndexExclusive()) {
                lowArrayIndex = middleArrayIndex + 1;
            } else {
                return midValue;
            }
        }

        return null;
    }
}
