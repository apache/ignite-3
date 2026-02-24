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

import static java.lang.Math.toIntExact;
import static org.apache.ignite.internal.raft.storage.segstore.IndexFileManager.SEGMENT_FILE_OFFSET_SIZE;

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
                        array[size - 1].lastLogIndexExclusive(),
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
        IndexFileMeta firstMeta = array[0];
        IndexFileMeta lastMeta = array[size - 1];

        if (firstMeta.firstLogIndexInclusive() >= lastMeta.lastLogIndexExclusive()) {
            // Log for this group has been truncated.
            return -1;
        }

        return firstMeta.firstLogIndexInclusive();
    }

    long lastLogIndexExclusive() {
        return array[size - 1].lastLogIndexExclusive();
    }

    /**
     * Returns the {@link IndexFileMeta} containing the given Raft log index or {@code null} if no such meta exists.
     */
    @Nullable
    IndexFileMeta find(long logIndex) {
        int arrayIndex = findArrayIndex(logIndex);

        return arrayIndex == -1 ? null : array[arrayIndex];
    }

    private int findArrayIndex(long logIndex) {
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
                return middleArrayIndex;
            }
        }

        return -1;
    }

    IndexFileMetaArray truncateIndicesSmallerThan(long firstLogIndexKept) {
        int firstLogIndexKeptArrayIndex = findArrayIndex(firstLogIndexKept);

        assert firstLogIndexKeptArrayIndex >= 0 : String.format(
                "Missing entry for log index %d in range [%d:%d).",
                firstLogIndexKept, firstLogIndexInclusive(), lastLogIndexExclusive()
        );

        IndexFileMeta metaToUpdate = array[firstLogIndexKeptArrayIndex];

        int numEntriesToSkip = toIntExact(firstLogIndexKept - metaToUpdate.firstLogIndexInclusive());

        assert numEntriesToSkip >= 0 : String.format(
                "Trying to do a no-op prefix truncate from index %d in range [%d:%d).",
                firstLogIndexKept, firstLogIndexInclusive(), lastLogIndexExclusive()
        );

        // Move the payload offset pointer to skip truncated entries (each entry is 4 bytes).
        int adjustedPayloadOffset = metaToUpdate.indexFilePayloadOffset() + numEntriesToSkip * SEGMENT_FILE_OFFSET_SIZE;

        var trimmedMeta = new IndexFileMeta(
                firstLogIndexKept,
                metaToUpdate.lastLogIndexExclusive(),
                adjustedPayloadOffset,
                metaToUpdate.indexFileProperties()
        );

        // Create a new array: the trimmed meta becomes the first element, other elements with "firstLogIndexInclusive" larger
        // than "firstLogIndexKept" are copied from the old array.
        IndexFileMeta[] newArray = new IndexFileMeta[array.length];

        newArray[0] = trimmedMeta;

        int newSize = size - firstLogIndexKeptArrayIndex;

        if (newSize > 1) {
            System.arraycopy(array, firstLogIndexKeptArrayIndex + 1, newArray, 1, newSize - 1);
        }

        return new IndexFileMetaArray(newArray, newSize);
    }

    IndexFileMetaArray onIndexCompacted(FileProperties oldProperties, IndexFileMeta newMeta) {
        // Find index meta associated with the file being compacted.
        int smallestOrdinal = array[0].indexFileProperties().ordinal();

        assert oldProperties.ordinal() >= smallestOrdinal;

        int updateIndex = oldProperties.ordinal() - smallestOrdinal;

        if (updateIndex >= size) {
            return this;
        }

        IndexFileMeta[] newArray = array.clone();

        newArray[updateIndex] = newMeta;

        return new IndexFileMetaArray(newArray, size);
    }

    @Override
    public String toString() {
        return Arrays.toString(array);
    }
}
