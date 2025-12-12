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

import org.apache.ignite.internal.tostring.S;

/**
 * Meta information about a payload in an index file.
 *
 * @see IndexFileManager
 */
class IndexFileMeta {
    private final long firstLogIndexInclusive;

    private final long lastLogIndexExclusive;

    private final int indexFilePayloadOffset;

    private final int indexFileOrdinal;

    IndexFileMeta(long firstLogIndexInclusive, long lastLogIndexExclusive, int indexFilePayloadOffset, int indexFileOrdinal) {
        assert firstLogIndexInclusive >= 0 : "Invalid first log index: " + firstLogIndexInclusive;
        assert lastLogIndexExclusive >= 0 : "Invalid first log index: " + firstLogIndexInclusive;

        if (lastLogIndexExclusive < firstLogIndexInclusive) {
            throw new IllegalArgumentException("Invalid log index range: [" + firstLogIndexInclusive + ", " + lastLogIndexExclusive + ").");
        }

        if (indexFileOrdinal < 0) {
            throw new IllegalArgumentException("Invalid index file ordinal: " + indexFileOrdinal);
        }

        this.firstLogIndexInclusive = firstLogIndexInclusive;
        this.lastLogIndexExclusive = lastLogIndexExclusive;
        this.indexFilePayloadOffset = indexFilePayloadOffset;
        this.indexFileOrdinal = indexFileOrdinal;
    }

    /**
     * Returns the inclusive lower bound of log indices stored in the index file for the Raft Group.
     */
    long firstLogIndexInclusive() {
        return firstLogIndexInclusive;
    }

    /**
     * Returns the exclusive upper bound of log indices stored in the index file for the Raft Group.
     */
    long lastLogIndexExclusive() {
        return lastLogIndexExclusive;
    }

    /**
     * Returns the offset of the payload for the Raft Group in the index file.
     */
    int indexFilePayloadOffset() {
        return indexFilePayloadOffset;
    }

    /**
     * Returns the ordinal of the index file.
     */
    int indexFileOrdinal() {
        return indexFileOrdinal;
    }

    /**
     * Returns {@code true} if the index meta is empty. This happens if some data was inserted but then the log suffix got truncated,
     * completely wiping it out.
     */
    boolean isEmpty() {
        return firstLogIndexInclusive == lastLogIndexExclusive;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexFileMeta that = (IndexFileMeta) o;
        return firstLogIndexInclusive == that.firstLogIndexInclusive && lastLogIndexExclusive == that.lastLogIndexExclusive
                && indexFilePayloadOffset == that.indexFilePayloadOffset
                && indexFileOrdinal == that.indexFileOrdinal;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(firstLogIndexInclusive);
        result = 31 * result + Long.hashCode(lastLogIndexExclusive);
        result = 31 * result + indexFilePayloadOffset;
        result = 31 * result + indexFileOrdinal;
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
