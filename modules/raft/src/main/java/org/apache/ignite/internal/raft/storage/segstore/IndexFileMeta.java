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
    private final long firstLogIndex;

    private final long lastLogIndex;

    private final int indexFilePayloadOffset;

    private final int indexFileOrdinal;

    IndexFileMeta(long firstLogIndex, long lastLogIndex, int indexFilePayloadOffset, int indexFileOrdinal) {
        this.firstLogIndex = firstLogIndex;
        this.lastLogIndex = lastLogIndex;
        this.indexFilePayloadOffset = indexFilePayloadOffset;
        this.indexFileOrdinal = indexFileOrdinal;
    }

    /**
     * Returns the inclusive lower bound of log indices stored in the index file for the Raft Group.
     */
    long firstLogIndex() {
        return firstLogIndex;
    }

    /**
     * Returns the inclusive upper bound of log indices stored in the index file for the Raft Group.
     */
    long lastLogIndex() {
        return lastLogIndex;
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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexFileMeta that = (IndexFileMeta) o;
        return firstLogIndex == that.firstLogIndex && lastLogIndex == that.lastLogIndex
                && indexFilePayloadOffset == that.indexFilePayloadOffset
                && indexFileOrdinal == that.indexFileOrdinal;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(firstLogIndex);
        result = 31 * result + Long.hashCode(lastLogIndex);
        result = 31 * result + indexFilePayloadOffset;
        result = 31 * result + indexFileOrdinal;
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
