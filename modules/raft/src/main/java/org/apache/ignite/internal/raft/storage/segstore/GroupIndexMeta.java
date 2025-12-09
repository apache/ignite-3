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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.jetbrains.annotations.Nullable;

/**
 * Represents in-memory meta information about a particular Raft group stored in an index file.
 */
class GroupIndexMeta {
    private static class IndexMetaArrayHolder {
        private static final VarHandle FILE_METAS_VH;

        static {
            try {
                FILE_METAS_VH = MethodHandles.lookup().findVarHandle(IndexMetaArrayHolder.class, "fileMetas", IndexFileMetaArray.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        @SuppressWarnings("FieldMayBeFinal") // Updated through a VarHandle.
        volatile IndexFileMetaArray fileMetas;

        IndexMetaArrayHolder(IndexFileMeta startFileMeta) {
            this.fileMetas = new IndexFileMetaArray(startFileMeta);
        }

        void addIndexMeta(IndexFileMeta indexFileMeta) {
            IndexFileMetaArray fileMetas = this.fileMetas;

            setFileMetas(fileMetas, fileMetas.add(indexFileMeta));
        }

        long firstLogIndexInclusive() {
            return fileMetas.firstLogIndexInclusive();
        }

        long lastLogIndexExclusive() {
            return fileMetas.lastLogIndexExclusive();
        }

        /**
         * Removes all metas which log indices are smaller than the given value.
         */
        void truncateIndicesSmallerThan(long firstLogIndexKept) {
            IndexFileMetaArray fileMetas = this.fileMetas;

            setFileMetas(fileMetas, fileMetas.truncateIndicesSmallerThan(firstLogIndexKept));
        }

        private void setFileMetas(IndexFileMetaArray fileMetas, IndexFileMetaArray newFileMetas) {
            // Simple assignment would suffice, since we only have one thread writing to this field, but we use compareAndSet to verify
            // this invariant, just in case.
            boolean updated = FILE_METAS_VH.compareAndSet(this, fileMetas, newFileMetas);

            assert updated : "Concurrent writes detected";
        }
    }

    /**
     * A deque of index file meta blocks.
     *
     * <p>When a new index file is created, its meta is appended to the last block (represented by a {@link IndexMetaArrayHolder}) of the
     * deque if its {@link IndexFileMeta#firstLogIndexInclusive()} matches the most recent {@link IndexFileMeta#lastLogIndexExclusive()} in
     * the block. I.e. consecutive index file metas are merged into a single block, no new elements are added to the deque.
     *
     * <p>The only case when a new block is added to the deque is if a log suffix truncation happened somewhere during an index file's
     * lifecycle. In this case, the new block will have its {@link IndexFileMeta#firstLogIndexInclusive()} smaller than the most recent
     * block's {@link IndexFileMeta#lastLogIndexExclusive()} (two blocks "overlap"). During search, the newer block will be checked first,
     * so elements in the range {@code [newBlock#firstLogIndexInclusive : oldBlock#lastLogIndexExclusive)} will be taken from the new block,
     * effectively overriding the old block's entries in this range.
     */
    private final Deque<IndexMetaArrayHolder> fileMetaDeque = new ConcurrentLinkedDeque<>();

    GroupIndexMeta(IndexFileMeta startFileMeta) {
        fileMetaDeque.add(new IndexMetaArrayHolder(startFileMeta));
    }

    void addIndexMeta(IndexFileMeta indexFileMeta) {
        IndexMetaArrayHolder curFileMetas = fileMetaDeque.getLast();

        long curLastLogIndex = curFileMetas.lastLogIndexExclusive();

        long newFirstLogIndex = indexFileMeta.firstLogIndexInclusive();

        assert newFirstLogIndex <= curLastLogIndex :
                String.format(
                        "Gaps between Index File Metas are not allowed. Last log index: %d, new log index: %d",
                        curLastLogIndex, newFirstLogIndex
                );

        // Merge consecutive index metas into a single meta block. If there's an overlap (e.g. due to log truncation), start a new block,
        // which will override the previous one during search.
        if (curLastLogIndex == newFirstLogIndex) {
            curFileMetas.addIndexMeta(indexFileMeta);
        } else {
            fileMetaDeque.add(new IndexMetaArrayHolder(indexFileMeta));
        }
    }

    /**
     * Returns index file meta that uniquely identifies the index file for the given log index. Returns {@code null} if the given log index
     * is not found in any of the index files in this group.
     */
    @Nullable
    IndexFileMeta indexMeta(long logIndex) {
        Iterator<IndexMetaArrayHolder> it = fileMetaDeque.descendingIterator();

        while (it.hasNext()) {
            IndexFileMetaArray fileMetas = it.next().fileMetas;

            // Log suffix might have been truncated, so we can have an entry on the top of the queue that cuts off part of the search range.
            if (logIndex >= fileMetas.lastLogIndexExclusive()) {
                return null;
            }

            if (logIndex < fileMetas.firstLogIndexInclusive()) {
                continue;
            }

            IndexFileMeta indexMeta = fileMetas.find(logIndex);

            if (indexMeta != null) {
                return indexMeta;
            }
        }

        return null;
    }

    /**
     * Removes all index metas that have log indices smaller than the given value.
     */
    void truncatePrefix(long firstLogIndexKept) {
        Iterator<IndexMetaArrayHolder> it = fileMetaDeque.descendingIterator();

        // Find the most recent entry, which first index is smaller than firstLogIndexKept.
        while (it.hasNext()) {
            IndexMetaArrayHolder holder = it.next();

            long firstLogIndex = holder.firstLogIndexInclusive();

            if (firstLogIndex == firstLogIndexKept) {
                // We are right on the edge of meta range, keep this entry and simply drop everything older.
                break;
            } else if (firstLogIndex < firstLogIndexKept) {
                // Truncate this entry (possibly in its entirety) and drop everything older.
                if (holder.lastLogIndexExclusive() <= firstLogIndexKept) {
                    it.remove();
                } else {
                    holder.truncateIndicesSmallerThan(firstLogIndexKept);
                }

                break;
            }
        }

        // Remove all remaining entries.
        while (it.hasNext()) {
            it.next();
            it.remove();
        }
    }

    long firstLogIndexInclusive() {
        for (IndexMetaArrayHolder indexMetaArrayHolder : fileMetaDeque) {
            long firstLogIndex = indexMetaArrayHolder.firstLogIndexInclusive();

            // "firstLogIndexInclusive" can return -1 of the index file does not contain any entries for this group, only the truncation
            // record.
            if (firstLogIndex >= 0) {
                return firstLogIndex;
            }
        }

        return -1;
    }

    long lastLogIndexExclusive() {
        return fileMetaDeque.getLast().lastLogIndexExclusive();
    }
}
