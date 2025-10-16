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

            IndexFileMetaArray newFileMetas = fileMetas.add(indexFileMeta);

            // Simple assignment would suffice, since we only have one thread writing to this field, but we use compareAndSet to verify
            // this invariant, just in case.
            boolean updated = FILE_METAS_VH.compareAndSet(this, fileMetas, newFileMetas);

            assert updated : "Concurrent writes detected";
        }
    }

    private final Deque<IndexMetaArrayHolder> fileMetaDeque = new ConcurrentLinkedDeque<>();

    GroupIndexMeta(IndexFileMeta startFileMeta) {
        fileMetaDeque.add(new IndexMetaArrayHolder(startFileMeta));
    }

    void addIndexMeta(IndexFileMeta indexFileMeta) {
        IndexMetaArrayHolder curFileMetas = fileMetaDeque.getLast();

        long curLastLogIndex = curFileMetas.fileMetas.lastLogIndexExclusive();

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

    long firstLogIndexInclusive() {
        for (IndexMetaArrayHolder indexMetaArrayHolder : fileMetaDeque) {
            IndexFileMetaArray fileMetas = indexMetaArrayHolder.fileMetas;

            if (fileMetas.size() > 0) {
                return fileMetas.firstLogIndexInclusive();
            }
        }

        return -1;
    }

    long lastLogIndexExclusive() {
        return fileMetaDeque.getLast().fileMetas.lastLogIndexExclusive();
    }
}
