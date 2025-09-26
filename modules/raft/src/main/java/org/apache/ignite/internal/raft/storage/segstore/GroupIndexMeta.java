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
import org.jetbrains.annotations.Nullable;

class GroupIndexMeta {
    static class IndexFilePointer {
        private final int fileIndex;

        private final int offset;

        IndexFilePointer(int fileIndex, int offset) {
            this.fileIndex = fileIndex;
            this.offset = offset;
        }

        int fileIndex() {
            return fileIndex;
        }

        int offset() {
            return offset;
        }
    }

    private static final VarHandle FILE_METAS_VH;

    static {
        try {
            FILE_METAS_VH = MethodHandles.lookup().findVarHandle(GroupIndexMeta.class, "fileMetas", IndexFileMetaArray.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final int startFileIndex;

    @SuppressWarnings("FieldMayBeFinal") // Updated through a VarHandle.
    private volatile IndexFileMetaArray fileMetas;

    GroupIndexMeta(int startFileIndex, IndexFileMeta startFileMeta) {
        this.startFileIndex = startFileIndex;
        this.fileMetas = new IndexFileMetaArray(startFileMeta);
    }

    long firstLogIndex() {
        return fileMetas.get(0).firstLogIndex();
    }

    long lastLogIndex() {
        IndexFileMetaArray fileMetas = this.fileMetas;

        return fileMetas.get(fileMetas.size() - 1).lastLogIndex();
    }

    /**
     * Puts the given segment file offset under the given log index.
     */
    void addIndexMeta(IndexFileMeta indexFileMeta) {
        IndexFileMetaArray fileMetas = this.fileMetas;

        IndexFileMetaArray newFileMetas = fileMetas.add(indexFileMeta);

        // Simple assignment would suffice, since we only have one thread writing to this field, but we use compareAndSet to verify
        // this invariant, just in case.
        boolean updated = FILE_METAS_VH.compareAndSet(this, fileMetas, newFileMetas);

        assert updated : "Concurrent writes detected";
    }

    @Nullable
    IndexFilePointer indexFilePointer(long logIndex) {
        IndexFileMetaArray fileMetas = this.fileMetas;

        int arrayIndex = fileMetas.find(logIndex);

        if (arrayIndex < 0) {
            return null;
        }

        IndexFileMeta meta = fileMetas.get(arrayIndex);

        return new IndexFilePointer(startFileIndex + arrayIndex, meta.indexFilePayloadOffset());
    }
}
