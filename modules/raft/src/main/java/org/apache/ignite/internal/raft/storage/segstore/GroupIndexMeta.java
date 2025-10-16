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

/**
 * Represents in-memory meta information about a particular Raft group stored in an index file.
 */
class GroupIndexMeta {
    private static final VarHandle FILE_METAS_VH;

    static {
        try {
            FILE_METAS_VH = MethodHandles.lookup().findVarHandle(GroupIndexMeta.class, "fileMetas", IndexFileMetaArray.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("FieldMayBeFinal") // Updated through a VarHandle.
    private volatile IndexFileMetaArray fileMetas;

    GroupIndexMeta(IndexFileMeta startFileMeta) {
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

    /**
     * Returns a file pointer that uniquely identifies the index file for the given log index. Returns {@code null} if the given log index
     * is not found in any of the index files in this group.
     */
    @Nullable
    IndexFileMeta indexMeta(long logIndex) {
        return fileMetas.find(logIndex);
    }

    long firstLogIndex() {
        return fileMetas.get(0).firstLogIndex();
    }

    long lastLogIndex() {
        IndexFileMetaArray fileMetas = this.fileMetas;

        return fileMetas.get(fileMetas.size() - 1).lastLogIndex();
    }
}
