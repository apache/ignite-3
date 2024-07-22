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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.SortedIndexLocker;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.tx.LockManager;

/** Class that creates index storage and locker decorators for given partition. */
public abstract class IndexWrapper {
    final InternalTable tbl;
    final LockManager lockManager;
    final int indexId;
    final ColumnsExtractor indexRowResolver;

    private IndexWrapper(InternalTable tbl, LockManager lockManager, int indexId, ColumnsExtractor indexRowResolver) {
        this.tbl = tbl;
        this.lockManager = lockManager;
        this.indexId = indexId;
        this.indexRowResolver = indexRowResolver;
    }

    /**
     * Creates schema aware index storage wrapper.
     *
     * @param partitionId Partition id.
     */
    abstract TableSchemaAwareIndexStorage getStorage(int partitionId);

    /**
     * Creates schema aware index locker.
     *
     * @param partitionId Partition id.
     */
    abstract IndexLocker getLocker(int partitionId);

    /** {@link IndexWrapper} for sorted indexes. */
    static class SortedIndexWrapper extends IndexWrapper {
        SortedIndexWrapper(InternalTable tbl, LockManager lockManager, int indexId, ColumnsExtractor indexRowResolver) {
            super(tbl, lockManager, indexId, indexRowResolver);
        }

        @Override
        TableSchemaAwareIndexStorage getStorage(int partitionId) {
            IndexStorage index = tbl.storage().getIndex(partitionId, indexId);

            assert index != null : tbl.name() + " part " + partitionId;

            return new TableSchemaAwareIndexStorage(
                    indexId,
                    index,
                    indexRowResolver
            );
        }

        @Override
        IndexLocker getLocker(int partitionId) {
            IndexStorage index = tbl.storage().getIndex(partitionId, indexId);

            assert index != null : tbl.name() + " part " + partitionId;

            return new SortedIndexLocker(
                    indexId,
                    partitionId,
                    lockManager,
                    (SortedIndexStorage) index,
                    indexRowResolver
            );
        }
    }

    public static boolean fsm_err = false;

    /** {@link IndexWrapper} for hash indexes. */
    static class HashIndexWrapper extends IndexWrapper {
        private final boolean unique;

        HashIndexWrapper(InternalTable tbl, LockManager lockManager, int indexId, ColumnsExtractor indexRowResolver,
                boolean unique) {
            super(tbl, lockManager, indexId, indexRowResolver);
            this.unique = unique;
        }

        @Override
        TableSchemaAwareIndexStorage getStorage(int partitionId) {
            IndexStorage index = tbl.storage().getIndex(partitionId, indexId);

            //assert !fsm_err;

            assert index != null : "tableId=" + tbl.tableId() + ", indexId=" + indexId + ", partitionId=" + partitionId;

            return new TableSchemaAwareIndexStorage(
                    indexId,
                    index,
                    indexRowResolver
            );
        }

        @Override
        IndexLocker getLocker(int partitionId) {
            return new HashIndexLocker(
                    indexId,
                    unique,
                    lockManager,
                    indexRowResolver
            );
        }
    }
}
