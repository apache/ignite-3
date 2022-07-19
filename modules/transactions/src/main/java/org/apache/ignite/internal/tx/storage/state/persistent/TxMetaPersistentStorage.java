/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.tx.storage.state.persistent;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.storage.state.inmemory.TxMetaFreeList;
import org.apache.ignite.internal.tx.storage.state.inmemory.TxMetaTree;
import org.apache.ignite.internal.tx.storage.state.inmemory.TxMetaVolatileInMemoryStorage;

public class TxMetaPersistentStorage extends TxMetaVolatileInMemoryStorage {
    private final CheckpointTimeoutLock checkpointLock;

    public TxMetaPersistentStorage(TxMetaTree tree, TxMetaFreeList freeList, int partition, CheckpointTimeoutLock checkpointLock) {
        super(tree, freeList, partition);

        this.checkpointLock = checkpointLock;
    }

    /** {@inheritDoc} */
    @Override public TxMeta get(UUID txId) {
        checkpointLock.checkpointReadLock();

        try {
            return super.get(txId);
        } finally {
            checkpointLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void put(UUID txId, TxMeta txMeta) {
        checkpointLock.checkpointReadLock();

        try {
            super.put(txId, txMeta);
        } finally {
            checkpointLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(UUID txId, TxMeta txMetaExpected, TxMeta txMeta) {
        checkpointLock.checkpointReadLock();

        try {
            return super.compareAndSet(txId, txMetaExpected, txMeta);
        } finally {
            checkpointLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(UUID txId) {
        checkpointLock.checkpointReadLock();

        try {
            super.remove(txId);
        } finally {
            checkpointLock.checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        checkpointLock.checkpointReadLock();

        try {
            super.destroy();
        } finally {
            checkpointLock.checkpointReadUnlock();
        }
    }
}
