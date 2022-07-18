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
package org.apache.ignite.internal.tx.storage.state.inmemory;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.storage.state.TxMetaStorage;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.tx.storage.state.inmemory.TxMetaRowWrapper.unwrap;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_DESTROY_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;

public class TxMetaVolatileInMemoryStorage implements TxMetaStorage {
    protected final TxMetaTree tree;

    public TxMetaVolatileInMemoryStorage(TxMetaTree tree) {
        this.tree = tree;
    }

    @Override public void start() {
        // No-op.
    }

    @Override public boolean isStarted() {
        return true;
    }

    @Override public void stop() throws Exception {
        // No-op.
    }

    @Override public TxMeta get(UUID txId) {
        try {
            return unwrap(tree.findOne(new TxMetaRowWrapper(txId, null))).get2();
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    @Override public void put(UUID txId, TxMeta txMeta) {
        try {
            tree.put(new TxMetaRowWrapper(txId, txMeta));
        }
        catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    @Override public boolean compareAndSet(UUID txId, TxMeta txMetaExpected, TxMeta txMeta) {
        try {
            CASClosure closure = new CASClosure(new TxMetaRowWrapper(txId, txMetaExpected), new TxMetaRowWrapper(txId, txMeta));

            tree.invoke(new TxMetaRowWrapper(txId, null), null, closure);

            return closure.casResult();
        }
        catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    @Override public void remove(UUID txId) {
        try {
            tree.remove(new TxMetaRowWrapper(txId, null));
        }
        catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    @Override public void destroy() {
        try {
            tree.destroy();
        }
        catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_DESTROY_ERR, e);
        }
    }

    @Override public CompletableFuture<Void> snapshot(Path snapshotPath) {
        throw new UnsupportedOperationException("Snapshots are not supported yet.");
    }

    @Override public void restoreSnapshot(Path snapshotPath) {
        throw new UnsupportedOperationException("Snapshots are not supported yet.");
    }

    @Override public void close() throws Exception {
        tree.close();
    }

    private static class CASClosure implements InvokeClosure<TxMetaRowWrapper> {
        private final TxMetaRowWrapper expected;
        private final TxMetaRowWrapper newRow;
        private OperationType opType = OperationType.NOOP;

        public CASClosure(TxMetaRowWrapper expected, TxMetaRowWrapper newRow) {
            this.expected = expected;
            this.newRow = newRow;
        }

        @Override public void call(@Nullable TxMetaRowWrapper oldRow) throws IgniteInternalCheckedException {
            opType = expected.equals(oldRow) ? OperationType.PUT : OperationType.NOOP;
        }

        @Override public @Nullable TxMetaRowWrapper newRow() {
            return newRow;
        }

        @Override public OperationType operationType() {
            return opType;
        }

        public boolean casResult() {
            return opType != OperationType.NOOP;
        }
    }
}
