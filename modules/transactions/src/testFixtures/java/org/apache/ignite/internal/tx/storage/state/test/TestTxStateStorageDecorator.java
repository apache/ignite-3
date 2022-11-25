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

package org.apache.ignite.internal.tx.storage.state.test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Decorator for {@link TestTxStateStorage}.
 */
class TestTxStateStorageDecorator implements TxStateStorage {
    /** Storage from which we will read. */
    private final AtomicReference<TestTxStateStorage> readStorageReference = new AtomicReference<>();

    /** Storage to which we will write. */
    private final AtomicReference<TestTxStateStorage> writeStorageReference = new AtomicReference<>();

    /**
     * Constructor.
     *
     * @param storage Storage from which we will read and to which we will write.
     */
    TestTxStateStorageDecorator(TestTxStateStorage storage) {
        readStorageReference.set(storage);
        writeStorageReference.set(storage);
    }

    @Override
    public @Nullable TxMeta get(UUID txId) {
        return readStorageReference.get().get(txId);
    }

    @Override
    public void put(UUID txId, TxMeta txMeta) {
        writeStorageReference.get().put(txId, txMeta);
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
        return writeStorageReference.get().compareAndSet(txId, txStateExpected, txMeta, commandIndex, commandTerm);
    }

    @Override
    public void remove(UUID txId) {
        writeStorageReference.get().remove(txId);
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        return readStorageReference.get().scan();
    }

    @Override
    public CompletableFuture<Void> flush() {
        return writeStorageReference.get().flush();
    }

    @Override
    public long lastAppliedIndex() {
        return readStorageReference.get().lastAppliedIndex();
    }

    @Override
    public long lastAppliedTerm() {
        return readStorageReference.get().lastAppliedTerm();
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        writeStorageReference.get().lastApplied(lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public long persistedIndex() {
        return readStorageReference.get().persistedIndex();
    }

    @Override
    public void close() {
        readStorageReference.get().close();
        writeStorageReference.get().close();
    }

    @Override
    public void destroy() {
        readStorageReference.get().destroy();
        writeStorageReference.get().destroy();
    }

    /**
     * Starts a full rebalance of transaction state storage, by replacing the write-to storage with the new storage.
     *
     * @param newWriteStorage New write-to transaction state storage.
     * @return Old write-to transaction state storage.
     */
    TestTxStateStorage startRebalance(TestTxStateStorage newWriteStorage) {
        return writeStorageReference.getAndSet(newWriteStorage);
    }

    /**
     * Aborts a full rebalance of transaction state storage, by replacing the write-to storage with the old storage.
     *
     * @param oldWriteStorage Old write-to transaction state storage.
     */
    void abortRebalce(TestTxStateStorage oldWriteStorage) {
        writeStorageReference.set(oldWriteStorage);
    }

    /**
     * Finishes a full rebalance of transaction state storage, by replacing the read-from storage with the write-to storage.
     */
    void finishRebalance() {
        readStorageReference.set(writeStorageReference.get());
    }
}
