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

package org.apache.ignite.internal.tx.storage.state;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Decorator for {@link TxStateStorage} on full rebalance.
 *
 * <p>All readings will be from the old storage, and modifications in the new storage.
 *
 * <p>TODO: IGNITE-18022 не забудь про курсоры
 */
public class TxStateStorageOnRebalance implements TxStateStorage {
    private final TxStateStorage oldStorage;

    private final TxStateStorage newStorage;

    /**
     * Constructor.
     *
     * @param oldStorage Old transaction state storage to read from.
     * @param newStorage New transaction state storage to write into.
     */
    public TxStateStorageOnRebalance(TxStateStorage oldStorage, TxStateStorage newStorage) {
        this.oldStorage = oldStorage;
        this.newStorage = newStorage;
    }

    /**
     * Returns old transaction state storage.
     */
    public TxStateStorage getOldStorage() {
        return oldStorage;
    }

    /**
     * New transaction state storage.
     */
    public TxStateStorage getNewStorage() {
        return newStorage;
    }

    @Override
    public TxMeta get(UUID txId) {
        return oldStorage.get(txId);
    }

    @Override
    public void put(UUID txId, TxMeta txMeta) {
        newStorage.put(txId, txMeta);
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
        return newStorage.compareAndSet(txId, txStateExpected, txMeta, commandIndex, commandTerm);
    }

    @Override
    public void remove(UUID txId) {
        newStorage.remove(txId);
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        return oldStorage.scan();
    }

    @Override
    public CompletableFuture<Void> flush() {
        return newStorage.flush();
    }

    @Override
    public long lastAppliedIndex() {
        return oldStorage.lastAppliedIndex();
    }

    @Override
    public long lastAppliedTerm() {
        return oldStorage.lastAppliedTerm();
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        newStorage.lastApplied(lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public long persistedIndex() {
        return oldStorage.persistedIndex();
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }
}
