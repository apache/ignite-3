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
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Decorator for {@link TxStateStorage} on full rebalance.
 *
 * <p>Until the full rebalance is completed, all readings (including the cursor) will be from the old transaction state storage, after the
 * full rebalancing is completed ({@link #finishRebalance()}), all readings and new cursors will be switched to the new
 * transaction state storage. All modifications take place on the new transaction state storage.
 */
public class TxStateStorageOnRebalance implements TxStateStorage {
    private final TxStateStorage oldStorage;

    private final TxStateStorage newStorage;

    private final IgniteSpinBusyLock finishRebalanceBusyLock = new IgniteSpinBusyLock();

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
     * Returns new transaction state storage.
     */
    public TxStateStorage getNewStorage() {
        return newStorage;
    }

    @Override
    @Nullable
    public TxMeta get(UUID txId) {
        if (!finishRebalanceBusyLock.enterBusy()) {
            return newStorage.get(txId);
        }

        try {
            return oldStorage.get(txId);
        } finally {
            finishRebalanceBusyLock.leaveBusy();
        }
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
        if (!finishRebalanceBusyLock.enterBusy()) {
            return newStorage.scan();
        }

        try {
            return oldStorage.scan();
        } finally {
            finishRebalanceBusyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> flush() {
        return newStorage.flush();
    }

    @Override
    public long lastAppliedIndex() {
        if (!finishRebalanceBusyLock.enterBusy()) {
            return newStorage.lastAppliedIndex();
        }

        try {
            return oldStorage.lastAppliedIndex();
        } finally {
            finishRebalanceBusyLock.leaveBusy();
        }
    }

    @Override
    public long lastAppliedTerm() {
        if (!finishRebalanceBusyLock.enterBusy()) {
            return newStorage.lastAppliedTerm();
        }

        try {
            return oldStorage.lastAppliedTerm();
        } finally {
            finishRebalanceBusyLock.leaveBusy();
        }
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        newStorage.lastApplied(lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public long persistedIndex() {
        if (!finishRebalanceBusyLock.enterBusy()) {
            return newStorage.persistedIndex();
        }

        try {
            return oldStorage.persistedIndex();
        } finally {
            finishRebalanceBusyLock.leaveBusy();
        }
    }

    @Override
    public void destroy() {
        oldStorage.destroy();
        newStorage.destroy();
    }

    @Override
    public void close() {
        oldStorage.close();
        newStorage.close();
    }

    /**
     * Switches all reads and new cursors to read from the new transaction state storage.
     */
    public void finishRebalance() {
        finishRebalanceBusyLock.block();
    }
}
