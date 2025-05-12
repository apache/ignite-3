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

import static org.apache.ignite.internal.thread.ThreadOperation.TX_STATE_STORAGE_ACCESS;
import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsTo;
import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsToRead;
import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsToWrite;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.worker.ThreadAssertingCursor;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TxStatePartitionStorage} that performs thread assertions when doing read/write operations.
 *
 * @see ThreadAssertions
 */
public class ThreadAssertingTxStatePartitionStorage implements TxStatePartitionStorage {
    private final TxStatePartitionStorage storage;

    /** Constructor. */
    public ThreadAssertingTxStatePartitionStorage(TxStatePartitionStorage storage) {
        this.storage = storage;
    }

    @Override
    public @Nullable TxMeta get(UUID txId) {
        assertThreadAllowsToRead();

        return storage.get(txId);
    }

    @Override
    public void putForRebalance(UUID txId, TxMeta txMeta) {
        assertThreadAllowsToWrite();

        storage.putForRebalance(txId, txMeta);
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
        assertThreadAllowsToWrite();

        return storage.compareAndSet(txId, txStateExpected, txMeta, commandIndex, commandTerm);
    }

    @Override
    public void remove(UUID txId, long commandIndex, long commandTerm) {
        assertThreadAllowsToWrite();

        storage.remove(txId, commandIndex, commandTerm);
    }

    @Override
    public void removeAll(Collection<UUID> txIds, long commandIndex, long commandTerm) {
        assertThreadAllowsToWrite();

        storage.removeAll(txIds, commandIndex, commandTerm);
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        assertThreadAllowsTo(TX_STATE_STORAGE_ACCESS);

        return new ThreadAssertingCursor<>(storage.scan());
    }

    @Override
    public CompletableFuture<Void> flush() {
        assertThreadAllowsToWrite();

        return storage.flush();
    }

    @Override
    public long lastAppliedIndex() {
        return storage.lastAppliedIndex();
    }

    @Override
    public long lastAppliedTerm() {
        return storage.lastAppliedTerm();
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        assertThreadAllowsToWrite();

        storage.lastApplied(lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public void close() {
        storage.close();
    }

    @Override
    public void destroy() {
        assertThreadAllowsToWrite();

        storage.destroy();
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        assertThreadAllowsToWrite();

        return storage.startRebalance();
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        assertThreadAllowsToWrite();

        return storage.abortRebalance();
    }

    @Override
    public CompletableFuture<Void> finishRebalance(MvPartitionMeta partitionMeta) {
        assertThreadAllowsToWrite();

        return storage.finishRebalance(partitionMeta);
    }

    @Override
    public CompletableFuture<Void> clear() {
        assertThreadAllowsToWrite();

        return storage.clear();
    }

    @Override
    public void committedGroupConfiguration(byte[] config, long index, long term) {
        assertThreadAllowsToWrite();

        storage.committedGroupConfiguration(config, index, term);
    }

    @Override
    public byte @Nullable [] committedGroupConfiguration() {
        return storage.committedGroupConfiguration();
    }

    @Override
    public @Nullable LeaseInfo leaseInfo() {
        return storage.leaseInfo();
    }

    @Override
    public void leaseInfo(LeaseInfo leaseInfo, long index, long term) {
        assertThreadAllowsToWrite();

        storage.leaseInfo(leaseInfo, index, term);
    }

    @Override
    public byte @Nullable [] snapshotInfo() {
        return storage.snapshotInfo();
    }

    @Override
    public void snapshotInfo(byte[] snapshotInfo) {
        assertThreadAllowsToWrite();

        storage.snapshotInfo(snapshotInfo);
    }
}
