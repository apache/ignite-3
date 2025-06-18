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

package org.apache.ignite.internal.table.distributed.storage;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageException;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TxStatePartitionStorage} implementation that throws exceptions when attempted to be used.
 * It is intended to make sure that with enabled colocation, table-scoped tx state storages are not used.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 - remove this.
public class BrokenTxStatePartitionStorage implements TxStatePartitionStorage {
    @Override
    public @Nullable TxMeta get(UUID txId) {
        return throwException();
    }

    @Override
    public void putForRebalance(UUID txId, TxMeta txMeta) {
        throwException();
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
        return throwException();
    }

    @Override
    public void remove(UUID txId, long commandIndex, long commandTerm) {
        throwException();
    }

    @Override
    public void removeAll(Collection<UUID> txIds, long commandIndex, long commandTerm) {
        throwException();
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        return throwException();
    }

    @Override
    public CompletableFuture<Void> flush() {
        return throwException();
    }

    @Override
    public long lastAppliedIndex() {
        return throwException();
    }

    @Override
    public long lastAppliedTerm() {
        return throwException();
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        throwException();
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    public void destroy() {
        // No-op.
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        return throwException();
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        return throwException();
    }

    @Override
    public CompletableFuture<Void> finishRebalance(MvPartitionMeta partitionMeta) {
        return throwException();
    }

    @Override
    public CompletableFuture<Void> clear() {
        return throwException();
    }

    @Override
    public void committedGroupConfiguration(byte[] config, long index, long term) {
        throwException();
    }

    @Override
    public byte @Nullable [] committedGroupConfiguration() {
        return throwException();
    }

    @Override
    public void leaseInfo(LeaseInfo leaseInfo, long index, long term) {
        throwException();
    }

    @Override
    public @Nullable LeaseInfo leaseInfo() {
        return throwException();
    }

    @Override
    public void snapshotInfo(byte[] snapshotInfo) {
        throwException();
    }

    @Override
    public byte @Nullable [] snapshotInfo() {
        return throwException();
    }

    private static <T> T throwException() {
        throw new TxStateStorageException("Running in colocation mode, tables should not access TX state storages");
    }
}
