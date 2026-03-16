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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class SortedIndexLockerTest extends BaseIgniteAbstractTest {
    private static final int PARTITION_ID = 0;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    private final HybridClock clock = new HybridClockImpl();

    private final BinaryTuple binaryTuple = new BinaryTuple(1, new BinaryTupleBuilder(1).appendInt(42).build());

    @Test
    void takingInsertLocksOnDestroyedIndexStorageYieldsNullLock() {
        TestSortedIndexStorage indexStorage = new TestSortedIndexStorage(
                PARTITION_ID,
                new StorageSortedIndexDescriptor(
                        1,
                        List.of(new StorageSortedIndexColumnDescriptor("c1", NativeTypes.INT32, false, true, false)),
                        false
                )
        );
        indexStorage.destroy();

        SortedIndexLocker locker = new SortedIndexLocker(1, PARTITION_ID, lockManager(), indexStorage, row -> binaryTuple, false);

        UUID txId = TestTransactionIds.TRANSACTION_ID_GENERATOR.transactionIdFor(clock.now());
        CompletableFuture<@Nullable Lock> lockFuture = locker.locksForInsert(txId, mock(BinaryRow.class), new RowId(PARTITION_ID));

        assertThat(lockFuture, willBe(nullValue()));
    }

    @Test
    void insertRetriesWhenSuccessorChangesAfterSuccessorLock() {
        TestSortedIndexStorage indexStorage = indexStorage();
        indexStorage.put(indexRow(40, 40));

        HookedLockManager lockManager = lockManager();
        lockManager.doOnceOnAcquire(
                lock -> lock.lockKey().equals(lockKey(40)) && lock.lockMode() == LockMode.IX,
                () -> indexStorage.put(indexRow(30, 30))
        );

        SortedIndexLocker locker = new SortedIndexLocker(1, PARTITION_ID, lockManager, indexStorage, row -> tuple(20), false);

        UUID txId = TestTransactionIds.newTransactionId();

        CompletableFuture<@Nullable Lock> lockFuture = locker.locksForInsert(txId, mock(BinaryRow.class), new RowId(PARTITION_ID));

        assertThat(lockFuture, willCompleteSuccessfully());
        Lock shortTermLock = lockFuture.join();

        assertEquals(lockKey(30), shortTermLock.lockKey());
        assertThat(lockKeys(txId, lockManager), containsInAnyOrder(lockKey(20), lockKey(30)));
    }

    @Test
    void insertRetriesAndReleasesCurrentKeyLockWhenSuccessorChangesAfterCurrentKeyLock() {
        TestSortedIndexStorage indexStorage = indexStorage();
        indexStorage.put(indexRow(40, 40));

        HookedLockManager lockManager = lockManager();
        lockManager.doOnceOnAcquire(
                lock -> lock.lockKey().equals(lockKey(20)) && lock.lockMode() == LockMode.IX,
                () -> indexStorage.put(indexRow(30, 30))
        );

        SortedIndexLocker locker = new SortedIndexLocker(1, PARTITION_ID, lockManager, indexStorage, row -> tuple(20), false);

        UUID txId = TestTransactionIds.newTransactionId();

        CompletableFuture<@Nullable Lock> lockFuture = locker.locksForInsert(txId, mock(BinaryRow.class), new RowId(PARTITION_ID));

        assertThat(lockFuture, willCompleteSuccessfully());
        Lock shortTermLock = lockFuture.join();

        assertEquals(lockKey(30), shortTermLock.lockKey());
        assertThat(lockKeys(txId, lockManager), containsInAnyOrder(lockKey(20), lockKey(30)));
    }

    private TestSortedIndexStorage indexStorage() {
        return new TestSortedIndexStorage(
                PARTITION_ID,
                new StorageSortedIndexDescriptor(
                        1,
                        List.of(new StorageSortedIndexColumnDescriptor("c1", NativeTypes.INT32, false, true, false)),
                        false
                )
        );
    }

    private HookedLockManager lockManager() {
        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();
        HookedLockManager lockManager = new HookedLockManager(systemLocalConfiguration, txStateVolatileStorage);
        lockManager.start(new WaitDieDeadlockPreventionPolicy());
        return lockManager;
    }

    private static BinaryTuple tuple(int value) {
        return new BinaryTuple(1, new BinaryTupleBuilder(1).appendInt(value).build());
    }

    private static IndexRow indexRow(int indexValue, long rowIdLsb) {
        return new IndexRowImpl(tuple(indexValue), new RowId(PARTITION_ID, 0, rowIdLsb));
    }

    private static LockKey lockKey(int indexValue) {
        return new LockKey(1, tuple(indexValue).byteBuffer());
    }

    private static List<LockKey> lockKeys(UUID txId, LockManager lockManager) {
        Iterator<Lock> locks = lockManager.locks(txId);
        var result = new ArrayList<LockKey>();

        while (locks.hasNext()) {
            result.add(locks.next().lockKey());
        }

        return result;
    }

    private static class HookedLockManager extends HeapLockManager {
        private final List<AcquireHook> hooks = new ArrayList<>();

        private HookedLockManager(SystemLocalConfiguration systemProperties, VolatileTxStateMetaStorage txStateVolatileStorage) {
            super(systemProperties, txStateVolatileStorage);
        }

        void doOnceOnAcquire(Predicate<Lock> predicate, Runnable action) {
            hooks.add(new AcquireHook(predicate, action));
        }

        @Override
        public CompletableFuture<Lock> acquire(UUID txId, LockKey lockKey, LockMode lockMode) {
            Lock requestedLock = new Lock(lockKey, lockMode, txId);

            for (AcquireHook hook : hooks) {
                hook.runIfMatches(requestedLock);
            }

            return super.acquire(txId, lockKey, lockMode);
        }

        private static class AcquireHook {
            private final Predicate<Lock> predicate;

            private final Runnable action;

            private boolean consumed;

            private AcquireHook(Predicate<Lock> predicate, Runnable action) {
                this.predicate = predicate;
                this.action = action;
            }

            private void runIfMatches(Lock lock) {
                if (consumed || !predicate.test(lock)) {
                    return;
                }

                consumed = true;
                action.run();
            }
        }
    }
}
