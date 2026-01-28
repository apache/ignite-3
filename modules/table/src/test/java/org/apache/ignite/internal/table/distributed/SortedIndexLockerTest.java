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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockManager;
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

    private LockManager lockManager() {
        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();
        HeapLockManager lockManager = new HeapLockManager(systemLocalConfiguration, txStateVolatileStorage);
        lockManager.start(new WaitDieDeadlockPreventionPolicy());
        return lockManager;
    }
}
