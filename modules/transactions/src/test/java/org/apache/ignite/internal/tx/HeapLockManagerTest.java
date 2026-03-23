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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.impl.HeapLockManager.DEFAULT_SLOTS;
import static org.apache.ignite.internal.tx.impl.HeapLockManager.LOCK_MAP_SIZE_PROPERTY_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.utils.SystemConfigurationPropertyCompatibilityChecker;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link HeapLockManager}.
 */
public class HeapLockManagerTest extends AbstractLockManagerTest {
    @Override
    protected LockManager newInstance(SystemLocalConfiguration systemLocalConfiguration) {
        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();
        HeapLockManager lockManager = new HeapLockManager(systemLocalConfiguration, txStateVolatileStorage);
        lockManager.start(new WaitDieDeadlockPreventionPolicy());
        return lockManager;
    }

    @Override
    protected LockKey lockKey() {
        return new LockKey(0, "test");
    }

    @Test
    public void testLockTableOverflow() throws Exception {
        int maxSlots = 16;

        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();
        HeapLockManager lockManager = new HeapLockManager(maxSlots, txStateVolatileStorage);
        lockManager.start(new WaitDieDeadlockPreventionPolicy());

        UUID[] txs = new UUID[maxSlots];

        for (int i = 0; i < maxSlots; i++) {
            txs[i] = TestTransactionIds.newTransactionId();
            lockManager.acquire(txs[i], new LockKey(txs[i], txs[i]), LockMode.S).get();
        }

        UUID overflowTx = TestTransactionIds.newTransactionId();

        CompletableFuture<Lock> overflowLockFut = lockManager.acquire(overflowTx, new LockKey(overflowTx, overflowTx), LockMode.S);

        assertThat(overflowLockFut, willThrowWithCauseOrSuppressed(
                LockTableOverflowException.class,
                "Failed to acquire a lock due to lock table overflow"
        ));

        for (int i = 0; i < maxSlots; i++) {
            lockManager.releaseAll(txs[i]);
        }

        overflowLockFut = lockManager.acquire(overflowTx, new LockKey(overflowTx, overflowTx), LockMode.S);

        assertThat(overflowLockFut, willCompleteSuccessfully());

        lockManager.releaseAll(overflowTx);

        assertTrue(lockManager.isEmpty());
    }

    @Test
    public void testLockTooManyKeysInTx() throws Exception {
        int maxSlots = 16;

        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();
        HeapLockManager lockManager = new HeapLockManager(maxSlots, txStateVolatileStorage);
        lockManager.start(new WaitDieDeadlockPreventionPolicy());

        UUID txId = TestTransactionIds.newTransactionId();

        for (int i = 0; i < maxSlots; i++) {
            lockManager.acquire(txId, new LockKey(i, i), LockMode.S).get();
        }

        int moreKeys = 2 * maxSlots;

        for (int i = maxSlots; i < moreKeys; i++) {
            CompletableFuture<Lock> overflowLockFut = lockManager.acquire(txId, new LockKey(i, i), LockMode.S);

            assertThat(overflowLockFut, willThrowWithCauseOrSuppressed(
                    LockTableOverflowException.class,
                    "Failed to acquire a lock due to lock table overflow"
            ));
        }

        lockManager.releaseAll(txId);

        assertTrue(lockManager.isEmpty());
    }

    @Test
    public void testDefaultConfiguration() {
        assertThat(((HeapLockManager) lockManager).available(), is(DEFAULT_SLOTS));
        assertThat(((HeapLockManager) lockManager).getSlots(), is(arrayWithSize(0)));
    }

    @Test
    public void testNonDefaultConfiguration(
            @InjectConfiguration("mock.properties: { lockMapSize: \"42\" }")
            SystemLocalConfiguration systemLocalConfiguration
    ) {
        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();

        var lockManager = new HeapLockManager(systemLocalConfiguration, txStateVolatileStorage);

        lockManager.start(DeadlockPreventionPolicy.NO_OP);

        assertThat(lockManager.available(), is(42));
        assertThat(lockManager.getSlots(), is(arrayWithSize(0)));
    }

    @Test
    public void testCompatibilityLockMapSizePropertyNameWasNotChanged() {
        SystemConfigurationPropertyCompatibilityChecker.checkSystemConfigurationPropertyNameWasNotChanged(
                "LOCK_MAP_SIZE_PROPERTY_NAME",
                "lockMapSize",
                LOCK_MAP_SIZE_PROPERTY_NAME
        );
    }
}
