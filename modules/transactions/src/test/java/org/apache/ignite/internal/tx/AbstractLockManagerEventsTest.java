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

import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.tx.event.LockEvent;
import org.apache.ignite.internal.tx.event.LockEventParameters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class that contains the tests for lock manager events producing.
 */
public abstract class AbstractLockManagerEventsTest extends AbstractLockingTest {
    private final AtomicReference<LockEventParameters> eventParamsRef = new AtomicReference<>();

    private final EventListener<LockEventParameters> lockEventListener = this::lockEventListener;

    private final LockKey key = key(new Object());

    private boolean listenerAdded;

    @BeforeEach
    public void reset() {
        eventParamsRef.set(null);

        if (!listenerAdded) {
            lockManager.listen(LockEvent.LOCK_CONFLICT, lockEventListener);

            listenerAdded = true;
        }
    }

    private CompletableFuture<Boolean> lockEventListener(LockEventParameters params) {
        eventParamsRef.set(params);

        return falseCompletedFuture();
    }

    private void checkLockConflictEvent(UUID lockAcquirerTx, UUID lockHolderTx) {
        assertNotNull(eventParamsRef.get());
        assertEquals(lockAcquirerTx, eventParamsRef.get().lockAcquirerTx());
        assertTrue(eventParamsRef.get().lockHolderTxs().contains(lockHolderTx));
    }

    private void checkNoEvent() {
        assertNull(eventParamsRef.get());
    }

    @Test
    public void simpleConflictTest() {
        UUID tx0 = beginTx();
        UUID tx1 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");

        LockKey key = key(new Object());

        xlock(tx0, key);
        xlock(tx1, key);

        checkLockConflictEvent(tx1, tx0);
    }

    @Test
    public void simpleConflictReverseOrder() {
        UUID tx0 = beginTx();
        UUID tx1 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");

        xlock(tx1, key);
        xlock(tx0, key);

        checkLockConflictEvent(tx0, tx1);
    }

    @Test
    public void conflictWithMultipleTxDirectOrder() {
        UUID tx0 = beginTx();
        UUID tx1 = beginTx();
        UUID tx2 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");
        addTxLabel(tx1, "tx2");

        slock(tx0, key);
        slock(tx1, key);
        checkNoEvent();

        xlock(tx2, key);

        checkLockConflictEvent(tx2, tx0);
    }

    @Test
    public void conflictWithMultipleTxReverseOrder() {
        UUID tx0 = beginTx();
        UUID tx1 = beginTx();
        UUID tx2 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");
        addTxLabel(tx1, "tx2");

        slock(tx1, key);
        slock(tx0, key);
        checkNoEvent();

        xlock(tx2, key);

        checkLockConflictEvent(tx2, tx0);
    }

    @Test
    public void reenterWithConflict() {
        UUID tx0 = beginTx();
        UUID tx1 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");

        slock(tx0, key);
        slock(tx1, key);
        checkNoEvent();

        xlock(tx0, key);

        checkLockConflictEvent(tx0, tx1);
    }

    @Test
    public void reenterNoEvent() {
        UUID tx0 = beginTx();

        addTxLabel(tx0, "tx0");

        slock(tx0, key);
        xlock(tx0, key);
        checkNoEvent();
    }

    @Test
    public void multipleCompatibleLockConflictWithIncompatible() {
        UUID tx0 = beginTx();
        UUID tx1 = beginTx();
        UUID tx2 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");
        addTxLabel(tx1, "tx2");

        xlock(tx0, key);

        slock(tx1, key);
        checkLockConflictEvent(tx1, tx0);

        slock(tx2, key);
        checkLockConflictEvent(tx2, tx0);
    }

    @Test
    public void noConflict() {
        UUID tx0 = beginTx();
        UUID tx1 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");

        slock(tx0, key);
        slock(tx1, key);
        checkNoEvent();
    }

    @Test
    public void conflictOnRelease() {
        UUID tx0 = beginTx();
        UUID tx1 = beginTx();
        UUID tx2 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");
        addTxLabel(tx1, "tx2");

        slock(tx1, key);
        slock(tx2, key);
        checkNoEvent();

        xlock(tx0, key);
        checkLockConflictEvent(tx0, tx1);

        commitTx(tx1);

        checkLockConflictEvent(tx0, tx2);
    }

    @Test
    public void conflictWithWinnerOnRelease() {
        UUID tx0 = beginTx();
        UUID tx1 = beginTx();
        UUID tx2 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");
        addTxLabel(tx1, "tx2");

        xlock(tx2, key);

        xlock(tx1, key);
        checkLockConflictEvent(tx1, tx2);

        xlock(tx0, key);
        checkLockConflictEvent(tx0, tx2);

        commitTx(tx2);

        checkLockConflictEvent(tx1, tx0);
    }

    @Test
    public void noConflictOnRelease() {
        UUID tx0 = beginTx();
        UUID tx1 = beginTx();
        UUID tx2 = beginTx();

        addTxLabel(tx0, "tx0");
        addTxLabel(tx1, "tx1");
        addTxLabel(tx1, "tx2");

        xlock(tx0, key);
        slock(tx1, key);
        slock(tx2, key);

        reset();

        commitTx(tx0);

        checkNoEvent();
    }
}
