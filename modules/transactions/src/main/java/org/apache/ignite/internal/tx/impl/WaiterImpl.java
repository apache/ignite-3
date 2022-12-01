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

package org.apache.ignite.internal.tx.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.Waiter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class WaiterImpl implements Waiter {
    private static final short INTENTION_MASK = (short) (1 << (Short.SIZE - 1));
    private final short[] lockCounts = new short[LockMode.values().length];
    private LockMode lockedSupremum;
    private LockMode intentionSupremum;

    /** Locked future. */
    @IgniteToStringExclude
    private CompletableFuture<Void> fut;

    /** Waiter transaction id. */
    private final UUID txId;

    /**
     * The filed has a value when the waiter couldn't lock a key.
     */
    private LockException ex;

    /**
     * The constructor.
     *
     * @param txId Transaction id.
     */
    public WaiterImpl(UUID txId) {
        this.txId = txId;
    }

    private static short addLockCounter(short counter, short value) {
        return (short) ((counter & (~INTENTION_MASK) + value) | (counter & INTENTION_MASK));
    }

    private static short getLockCount(short counter) {
        return (short) (counter & (~INTENTION_MASK));
    }

    private static short setIntention(short counter, boolean intention) {
        return (short) (intention ? counter | INTENTION_MASK : counter & (~INTENTION_MASK));
    }

    private static boolean isIntention(short counter) {
        return (counter & INTENTION_MASK) == INTENTION_MASK;
    }

    private static LockMode supremumNullAware(@Nullable LockMode lockMode1, LockMode lockMode2) {
        return lockMode1 == null ? lockMode2 : LockMode.supremum(lockMode1, lockMode2);
    }

    /**
     * Adds a lock mode.
     *
     * @param lockMode Lock mode.
     * @param intention Lock intention flag.
     */
    @Override
    public void addLock(LockMode lockMode, boolean intention) {
        int m = lockMode.ordinal();

        final short count = lockCounts[m];

        short actualCount = getLockCount(count);

        if (actualCount > 0) {
            assert isIntention(count) == intention : "Switching the lock intention is impossible within this operation, intention="
                    + intention + ", current=" + isIntention(count) + ", lockMode=" + lockMode + ", txId=" + txId;

            assert getLockCount(count) < Short.MAX_VALUE : "Too many locks acquired by one transaction on one key in the same mode, "
                    + ", lockMode=" + lockMode + ", txId=" + txId;

            lockCounts[m] = addLockCounter(count, (short) 1);
        } else {
            assert actualCount == 0;

            if (!hasLocks(true)) {
                fut = new CompletableFuture<>();
            }

            lockCounts[m] = setIntention((short) 1, intention);

            recalculateSuprema();
        }
    }

    /**
     * Removes a lock mode.
     *
     * @param lockMode Lock mode.
     */
    @Override
    public void removeLock(LockMode lockMode) {
        int m = lockMode.ordinal();

        final short count = lockCounts[m];
        final short actualCount = getLockCount(count);

        assert actualCount > 0 : "Trying to remove lock which does not exist, lockMode=" + lockMode + ", txId=" + txId;
        assert !isIntention(count) : "Trying to remove lock intention, lockMode=" + lockMode + ", txId=" + txId;

        if (actualCount == 1) {
            lockCounts[m] = 0;

            recalculateSuprema();
        } else {
            lockCounts[m] = addLockCounter(count, (short) -1);
        }
    }

    /**
     * Recalculates lock mode based of all locks which the waiter has took.
     *
     * @return Recalculated lock mode.
     */
    private void recalculateSuprema() {
        lockedSupremum = null;
        intentionSupremum = null;

        for (int i = 0; i < lockCounts.length; i++) {
            short c = lockCounts[i];

            short actualCount = getLockCount(lockCounts[i]);

            lockedSupremum = !isIntention(lockCounts[i]) && actualCount > 0
                    ? supremumNullAware(lockedSupremum, LockMode.values()[i]) : lockedSupremum;

            intentionSupremum = actualCount > 0 ? supremumNullAware(intentionSupremum, LockMode.values()[i]) : intentionSupremum;
        }
    }

    /** Notifies a future listeners. */
    @Override
    public void lock() {
        if (ex != null) {
            completeIntentions(false);

            fut.completeExceptionally(ex);
        } else {
            int completed = completeIntentions(true);
            assert completed > 0;

            fut.complete(null);
        }

        ex = null;
    }

    private int completeIntentions(boolean lock) {
        int completed = 0;

        for (int i = 0; i < lockCounts.length; i++) {
            short c = lockCounts[i];

            if (isIntention(c) && getLockCount(c) > 0) {
                if (lock) {
                    lockCounts[i] = setIntention(c, false);
                } else {
                    lockCounts[i] = 0;
                }

                completed++;
            }
        }

        recalculateSuprema();

        return completed;
    }

    private boolean hasLocks(boolean intention) {
        for (short c : lockCounts) {
            if (isIntention(c) == intention && getLockCount(c) > 0) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean locked() {
        return lockedSupremum != null;
    }

    public boolean waiting() {
        return intentionSupremum != null && intentionSupremum != lockedSupremum;
    }

    @Override
    public LockMode lockMode() {
        return lockedSupremum;
    }

    @Override
    public LockMode intentionLockMode() {
        return intentionSupremum;
    }

    /**
     * Fails the lock waiter.
     *
     * @param e Lock exception.
     */
    @Override
    public void fail(LockException e) {
        assert fut != null;

        ex = e;
    }

    @Override
    public CompletableFuture<Void> fut() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override
    public UUID txId() {
        return txId;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(@NotNull Waiter o) {
        return txId.compareTo(o.txId());
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WaiterImpl)) {
            return false;
        }

        return compareTo((WaiterImpl) o) == 0;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return txId.hashCode();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(WaiterImpl.class, this, "isDone", fut.isDone());
    }
}
