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

import java.util.Comparator;
import java.util.UUID;
import org.apache.ignite.internal.tx.impl.TxIdPriorityComparator;
import org.jetbrains.annotations.Nullable;

/**
 * Deadlock prevention policy. Provides comparator for transaction ids, that allows to compare transactions in order to define
 * which one is allowed to wait and which one should be aborted in case of possible deadlock.
 * See also {@link org.apache.ignite.internal.tx.impl.HeapLockManager}.
 */
public interface DeadlockPreventionPolicy {
    /** Default comparator. */
    TxIdPriorityComparator TX_ID_PRIORITY_COMPARATOR = new TxIdPriorityComparator();

    /**
     * No-op policy which does nothing to prevent deadlocks.
     */
    DeadlockPreventionPolicy NO_OP = new DeadlockPreventionPolicy() {};

    /**
     * A comparator for transaction ids that orders transactions according to their priority. Transactions with higher priority
     * will acquire locks first. Also, the priority is used to prevent deadlocks, if a policy supports deadlock prevention.
     *
     * @return Transaction id comparator.
     */
    default Comparator<UUID> txIdComparator() {
        return TX_ID_PRIORITY_COMPARATOR;
    }

    /**
     * Timeout (in milliseconds) to wait before aborting a lock attempt that is made by a transaction in case of a conflict
     * of this transaction with another one on certain key. If a policy allows deadlock prevention,
     * then this timeout is applied only to a waiting transaction. If this method returns {@code 0} this means that
     * the lock attempt is aborted instantly (timeout is zero). If lesser that {@code 0}, it means that the wait time is infinite.
     * TODO IGNITE-28507 make configurable.
     *
     * @return Timeout, in milliseconds.
     */
    default long waitTimeout() {
        return -1;
    }

    /**
     * Invokes fail action on the owner.
     *
     * @param owner The owner.
     */
    default void failAction(UUID owner) {
        // No-op.
    }

    /**
     * Tests if waiter is allowed to wait for owner.
     *
     * @param waiter The waiter.
     * @param owner The owner.
     *
     * @return Waiter to fail or {@code null} if waiting is allowed.
     */
    default @Nullable Waiter allowWait(Waiter waiter, Waiter owner) {
        return null;
    }

    /**
     * Returns {@code true}, if wait order is inverted: high priority (older) transactions are allowed to wait for low priority (younger)
     * transactions.
     *
     * <p>Must be consistent with {@code allowWait} implementation: for example, if higher priority is allowed to wait for lower priority,
     * a search should start from low priority first, to have a chance of finding a valid waiter-owner pair.
     *
     * @return If {@code true} for inverted wait order.
     */
    default boolean invertedWaitOrder() {
        return false;
    }
}
