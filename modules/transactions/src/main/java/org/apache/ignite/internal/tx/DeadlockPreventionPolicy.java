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
import org.jetbrains.annotations.Nullable;

/**
 * Deadlock prevention policy. Provides comparator for transaction ids, that allows to compare transactions in order to define
 * which one is allowed to wait and which one should be aborted in case of possible deadlock.
 * See also {@link org.apache.ignite.internal.tx.impl.HeapLockManager}.
 */
public interface DeadlockPreventionPolicy {
    /**
     * No-op policy which does nothing to prevent deadlocks.
     */
    DeadlockPreventionPolicy NO_OP = new DeadlockPreventionPolicy() {};

    /**
     * Comparator for transaction ids that allows to set transaction priority, if deadlock prevention policy requires this priority.
     * The transaction with higher id has lower priority. If this comparator is {@code null} then behavior of any transaction
     * in case of conflict depends only on whether this transaction holds a lock or makes a request for lock acquisition.
     *
     * @return Transaction id comparator.
     */
    @Nullable default Comparator<UUID> txIdComparator() {
        return null;
    }

    /**
     * Timeout (in milliseconds) to wait before aborting a lock attempt that is made by a transaction in case of a conflict
     * of this transaction with another one on certain key. If transaction priority is applicable (see {@link #txIdComparator()})
     * then this timeout is applied only for transaction with lower priority. If this method returns {@code 0} this means that
     * the lock attempt is aborted instantly (timeout is zero). If lesser that {@code 0}, it means that the wait time is infinite.
     *
     * @return Timeout, in milliseconds.
     */
    default long waitTimeout() {
        return -1;
    }

    /**
     * Whether transaction priority if used for conflict resolution.
     *
     * @return Whether priority is used.
     */
    default boolean usePriority() {
        return txIdComparator() != null;
    }
}
