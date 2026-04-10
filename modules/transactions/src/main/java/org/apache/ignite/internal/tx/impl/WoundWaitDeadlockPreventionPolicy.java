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

import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.Waiter;

/**
 * Implements a deadlock prevention policy that resolves conflicts between two transactions (tx1 and tx2) contending for the same key. When
 * tx1 holds a lock and tx2 attempts to acquire it, the policy allows tx2 to wait for the lock if any of the following conditions are
 * met:
 * <ul>
 *     <li>tx2 is younger than tx1.</li>
 *     <li>tx2 is older than tx1 but has a lower {@link org.apache.ignite.internal.tx.TxPriority}.</li>
 * </ul>
 * If none of these conditions are met, tx1 is killed to prevent deadlock.
 */
public class WoundWaitDeadlockPreventionPolicy implements DeadlockPreventionPolicy {
    @Override
    public Waiter allowWait(Waiter waiter, Waiter owner) {
        int res = TransactionIds.compare(waiter.txId(), owner.txId());
        assert res != 0;

        // Waiter is allowed to wait for owner if it has lower priority.
        // Otherwise we have to fail owner.
        return res > 0 ? null : owner;
    }
}
