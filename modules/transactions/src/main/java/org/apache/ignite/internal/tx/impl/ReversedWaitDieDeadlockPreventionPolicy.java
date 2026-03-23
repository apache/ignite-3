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

import java.util.Comparator;
import java.util.UUID;
import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.Waiter;

/**
 * Reversed wait die implementation. Same as wait die, but reverses the wait order: younger is allowed to wait for older, older is rejected
 * if conflicts with younger.
 */
public class ReversedWaitDieDeadlockPreventionPolicy implements DeadlockPreventionPolicy {
    private static final TxIdPriorityComparator TX_ID_PRIORITY_COMPARATOR = new TxIdPriorityComparator();

    /** {@inheritDoc} */
    @Override
    public final Comparator<UUID> txIdComparator() {
        return TX_ID_PRIORITY_COMPARATOR;
    }

    @Override
    public Waiter allowWait(Waiter waiter, Waiter owner) {
        int res = txIdComparator().compare(waiter.txId(), owner.txId());
        assert res != 0;

        // Waiter is allowed to wait for owner if it's younger.
        // Otherwise we have to fail waiter.
        return res > 0 ? null : waiter;
    }
}
