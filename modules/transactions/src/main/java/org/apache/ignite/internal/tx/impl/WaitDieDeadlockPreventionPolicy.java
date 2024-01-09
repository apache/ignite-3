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

/**
 * Implements a deadlock prevention policy that resolves conflicts between two transactions (tx1 and tx2) contending for the same key. When
 * tx1 holds a lock and tx2 attempts to acquire it, the policy allows tx2 to wait for the lock if any of the following conditions are
 * met:
 * <ul>
 *     <li>tx2 is older than tx1.</li>
 *     <li>tx2 is younger than tx1 but has a higher {@link org.apache.ignite.internal.tx.TxPriority}.</li>
 *     <li>The wait timeout is greater than 0.</li>
 * </ul>
 * If none of these conditions are met, tx2 is aborted to prevent deadlock.
 */
public class WaitDieDeadlockPreventionPolicy implements DeadlockPreventionPolicy {
    private static final TxIdPriorityComparator TX_ID_PRIORITY_COMPARATOR = new TxIdPriorityComparator();

    /** {@inheritDoc} */
    @Override
    public Comparator<UUID> txIdComparator() {
        return TX_ID_PRIORITY_COMPARATOR;
    }

    /** {@inheritDoc} */
    @Override
    public long waitTimeout() {
        return 0;
    }
}
