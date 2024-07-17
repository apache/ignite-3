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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxPriority;
import org.junit.jupiter.api.Test;

class TxIdPriorityComparatorTest {
    private final TxIdPriorityComparator comparator = new TxIdPriorityComparator();

    private final HybridClock clock = new HybridClockImpl();

    @Test
    public void compareEqualPriorities() {
        var tx1 = TransactionIds.transactionId(clock.now(), 1, TxPriority.NORMAL);
        var tx2 = TransactionIds.transactionId(clock.now(), 1, TxPriority.NORMAL);

        assertTrue(comparator.compare(tx1, tx2) < 0);
        assertTrue(comparator.compare(tx2, tx1) > 0);
        assertEquals(0, comparator.compare(tx1, tx1));
    }

    @Test
    public void compareOldNormalTxVsNewLowTx() {
        var tx1 = TransactionIds.transactionId(clock.now(), 1, TxPriority.NORMAL);
        var tx2 = TransactionIds.transactionId(clock.now(), 1, TxPriority.LOW);

        assertTrue(comparator.compare(tx1, tx2) < 0);
        assertTrue(comparator.compare(tx2, tx1) > 0);
    }

    @Test
    public void compareOldLowTxVsNewNormalTx() {
        var tx1 = TransactionIds.transactionId(clock.now(), 1, TxPriority.LOW);
        var tx2 = TransactionIds.transactionId(clock.now(), 1, TxPriority.NORMAL);

        assertTrue(comparator.compare(tx1, tx2) > 0);
        assertTrue(comparator.compare(tx2, tx1) < 0);
    }
}
