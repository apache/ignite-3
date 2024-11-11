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

package org.apache.ignite.internal.tx.test;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.tx.TxPriority;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;

/**
 * Contains methods to generate transaction IDs for the cases when the clock and nodeId might be hard-coded
 * (just for tests).
 */
public class TestTransactionIds {
    /** Global clock instance. */
    private static final HybridClock GLOBAL_CLOCK = new HybridClockImpl();

    /** Hard-coded node ID. */
    private static final int SOLE_NODE_ID = 0xdeadbeef;

    public static final TransactionIdGenerator TRANSACTION_ID_GENERATOR = new TransactionIdGenerator(SOLE_NODE_ID);

    /**
     * Generates new transaction ID with {@link TxPriority#NORMAL} priority using the global clock and hard-coded node ID.
     *
     * @return New transaction ID.
     */
    public static UUID newTransactionId() {
        return newTransactionId(TxPriority.NORMAL);
    }

    /**
     * Generates new transaction ID with the specified priority using the global clock and hard-coded node ID.
     *
     * @param priority Transaction priority.
     * @return New transaction ID.
     */
    public static UUID newTransactionId(TxPriority priority) {
        return TRANSACTION_ID_GENERATOR.transactionIdFor(GLOBAL_CLOCK.now(), priority);
    }
}
