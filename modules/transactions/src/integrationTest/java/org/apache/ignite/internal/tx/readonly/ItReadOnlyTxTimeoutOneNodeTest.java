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

package org.apache.ignite.internal.tx.readonly;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.tx.impl.ReadOnlyTransactionImpl;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

abstract class ItReadOnlyTxTimeoutOneNodeTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "TEST";

    @Override
    protected int initialNodes() {
        return 1;
    }

    abstract Ignite ignite();

    abstract ReadOnlyTransactionImpl transactionImpl(Transaction tx);

    @Test
    void roTransactionTimesOut() throws Exception {
        Ignite ignite = ignite();

        ignite.sql().executeScript("CREATE TABLE " + TABLE_NAME + " (ID INT PRIMARY KEY, VAL VARCHAR)");

        Table table = ignite.tables().table(TABLE_NAME);

        Transaction roTx = ignite.transactions().begin(new TransactionOptions().readOnly(true).timeoutMillis(100));

        // Make sure the RO tx actually begins on the server (as thin client transactions are lazy).
        doGetOn(table, roTx);

        assertTrue(
                waitForCondition(() -> transactionImpl(roTx).isFinishingOrFinished(), SECONDS.toMillis(10)),
                "Transaction should have been finished due to timeout"
        );

        assertThrows(TransactionException.class, () -> doGetOn(table, roTx));
        // TODO: uncomment the following assert after IGNITE-24233 is fixed.
        // assertThrows(TransactionException.class, roTx::commit);
    }

    private static void doGetOn(Table table, Transaction tx) {
        table.keyValueView(Integer.class, String.class).get(tx, 1);
    }
}
