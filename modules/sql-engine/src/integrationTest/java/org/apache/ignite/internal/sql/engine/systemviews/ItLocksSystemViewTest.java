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

package org.apache.ignite.internal.sql.engine.systemviews;

import static org.apache.ignite.internal.TestWrappers.unwrapInternalTransaction;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_VARLEN_LENGTH;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify {@code LOCKS} system view.
 */
public class ItLocksSystemViewTest extends AbstractSystemViewTest {
    private Set<String> nodeNames;

    private Set<String> lockModes;

    @Override
    protected int initialNodes() {
        return 2;
    }

    @BeforeAll
    void beforeAll() {
        nodeNames = CLUSTER.runningNodes()
                .map(Ignite::name)
                .collect(Collectors.toSet());

        lockModes = Arrays.stream(LockMode.values())
                .map(Enum::name)
                .collect(Collectors.toSet());
    }

    @Test
    public void testMetadata() {
        assertQuery("SELECT * FROM SYSTEM.LOCKS")
                .columnMetadata(
                        new MetadataMatcher().name("OWNING_NODE_ID").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH)
                                .nullable(false),
                        new MetadataMatcher().name("TRANSACTION_ID").type(ColumnType.STRING).precision(36).nullable(true),
                        new MetadataMatcher().name("OBJECT_ID").type(ColumnType.STRING).precision(DEFAULT_VARLEN_LENGTH).nullable(true),
                        new MetadataMatcher().name("LOCK_MODE").type(ColumnType.STRING).precision(2).nullable(true),

                        // Legacy columns.
                        new MetadataMatcher().name("TX_ID").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("MODE").type(ColumnType.STRING).nullable(true)
                )
                // RO tx doesn't take locks.
                .returnNothing()
                .check();
    }

    @Test
    public void testData() {
        Ignite node = CLUSTER.aliveNode();

        sql("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        String[] operations = {
                "INSERT INTO test VALUES (1, 1)",
                "UPDATE test SET val = 1 WHERE id = 0",
                "DELETE FROM test WHERE id = 2"
        };

        for (String op : operations) {
            sql("DELETE FROM test");
            sql("INSERT INTO test VALUES (0, 0), (2, 2)");

            InternalTransaction tx = (InternalTransaction) node.transactions().begin();

            try {
                sql(tx, op);

                List<List<Object>> rows = sql("SELECT * FROM SYSTEM.LOCKS WHERE TX_ID=?", tx.id().toString());

                assertThat(rows, is(not(empty())));

                for (List<Object> row : rows) {
                    verifyLockInfo(row, tx.id().toString());
                }
            } finally {
                tx.rollback();
            }
        }
    }

    @Test
    void testLocksViewWorksCorrectlyWhenTxConflict() {
        Ignite ignite = CLUSTER.aliveNode();

        ignite.sql().executeScript("CREATE TABLE testTable (accountNumber INT PRIMARY KEY, balance DOUBLE)");

        Table test = ignite.tables().table("testTable");

        test.recordView().upsert(null, makeValue(1, 100.0));

        IgniteTransactions igniteTransactions = igniteTx();

        InternalTransaction tx1 = unwrapInternalTransaction(igniteTransactions.begin());
        InternalTransaction tx2 = unwrapInternalTransaction(igniteTransactions.begin());

        var table = test.recordView();

        table.upsert(tx2, makeValue(1, 1.0));

        var fut = table.upsertAsync(tx1, makeValue(1, 2.0));

        assertFalse(fut.isDone());

        List<List<Object>> rows = sql("SELECT * FROM SYSTEM.LOCKS");

        // pk lock, row lock, partition lock
        assertThat(rows.size(), is(3));

        verifyTxIdAndLockMode(rows, tx2.id().toString(), LockMode.X.name());
        verifyTxIdAndLockMode(rows, tx2.id().toString(), LockMode.IX.name());

        tx2.commit();

        rows = sql("SELECT * FROM SYSTEM.LOCKS");

        assertThat(rows.size(), is(3));

        verifyTxIdAndLockMode(rows, tx1.id().toString(), LockMode.X.name());
        verifyTxIdAndLockMode(rows, tx1.id().toString(), LockMode.IX.name());

        tx1.commit();
    }

    /**
     * Makes a tuple containing key and value.
     *
     * @param id The id.
     * @param balance The balance.
     * @return The value tuple.
     */
    private static Tuple makeValue(int id, double balance) {
        return Tuple.create().set("accountNumber", id).set("balance", balance);
    }

    private void verifyLockInfo(List<Object> row, String expectedTxId) {
        int idx = 0;

        String owningNode = (String) row.get(idx++);
        String txId = (String) row.get(idx++);
        String objectId = (String) row.get(idx++);
        String mode = (String) row.get(idx);

        assertThat(nodeNames, hasItem(owningNode));
        assertThat(txId, equalTo(expectedTxId));
        assertThat(objectId, not(emptyString()));
        assertThat(lockModes, hasItem(mode));
    }

    private static void verifyTxIdAndLockMode(List<List<Object>> rows, String expectedTxId, String expectedMode) {
        boolean assertResult = false;

        for (List<Object> row : rows) {
            String txId = getTxId(row);
            String mode = getLockMode(row);

            if (txId.equals(expectedTxId)) {
                if (mode.equals(expectedMode)) {
                    assertResult = true;
                    break;
                }
            }
        }

        assertTrue(assertResult);
    }

    private static String getTxId(List<Object> row) {
        return (String) row.get(1);
    }

    private static String getLockMode(List<Object> row) {
        return (String) row.get(3);
    }
}
