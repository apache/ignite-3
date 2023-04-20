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

package org.apache.ignite.internal.sql.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Checks basic operations on a single column table.
 */
public class ItSingleColumnTableCrossApiTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test";

    private static final String[] ENGINES = {"aipersist", "aimem", "rocksdb"};

    @Override
    protected int nodes() {
        return 1;
    }

    @AfterEach
    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        for (String engine : ENGINES) {
            sql("delete from " + tableName(engine));
        }

        tearDownBase(testInfo);
    }

    @BeforeAll
    public void beforeAll() {
        for (String engine : ENGINES) {
            String testZoneName = ("test_zone_" + engine).toUpperCase();

            sql(String.format("create zone %s engine %s with partitions=1, replicas=3;", testZoneName, engine));
            sql(String.format("create table %s (ID int primary key) with primary_zone='%s'", tableName(engine), testZoneName));
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testRecordView(TestEnvironment env) {
        Table tab = env.table();
        RecordView<Tuple> view = tab.recordView();

        // Test record view.
        Tuple tuple = Tuple.create().set("id", 0);

        env.runInTransaction(
                rwTx -> {
                    view.upsert(rwTx, tuple);

                    // Try to replace.
                    view.upsert(rwTx, tuple);
                },
                tx -> {
                    assertEquals(tuple, view.get(tx, tuple));

                    // Ensure there are no duplicates.
                    assertEquals(1, view.getAll(tx, Collections.singleton(tuple)).size());

                    if (!tx.isReadOnly()) {
                        view.delete(tx, tuple);

                        assertNull(view.get(tx, tuple));
                    }
                }
        );
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testKeyValueView(TestEnvironment env) {
        Table tab = env.table();

        KeyValueView<Integer, Void> kvView = tab.keyValueView(Integer.class, Void.class);

        int key = 0;

        env.runInTransaction(
                rwTx -> {
                    kvView.put(rwTx, key, null);

                    // Try to replace.
                    kvView.put(rwTx, key, null);
                },
                tx -> {
                    assertNull(kvView.get(tx, key));

                    assertNotNull(kvView.getNullable(tx, key));
                    assertTrue(kvView.contains(tx, key));
                    assertEquals(1, kvView.getAll(tx, Collections.singleton(0)).size());

                    if (!tx.isReadOnly()) {
                        kvView.remove(tx, key);

                        assertNull(kvView.getNullable(tx, key));
                        assertFalse(kvView.contains(tx, key));
                    }
                }
        );
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testBinaryView(TestEnvironment env) {
        KeyValueView<Tuple, Tuple> binView = env.table().keyValueView();
        Tuple key = Tuple.create().set("id", 0);

        env.runInTransaction(
                rwTx -> {
                    binView.put(rwTx, key, null);

                    // Try to replace.
                    binView.put(rwTx, key, null);
                },
                tx -> {
                    assertNull(binView.get(tx, key));
                    assertTrue(binView.contains(tx, key));

                    if (!tx.isReadOnly()) {
                        binView.remove(tx, key);

                        assertFalse(binView.contains(tx, key));
                    }
                }
        );
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testSql(TestEnvironment env) {
        String tableName = env.table().name();

        env.runInTransaction(
                rwTx -> sql(rwTx, "insert into " + tableName + " values (0), (1)"),
                tx -> assertQuery(tx, "select count(*) from " + tableName).returns(2)
        );
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testMixed(TestEnvironment env) {
        Table tab = env.table();

        RecordView<Tuple> recordView = tab.recordView();
        KeyValueView<Integer, Void> kvView = tab.keyValueView(Integer.class, Void.class);

        env.runInTransaction(
                rwTx -> {
                    recordView.upsert(rwTx, Tuple.create().set("id", 0));

                    SqlException ex = assertThrows(SqlException.class, () -> sql(rwTx, "insert into " + tab.name() + " values (0)"));
                    assertEquals(Sql.DUPLICATE_KEYS_ERR, ex.code());

                    kvView.put(rwTx, 1, null);

                    ex = assertThrows(SqlException.class, () -> sql(rwTx, "insert into " + tab.name() + " values (1)"));
                    assertEquals(Sql.DUPLICATE_KEYS_ERR, ex.code());

                    sql(rwTx, "insert into " + tab.name() + " values (2)");
                },
                tx -> {
                    for (int i = 0; i < 3; i++) {
                        Tuple exp = Tuple.create().set("id", i);

                        assertEquals(exp, recordView.get(tx, exp));

                        assertTrue(kvView.contains(tx, i));
                        assertNotNull(kvView.getNullable(tx, 0));

                        assertQuery("select * from " + tab.name() + " where id=" + i).returns(i);
                    }

                    assertQuery("select count(*) from " + tab.name()).returns(3);
                }
        );
    }

    private static List<TestEnvironment> parameters() {
        List<TestEnvironment> params = new ArrayList<>(ENGINES.length * 2);

        boolean[] modes = {false, true};

        for (String engine : ENGINES) {
            for (boolean readonly : modes) {
                params.add(new TestEnvironment(engine, readonly));
            }
        }

        return params;
    }

    private static String tableName(String engineName) {
        return TABLE_NAME + "_" + engineName;
    }

    private static class TestEnvironment {
        private final String engine;
        private final boolean readOnly;

        private TestEnvironment(String engine, boolean readonly)  {
            this.engine = engine;
            this.readOnly = readonly;
        }

        private Table table() {
            return CLUSTER_NODES.get(0).tables().table(tableName(engine));
        }

        private void runInTransaction(Consumer<Transaction> writeOp, Consumer<Transaction> readOp) {
            IgniteTransactions transactions = CLUSTER_NODES.get(0).transactions();

            if (readOnly) {
                // Start a separate transaction for the write operation.
                transactions.runInTransaction(writeOp);
                transactions.runInTransaction(readOp, new TransactionOptions().readOnly(true));
            } else {
                // Run both operations inside the same read-write transaction.
                transactions.runInTransaction(tx -> {
                    writeOp.accept(tx);
                    readOp.accept(tx);
                });
            }
        }

        @Override
        public String toString() {
            return "engine=" + engine + ", tx=" + (readOnly ? "readonly" : "readwrite");
        }
    }
}
