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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Sql.CONSTRAINT_VIOLATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
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
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.lang.UnexpectedNullValueException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests basic operations using a different API
 * on a table where all columns belong to the primary key.
 */
public class ItPkOnlyTableCrossApiTest extends BaseSqlIntegrationTest {
    /** Storage engine types. */
    private static final String[] ENGINES = {"aipersist", "aimem", "rocksdb"};

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    public void tearDown() {
        for (String engine : ENGINES) {
            sql("delete from " + tableName(engine));
        }
    }

    @BeforeAll
    public void beforeAll() {
        for (String engine : ENGINES) {
            String testZoneName = ("test_zone_" + engine).toUpperCase();

            String regionName = "default_" + engine;
            sql(String.format("create zone %s with partitions=1, replicas=3, storage_profiles = '%s';",
                    testZoneName, regionName, regionName));

            sql(String.format(
                    "create table %s (ID int, NAME varchar, primary key(ID, NAME)) with primary_zone='%s', storage_profile='%s'",
                    tableName(engine),
                    testZoneName,
                    regionName
            ));
        }
    }

    /**
     * Ensures that {@link RecordView} can operate on a table containing only key columns.
     *
     * <p>It is expected that a tuple containing only key columns can be inserted, replaced and found in the table.
     *
     * @param env Test environment.
     */
    @ParameterizedTest
    @MethodSource("parameters")
    public void testRecordView(TestEnvironment env) {
        Table tab = env.table();
        RecordView<Tuple> view = tab.recordView();
        Tuple key = Tuple.create().set("id", 0).set("name", "John");

        env.runInTransaction(
                rwTx -> {
                    assertThrows(IgniteException.class, () -> view.upsert(rwTx, Tuple.copy(key).set("val", 1)));

                    view.upsert(rwTx, key);

                    // Try to replace.
                    view.upsert(rwTx, key);
                },
                tx -> {
                    assertEquals(key, view.get(tx, key));

                    // Ensure there are no duplicates.
                    assertEquals(1, view.getAll(tx, Collections.singleton(key)).size());

                    if (!tx.isReadOnly()) {
                        view.delete(tx, key);

                        assertNull(view.get(tx, key));
                    }
                }
        );
    }

    /**
     * Ensures that {@code KeyValueView<K, V>} can operate on a table containing only key columns.
     *
     * <ul>
     *     <li>{@code Null} must be specified as a value. Otherwise, an exception should be thrown.</li>
     *     <li>Re-inserting the same key must not create duplicates.</li>
     *     <li>Calling {@link KeyValueView#get(Transaction, Object)} on an existing key must throws an exception.</li>
     *     <li>Calling {@link KeyValueView#get(Transaction, Object)} on non-existing key must return {@code null}.</li>
     *     <li>Calling {@link KeyValueView#getNullable(Transaction, Object)} on an existing key
     *     must return {@link NullableValue} with no value.</li>
     *     <li>Calling {@link KeyValueView#contains(Transaction, Object)} must return {@code true} for an existing key
     *     and {@code false} if the key doesn't exist.</li>
     * </ul>
     *
     * @param env Test environment.
     */
    @ParameterizedTest
    @MethodSource("parameters")
    public void testKeyValueView(TestEnvironment env) {
        Table tab = env.table();

        KeyValueView<KeyObject, Void> kvView = tab.keyValueView(KeyObject.class, Void.class);

        KeyObject key = new KeyObject(0, "John");

        env.runInTransaction(
                rwTx -> {
                    IgniteException ex = assertThrows(IgniteException.class,
                            () -> tab.keyValueView(KeyObject.class, Integer.class).put(rwTx, key, 1));
                    assertThat(ex.getCause().getCause(), is(instanceOf(IllegalArgumentException.class)));

                    kvView.put(rwTx, key, null);

                    // Try to replace.
                    kvView.put(rwTx, key, null);
                },
                tx -> {
                    assertThrows(UnexpectedNullValueException.class, () -> kvView.get(tx, key));
                    assertNull(kvView.get(tx, new KeyObject(0, "Mary")));

                    NullableValue<Void> val = kvView.getNullable(tx, key);
                    assertNotNull(val);
                    assertNull(val.get());

                    assertTrue(kvView.contains(tx, key));
                    assertEquals(1, kvView.getAll(tx, Collections.singleton(key)).size());

                    if (!tx.isReadOnly()) {
                        kvView.remove(tx, key);

                        assertNull(kvView.getNullable(tx, key));
                        assertFalse(kvView.contains(tx, key));
                    }
                }
        );
    }

    /**
     * Ensures that binary {@code KeyValueView} can operate on a table containing only key columns.
     *
     * <ul>
     *     <li>An empty tuple must be specified as a value, {@code null} value produces {@link NullPointerException}.</li>
     *     <li>Re-inserting the same key must not create duplicates.</li>
     *     <li>Calling {@link KeyValueView#get(Transaction, Object)} on an existing key must return an empty tuple.</li>
     *     <li>Calling {@link KeyValueView#get(Transaction, Object)} on non-existing key must return {@code null}.</li>
     *     <li>Calling {@link KeyValueView#contains(Transaction, Object)} must return {@code true} for an existing key
     *     and {@code false} if the key doesn't exists.</li>
     * </ul>
     *
     * @param env Test environment.
     */
    @ParameterizedTest
    @MethodSource("parameters")
    public void testBinaryView(TestEnvironment env) {
        KeyValueView<Tuple, Tuple> binView = env.table().keyValueView();
        Tuple key = Tuple.create().set("id", 0).set("name", "John");
        Tuple emptyVal = Tuple.create();

        env.runInTransaction(
                rwTx -> {
                    // Try to put null.
                    assertThrows(NullPointerException.class, () -> binView.put(rwTx, key, null));

                    binView.put(rwTx, key, emptyVal);

                    // Try to replace.
                    binView.put(rwTx, key, emptyVal);
                },
                tx -> {
                    assertNull(binView.get(tx, Tuple.copy(key).set("name", "Mary")));
                    assertEquals(emptyVal, binView.get(tx, key));
                    assertTrue(binView.contains(tx, key));
                    assertEquals(1, binView.getAll(tx, Collections.singleton(key)).size());

                    if (!tx.isReadOnly()) {
                        binView.remove(tx, key);

                        assertFalse(binView.contains(tx, key));
                    }
                }
        );
    }

    /**
     * Ensures that we can work with a table containing only key columns using the SQL API.
     *
     * @param env Test environment.
     */
    @ParameterizedTest
    @MethodSource("parameters")
    public void testSql(TestEnvironment env) {
        String tableName = env.table().name();

        env.runInTransaction(
                rwTx -> sql(rwTx, "insert into " + tableName + " values (0, 'A'), (1, 'B')"),
                tx -> assertQuery((InternalTransaction) tx, "select count(*) from " + tableName).returns(2L).check()
        );
    }

    /**
     * Ensures that we can work with a table containing only key columns using a different APIs.
     *
     * <p>The test sequentially adds data to the table using record view, kv-view, binary view and SQL.
     * Then all written data is read using all listed APIs.
     *
     * @param env Test environment.
     */
    @ParameterizedTest
    @MethodSource("readWriteParameters")
    public void testMixed(TestEnvironment env) {
        Table tab = env.table();

        String sqlInsert = "insert into " + tab.name() + " values (%d, '%s')";
        String[] names = {"a", "b", "c", "d"};

        RecordView<Tuple> recordView = tab.recordView();
        KeyValueView<KeyObject, Void> kvView = tab.keyValueView(KeyObject.class, Void.class);
        KeyValueView<Tuple, Tuple> binView = tab.keyValueView();

        env.runInTransaction(
                List.of(
                        tx -> recordView.upsert(tx, Tuple.create().set("id", 0).set("name", names[0])),
                        tx -> {
                            assertThrowsSqlException(
                                    CONSTRAINT_VIOLATION_ERR,
                                    "PK unique constraint is violated",
                                    () -> sql(tx, String.format(sqlInsert, 0, names[0])));
                        },

                        rwTx -> {
                            assertThrowsSqlException(
                                    CONSTRAINT_VIOLATION_ERR,
                                    "PK unique constraint is violated",
                                    () -> sql(rwTx, String.format(sqlInsert, 0, names[0])));
                        },

                        rwTx -> {
                            assertThrowsSqlException(
                                    CONSTRAINT_VIOLATION_ERR,
                                    "PK unique constraint is violated",
                                    () -> sql(rwTx, String.format(sqlInsert, 0, names[0])));
                        },

                        rwTx -> kvView.put(rwTx, new KeyObject(1, names[1]), null),

                        rwTx -> binView.put(rwTx, Tuple.create().set("id", 2).set("name", names[2]), Tuple.create()),

                        rwTx -> {
                            assertThrowsSqlException(
                                    CONSTRAINT_VIOLATION_ERR,
                                    "PK unique constraint is violated",
                                    () -> sql(rwTx, String.format(sqlInsert, 2, names[2])));
                        },

                        rwTx -> sql(rwTx, String.format(sqlInsert, 3, names[3]))
                ),
                tx -> {
                    for (int i = 0; i < 4; i++) {
                        Tuple key = Tuple.create().set("id", i).set("name", names[i]);
                        KeyObject kvKey = new KeyObject(i, names[i]);

                        assertEquals(key, recordView.get(tx, key));

                        assertTrue(kvView.contains(tx, kvKey));
                        assertNotNull(kvView.getNullable(tx, kvKey));

                        assertTrue(binView.contains(tx, key));

                        assertQuery(
                                (InternalTransaction) tx, format("select * from {} where ID={} and NAME='{}'", tab.name(), i, names[i]))
                                .returns(i, names[i]).check();
                    }

                    assertQuery((InternalTransaction) tx, "select count(*) from " + tab.name()).returns(4L).check();
                }
        );
    }

    private static List<TestEnvironment> readWriteParameters() {
        List<TestEnvironment> params = new ArrayList<>(ENGINES.length * 2);

        for (String engine : ENGINES) {
            params.add(new TestEnvironment(engine, false));
        }

        return params;
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
        return "test_" + engineName;
    }

    private static class KeyObject {
        int id;
        String name;

        KeyObject() {
        }

        KeyObject(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    private static class TestEnvironment {
        private final String engine;
        private final boolean readOnlyTx;

        private TestEnvironment(String engine, boolean readOnlyTx)  {
            this.engine = engine;
            this.readOnlyTx = readOnlyTx;
        }

        private Table table() {
            return CLUSTER.aliveNode().tables().table(tableName(engine));
        }

        private void runInTransaction(List<Consumer<Transaction>> writeOps, Consumer<Transaction> readOp) {
            IgniteTransactions transactions = CLUSTER.aliveNode().transactions();

            for (Consumer<Transaction> writeOp : writeOps) {
                transactions.runInTransaction(writeOp, new TransactionOptions().readOnly(false));
            }

            transactions.runInTransaction(readOp, new TransactionOptions().readOnly(true));
        }

        private void runInTransaction(Consumer<Transaction> writeOp, Consumer<Transaction> readOp) {
            IgniteTransactions transactions = CLUSTER.aliveNode().transactions();

            if (readOnlyTx) {
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
            return "engine=" + engine + ", tx=" + (readOnlyTx ? "readonly" : "readwrite");
        }
    }
}
