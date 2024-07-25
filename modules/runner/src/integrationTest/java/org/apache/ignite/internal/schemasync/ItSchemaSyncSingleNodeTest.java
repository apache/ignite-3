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

package org.apache.ignite.internal.schemasync;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IncompatibleSchemaException;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests about basic Schema Synchronization properties that can be tested using just one Ignite node.
 */
class ItSchemaSyncSingleNodeTest extends ClusterPerTestIntegrationTest {
    private static final int NODES_TO_START = 1;

    private static final String TABLE_NAME = "test";
    private static final String UNRELATED_TABLE_NAME = "unrelated_table";

    private static final int KEY = 1;

    private static final int NON_EXISTENT_KEY = Integer.MAX_VALUE;

    private IgniteImpl node;

    @Override
    protected int initialNodes() {
        return NODES_TO_START;
    }

    @BeforeEach
    void assignNode() {
        node = cluster.node(0);
    }

    /**
     * Makes sure that the following sequence results in an operation failure and transaction rollback.
     *
     * <ol>
     *     <li>A table is created</li>
     *     <li>A transaction is started</li>
     *     <li>The table is enlisted in the transaction</li>
     *     <li>The table is ALTERed</li>
     *     <li>An attempt to read or write to the table in the transaction is made</li>
     * </ol>
     */
    @ParameterizedTest
    @EnumSource(Operation.class)
    void readWriteOperationInTxAfterAlteringSchemaOnTargetTableIsRejected(Operation operation) {
        createTable();

        Table table = node.tables().table(TABLE_NAME);

        putPreExistingValueTo(table);

        InternalTransaction tx = (InternalTransaction) node.transactions().begin();

        enlistTableInTransaction(table, tx);

        alterTable(TABLE_NAME);

        IgniteException ex;

        if (operation.sql()) {
            ex = assertThrows(IgniteException.class, () -> operation.execute(table, tx, cluster));

            assertThat(
                    ex.getMessage(),
                    containsString(String.format(
                            "Table schema was updated after the transaction was started [table=%s, startSchema=1, operationSchema=2]",
                            table.name()
                    ))
            );
        } else {
            ex = assertThrows(IncompatibleSchemaException.class, () -> operation.execute(table, tx, cluster));

            assertThat(
                    ex.getMessage(),
                    is(String.format(
                            "Table schema was updated after the transaction was started [table=%s, startSchema=1, operationSchema=2]",
                            table.name()
                    ))
            );
        }

        assertThat(ex.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));

        assertThat(tx.state(), is(TxState.ABORTED));
    }

    private void createTable() {
        executeUpdate("CREATE TABLE " + TABLE_NAME + " (id int, val varchar, PRIMARY KEY USING HASH (id))", node.sql());
    }

    private void alterTable(String tableName) {
        executeUpdate("ALTER TABLE " + tableName + " ADD COLUMN added int", node.sql());
    }

    private static void putPreExistingValueTo(Table table) {
        table.keyValueView().put(null, Tuple.create().set("id", KEY), Tuple.create().set("val", "original"));
    }

    private void enlistTableInTransaction(Table table, Transaction tx) {
        executeRwReadOn(table, tx, NON_EXISTENT_KEY, cluster);
    }

    private static void executeRwReadOn(Table table, Transaction tx, int key, Cluster cluster) {
        cluster.doInSession(0, session -> {
            executeUpdate("SELECT * FROM " + table.name() + " WHERE id = " + key, session, tx);
        });
    }

    private enum Operation {
        KV_WRITE {
            @Override
            void execute(Table table, Transaction tx, Cluster cluster) {
                putInTx(table, tx);
            }

            @Override
            boolean sql() {
                return false;
            }
        },
        KV_READ {
            @Override
            void execute(Table table, Transaction tx, Cluster cluster) {
                table.keyValueView().get(tx, Tuple.create().set("id", KEY));
            }

            @Override
            boolean sql() {
                return false;
            }
        },
        SQL_WRITE {
            @Override
            void execute(Table table, Transaction tx, Cluster cluster) {
                cluster.doInSession(0, session -> {
                    executeUpdate("UPDATE " + table.name() + " SET val = 'new value' WHERE id = " + KEY, session, tx);
                });
            }

            @Override
            boolean sql() {
                return true;
            }
        },
        SQL_READ {
            @Override
            void execute(Table table, Transaction tx, Cluster cluster) {
                executeRwReadOn(table, tx, KEY, cluster);
            }

            @Override
            boolean sql() {
                return true;
            }
        };

        abstract void execute(Table table, Transaction tx, Cluster cluster);

        abstract boolean sql();
    }

    private static void putInTx(Table table, Transaction tx) {
        table.keyValueView().put(tx, Tuple.create().set("id", 1), Tuple.create().set("val", "one"));
    }

    /**
     * Makes sure that ALTERation of a table does not affect a transaction in which it's not enlisted.
     */
    @Test
    void readWriteOperationInTxAfterAlteringSchemaOnAnotherTableIsUnaffected() {
        cluster.doInSession(0, session -> {
            executeUpdate("CREATE TABLE " + TABLE_NAME + " (id int PRIMARY KEY, val varchar)", session);
            executeUpdate("CREATE TABLE " + UNRELATED_TABLE_NAME + " (id int PRIMARY KEY, val varchar)", session);
        });

        Table table = node.tables().table(TABLE_NAME);

        InternalTransaction tx = (InternalTransaction) node.transactions().begin();

        enlistTableInTransaction(table, tx);

        alterTable(UNRELATED_TABLE_NAME);

        assertDoesNotThrow(() -> putInTx(table, tx));
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    void readWriteOperationAfterDroppingTargetTableIsRejected(Operation operation) {
        createTable();

        Table table = node.tables().table(TABLE_NAME);

        putPreExistingValueTo(table);

        InternalTransaction tx = (InternalTransaction) node.transactions().begin();

        enlistTableInTransaction(table, tx);

        dropTable(TABLE_NAME);

        int errorCode;

        int tableId = unwrapTableViewInternal(table).tableId();

        if (operation.sql()) {
            IgniteException ex = assertThrows(IgniteException.class, () -> operation.execute(table, tx, cluster));
            errorCode = ex.code();

            assertThat(
                    ex.getMessage(),
                    // TODO https://issues.apache.org/jira/browse/IGNITE-22309 use tableName instead
                    containsString(String.format("Table was dropped [tableId=%s]", tableId))
            );
        } else {
            IncompatibleSchemaException ex = assertThrows(IncompatibleSchemaException.class, () -> operation.execute(table, tx, cluster));
            errorCode = ex.code();

            assertThat(
                    ex.getMessage(),
                    // TODO https://issues.apache.org/jira/browse/IGNITE-22309 use tableName instead
                    is(String.format("Table was dropped [tableId=%s]", tableId))
            );
        }

        assertThat(errorCode, is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));

        assertThat(tx.state(), is(TxState.ABORTED));
    }

    private void dropTable(String tableName) {
        cluster.doInSession(0, session -> {
            executeUpdate("DROP TABLE " + tableName, session);
        });
    }

    @ParameterizedTest
    @EnumSource(CommitOperation.class)
    void commitAfterDroppingTargetTableIsRejected(CommitOperation operation) {
        createTable();

        Table table = node.tables().table(TABLE_NAME);

        InternalTransaction tx = (InternalTransaction) node.transactions().begin();

        enlistTableInTransaction(table, tx);

        dropTable(TABLE_NAME);

        Throwable ex = assertThrows(Throwable.class, () -> operation.executeOn(tx));
        ex = ExceptionUtils.unwrapCause(ex);

        assertThat(ex, is(instanceOf(IncompatibleSchemaException.class)));
        assertThat(
                ex.getMessage(),
                containsString(String.format("Commit failed because a table was already dropped [table=%s]", table.name()))
        );

        assertThat(((IncompatibleSchemaException) ex).code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));

        assertThat(tx.state(), is(TxState.ABORTED));
    }

    private enum CommitOperation {
        COMMIT(tx -> tx.commit()),
        COMMIT_ASYNC_GET(tx -> tx.commitAsync().get(10, SECONDS))
        ;

        private final ConsumerX<Transaction> commitOp;

        CommitOperation(ConsumerX<Transaction> commitOp) {
            this.commitOp = commitOp;
        }

        void executeOn(Transaction tx) throws Exception {
            commitOp.accept(tx);
        }
    }

    /**
     * The scenario is like the following.
     *
     * <ol>
     *     <li>tx1 starts and enlists table's partition</li>
     *     <li>table's schema gets changed</li>
     *     <li>tx2 starts, adds/updates a tuple for key k1 and commits (in this test tx2 is implicit)</li>
     *     <li>tx1 tries to read k1 via get/scan: this must fail</li>
     * </ol>
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void readingDataInFutureVersionsFails(boolean scan) {
        createTable();

        Table table = node.tables().table(TABLE_NAME);
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        InternalTransaction tx1 = (InternalTransaction) node.transactions().begin();

        enlistTableInTransaction(table, tx1);

        alterTable(TABLE_NAME);

        Tuple keyTuple = Tuple.create().set("id", KEY);
        kvView.put(null, keyTuple, Tuple.create().set("val", "put-in-tx2"));

        Executable task = scan ? () -> consumeCursor(kvView.query(tx1, null)) : () -> kvView.get(tx1, keyTuple);

        IgniteException ex = assertThrows(IgniteException.class, task);

        assertThat(
                ex.getMessage(),
                containsString(String.format(
                        "Operation failed because it tried to access a row with newer schema version than transaction's [table=%s, "
                                + "txSchemaVersion=1, rowSchemaVersion=2]",
                        table.name()
                ))
        );

        assertThat(ex.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));
    }

    private static void consumeCursor(Cursor<?> cursor) {
        try (Cursor<?> c = cursor) {
            c.forEachRemaining(obj -> {});
        }
    }

    @FunctionalInterface
    private interface ConsumerX<T> {
        void accept(T obj) throws Exception;
    }
}
