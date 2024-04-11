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

import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.table.distributed.replicator.IncompatibleSchemaException;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests about basic Schema Synchronization properties that can be tested using just one Ignite node.
 */
class ItSchemaSyncSingleNodeTest extends ClusterPerTestIntegrationTest {
    private static final int NODES_TO_START = 1;

    private static final String TABLE_NAME = "test";
    private static final String UNRELATED_TABLE_NAME = "unrelated_table";

    private static final int KEY = 1;

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

        int tableId = unwrapTableViewInternal(table).tableId();

        if (operation.sql()) {
            ex = assertThrows(IgniteException.class, () -> operation.execute(table, tx, cluster));
            assertThat(
                    ex.getMessage(),
                    containsString(String.format(
                            "Table schema was updated after the transaction was started [table=%s, startSchema=1, operationSchema=2]",
                            tableId
                    ))
            );
        } else {
            ex = assertThrows(IncompatibleSchemaException.class, () -> operation.execute(table, tx, cluster));
            assertThat(
                    ex.getMessage(),
                    is(String.format(
                            "Table schema was updated after the transaction was started [table=%s, startSchema=1, operationSchema=2]",
                            tableId
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
        executeRwReadOn(table, tx, cluster);
    }

    private static void executeRwReadOn(Table table, Transaction tx, Cluster cluster) {
        cluster.doInSession(0, session -> {
            executeUpdate("SELECT * FROM " + table.name(), session, tx);
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
                executeRwReadOn(table, tx, cluster);
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

        IgniteException ex;

        int tableId = unwrapTableViewInternal(table).tableId();

        if (operation.sql()) {
            ex = assertThrows(IgniteException.class, () -> operation.execute(table, tx, cluster));
            assertThat(
                    ex.getMessage(),
                    containsString(String.format("Table was dropped [table=%s]", tableId))
            );
        } else {
            ex = assertThrows(IncompatibleSchemaException.class, () -> operation.execute(table, tx, cluster));
            assertThat(
                    ex.getMessage(),
                    is(String.format("Table was dropped [table=%s]", tableId))
            );
        }

        assertThat(ex.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));

        assertThat(tx.state(), is(TxState.ABORTED));
    }

    private void dropTable(String tableName) {
        cluster.doInSession(0, session -> {
            executeUpdate("DROP TABLE " + tableName, session);
        });
    }
}
