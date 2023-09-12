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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
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
        cluster.doInSession(0, session -> {
            executeUpdate("create table " + TABLE_NAME + " (id int primary key, val varchar)", session);
        });

        Table table = node.tables().table(TABLE_NAME);

        putPreExistingValueTo(table);

        InternalTransaction tx = (InternalTransaction) node.transactions().begin();

        enlistTableInTransaction(table, tx);

        cluster.doInSession(0, session -> {
            executeUpdate("alter table " + TABLE_NAME + " add column added int", session);
        });

        IgniteException ex;

        if (operation.sql()) {
            ex = assertThrows(IgniteException.class, () -> operation.execute(table, tx, cluster));
            assertThat(
                    ex.getMessage(),
                    containsString("Table schema was updated after the transaction was started [table=1, startSchema=1, operationSchema=2")
            );
        } else {
            ex = assertThrows(TransactionException.class, () -> operation.execute(table, tx, cluster));
            assertThat(
                    ex.getMessage(),
                    is("Table schema was updated after the transaction was started [table=1, startSchema=1, operationSchema=2")
            );
        }

        assertThat(ex.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));

        // TODO: IGNITE-20342 - Assert for SQL too.
        if (!operation.sql()) {
            assertThat(tx.state(), is(TxState.ABORTED));
        }
    }

    private static void putPreExistingValueTo(Table table) {
        table.keyValueView().put(null, Tuple.create().set("id", KEY), Tuple.create().set("val", "original"));
    }

    private void enlistTableInTransaction(Table table, Transaction tx) {
        executeReadOn(table, tx, cluster);
    }

    private static void executeReadOn(Table table, Transaction tx, Cluster cluster) {
        cluster.doInSession(0, session -> {
            executeUpdate("select * from " + table.name(), session, tx);
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
                    executeUpdate("update " + table.name() + " set val = 'new value' where id = " + KEY, session, tx);
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
                executeReadOn(table, tx, cluster);
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
            executeUpdate("create table " + TABLE_NAME + " (id int primary key, val varchar)", session);
            executeUpdate("create table " + UNRELATED_TABLE_NAME + " (id int primary key, val varchar)", session);
        });

        Table table = node.tables().table(TABLE_NAME);

        InternalTransaction tx = (InternalTransaction) node.transactions().begin();

        enlistTableInTransaction(table, tx);

        cluster.doInSession(0, session -> {
            executeUpdate("alter table " + UNRELATED_TABLE_NAME + " add column added int", session);
        });

        assertDoesNotThrow(() -> putInTx(table, tx));
    }
}
