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
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrow;
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
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IncompatibleSchemaException;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests about forward compatibility of table schemas as defined by IEP-110.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-110%3A+Schema+synchronization%3A+basic+schema+changes">IEP-110</a>
 */
class ItSchemaForwardCompatibilityTest extends ClusterPerTestIntegrationTest {
    private static final int NODES_TO_START = 1;

    private static final String TABLE_NAME = "test";

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
     * Makes sure forward-compatible schema changes happening between transaction operations and
     * commit do not prevent a commit from happening.
     */
    @ParameterizedTest
    @EnumSource(ForwardCompatibleDdl.class)
    void forwardCompatibleSchemaChangesAllowCommitting(ForwardCompatibleDdl ddl) {
        createTable();

        Transaction tx = node.transactions().begin();

        writeIn(tx);

        ddl.executeOn(cluster);

        assertDoesNotThrow(tx::commit);
    }

    private void writeIn(Transaction tx) {
        putInTx(cluster.node(0).tables().table(TABLE_NAME), tx);
    }

    /**
     * Makes sure forward-incompatible schema changes happening between transaction operations and
     * commit prevent a commit from happening: instead, the transaction is aborted.
     */
    @ParameterizedTest
    @EnumSource(ForwardIncompatibleDdl.class)
    void forwardIncompatibleSchemaChangesDoNotAllowSyncCommit(ForwardIncompatibleDdl ddl) {
        createTable();

        Table table = node.tables().table(TABLE_NAME);

        InternalTransaction tx = (InternalTransaction) node.transactions().begin();

        writeIn(tx);

        ddl.executeOn(cluster);

        IncompatibleSchemaException ex = assertThrows(IncompatibleSchemaException.class, tx::commit);
        assertThat(
                ex.getMessage(),
                containsString(String.format(
                        "Commit failed because schema is not forward-compatible [fromSchemaVersion=1, toSchemaVersion=2, table=%s, "
                                + "details=%s]",
                        table.name(),
                        ddl.expectedDetails
                ))
        );

        assertThat(ex.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));

        assertThat(tx.state(), is(TxState.ABORTED));
    }

    /**
     * Makes sure forward-incompatible schema changes happening between transaction operations and
     * commit prevent a commit from happening (for async API): instead, the transaction is aborted.
     */
    @Test
    void forwardIncompatibleSchemaChangesDoNotAllowAsyncCommitting() {
        createTable();

        InternalTransaction tx = (InternalTransaction) node.transactions().begin();

        writeIn(tx);

        ForwardIncompatibleDdl.CHANGE_DEFAULT.executeOn(cluster);

        IncompatibleSchemaException ex = assertWillThrow(tx.commitAsync(), IncompatibleSchemaException.class, 10, SECONDS);
        assertThat(
                ex.getMessage(),
                containsString(
                        "Commit failed because schema is not forward-compatible [fromSchemaVersion=1, toSchemaVersion=2, table=TEST, "
                                + "details=Column default value changed]"
                )
        );

        assertThat(ex.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));

        assertThat(tx.state(), is(TxState.ABORTED));
    }

    private void createTable() {
        cluster.doInSession(0, session -> {
            executeUpdate(
                    "CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, not_null_int INT NOT NULL, int_with_default INT DEFAULT 1)",
                    session
            );
        });
    }

    private static void putInTx(Table table, Transaction tx) {
        table.keyValueView().put(tx, Tuple.create().set("id", 1), Tuple.create().set("not_null_int", 1));
    }

    private enum ForwardCompatibleDdl {
        ADD_NULLABLE_COLUMN("ALTER TABLE " + TABLE_NAME + " ADD COLUMN new_col INT"),
        ADD_COLUMN_WITH_DEFAULT("ALTER TABLE " + TABLE_NAME + " ADD COLUMN new_col INT NOT NULL DEFAULT 42"),
        // TODO: IGNITE-19485, IGNITE-20315 - Uncomment this after column rename support gets added.
        // RENAME_COLUMN("ALTER TABLE " + TABLE_NAME + " RENAME COLUMN not_null_int to new_col"),
        DROP_NOT_NULL("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN not_null_int DROP NOT NULL"),
        WIDEN_COLUMN_TYPE("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN not_null_int SET DATA TYPE BIGINT");

        private final String ddl;

        ForwardCompatibleDdl(String ddl) {
            this.ddl = ddl;
        }

        void executeOn(Cluster cluster) {
            cluster.doInSession(0, session -> {
                executeUpdate(ddl, session);
            });
        }
    }

    private enum ForwardIncompatibleDdl {
        // TODO: Enable after https://issues.apache.org/jira/browse/IGNITE-19484 is fixed.
        // RENAME_TABLE("RENAME TABLE " + TABLE_NAME + " to new_table"),
        DROP_COLUMN("ALTER TABLE " + TABLE_NAME + " DROP COLUMN not_null_int", "Columns were dropped"),
        ADD_DEFAULT("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN not_null_int SET DEFAULT 102", "Column default value changed"),
        CHANGE_DEFAULT("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN int_with_default SET DEFAULT 102",
                "Column default value changed"),
        DROP_DEFAULT("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN int_with_default DROP DEFAULT", "Column default value changed");

        private final String ddl;
        private final String expectedDetails;

        ForwardIncompatibleDdl(String ddl, String expectedDetails) {
            this.ddl = ddl;
            this.expectedDetails = expectedDetails;
        }

        void executeOn(Cluster cluster) {
            cluster.doInSession(0, session -> {
                executeUpdate(ddl, session);
            });
        }
    }
}
