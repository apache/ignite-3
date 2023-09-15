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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** Test common SQL API. */
public class ItCommonApiTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int nodes() {
        return 1;
    }

    /** Check correctness of session expiration. */
    @Test
    public void testSessionExpiration() throws Exception {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TST(id INTEGER PRIMARY KEY, val INTEGER)");
        sql("INSERT INTO TST VALUES (1,1), (2,2), (3,3), (4,4)");

        Session ses1 = sql.sessionBuilder().defaultPageSize(1).idleTimeout(2, TimeUnit.SECONDS).build();
        Session ses2 = sql.sessionBuilder().defaultPageSize(1).idleTimeout(100, TimeUnit.SECONDS).build();

        assertEquals(2, queryProcessor().liveSessions().size());

        ResultSet rs1 = ses1.execute(null, "SELECT id FROM TST");
        ResultSet rs2 = ses2.execute(null, "SELECT id FROM TST");

        waitForCondition(() -> queryProcessor().liveSessions().size() == 1, 10_000);

        // first session should no longer exist for the moment
        ExecutionException err = assertThrows(ExecutionException.class, () -> ses1.executeAsync(null, "SELECT 1 + 1").get());
        assertThat(err.getCause(), instanceOf(IgniteException.class));
        assertThat(err.getCause().getMessage(), containsString("Session not found"));

        // already started query should fail due to session has been expired
        assertThrowsWithCause(() -> {
            while (rs1.hasNext()) {
                rs1.next();
            }
        }, QueryCancelledException.class);

        rs1.close();

        // second session could proceed with execution
        while (rs2.hasNext()) {
            rs2.next();
        }

        // second session could start new query
        ses2.execute(null, "SELECT 2 + 2").close();
    }

    /** Check timestamp type operations correctness using sql and kv api. */
    @Test
    public void checkTimestampOperations() {
        String kvTblName = "tbl_all_columns_sql";
        String keyCol = "KEY";

        Ignite node = CLUSTER_NODES.get(0);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19162 Trim all less than millisecond information from timestamp
        //String tsStr = "2023-03-29T08:22:33.005007Z";
        // TODO: IGNITE-20105 it should be "2023-03-29T08:22:33.005Z";
        String tsStr = "2023-03-29T08:22:33Z";

        Instant ins = Instant.parse(tsStr);

        sql("CREATE TABLE timestamps(id INTEGER PRIMARY KEY, i TIMESTAMP(9))");

        // TODO: IGNITE-19274 Add column with TIMESTAMP WITH LOCAL TIME ZONE
        sql(String.format("CREATE TABLE %s(\"%s\" INTEGER PRIMARY KEY, \"TIMESTAMP\" TIMESTAMP(9))", kvTblName, keyCol));

        Table tbl = node.tables().table(kvTblName);

        Tuple rec = Tuple.create()
                .set("KEY", 1)
                .set("TIMESTAMP", LocalDateTime.of(2023, 3, 29, 8, 22, 33));

        tbl.recordView().insert(null, rec);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19161 Can`t insert timestamp representing in ISO_INSTANT format
        tsStr = tsStr.replace("T", " ").substring(0, tsStr.length() - 1);

        sql("INSERT INTO timestamps VALUES (101, TIMESTAMP '" + tsStr + "')");

        try (Session ses = node.sql().createSession()) {
            // for projection pop up
            ResultSet<SqlRow> res = ses.execute(null, "SELECT i, id FROM timestamps");

            String srtRepr = ins.toString();

            String expDateTimeStr = srtRepr.substring(0, srtRepr.length() - 1);

            assertEquals(expDateTimeStr, res.next().datetimeValue(0).toString());

            String query = "select \"KEY\", \"TIMESTAMP\" from TBL_ALL_COLUMNS_SQL ORDER BY KEY";

            res = ses.execute(null, query);

            assertEquals(expDateTimeStr, res.next().datetimeValue(1).toString());
        }
    }

    /** Check transaction change status with erroneous statements. */
    @Test
    public void testTxStateChangedOnErroneousOp() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        TxManager txManager = txManager();

        SqlSchemaManager oldManager =
                (SqlSchemaManager) IgniteTestUtils.getFieldValue(queryProcessor(), SqlQueryProcessor.class, "sqlSchemaManager");

        Transaction tx = CLUSTER_NODES.get(0).transactions().begin();

        try {
            sql(tx, "INSERT INTO PUBLIC.TEST VALUES(1, 1)");
            sql(tx, "INSERT INTO NOTEXIST.TEST VALUES(1, 1)");
        } catch (Throwable ignore) {
            // No op.
        }

        assertEquals(1, txManager.pending());
        InternalTransaction tx0 = (InternalTransaction) tx;
        assertEquals(TxState.PENDING, tx0.state());

        tx.rollback();
        assertEquals(0, txManager.pending());

        sql("INSERT INTO TEST VALUES(1, 1)");
        assertEquals(0, txManager.pending());

        var schemaManager = new ErroneousSchemaManager();

        // TODO: refactor after https://issues.apache.org/jira/browse/IGNITE-17694
        IgniteTestUtils.setFieldValue(queryProcessor(), "sqlSchemaManager", schemaManager);

        try {
            sql("SELECT a FROM NOTEXIST.TEST");
        } catch (Throwable ignore) {
            // No op.
        }

        try {
            sql("INSERT INTO NOTEXIST.TEST VALUES(1, 1)");
        } catch (Throwable ignore) {
            // No op.
        }

        assertEquals(0, txManager.pending());

        IgniteTestUtils.setFieldValue(queryProcessor(), "sqlSchemaManager", oldManager);
    }

    private static class ErroneousSchemaManager implements SqlSchemaManager {

        /** {@inheritDoc} */
        @Override
        public @Nullable SchemaPlus schema(@Nullable String name, int version) {
            return null;
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable SchemaPlus schema(@Nullable String name, long timestamp) {
            return null;
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> schemaReadyFuture(int version) {
            throw new UnsupportedOperationException();
        }
    }
}
