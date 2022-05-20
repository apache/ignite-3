/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.platform.commons.util.ExceptionUtils;

/**
 * Tests for asynchronous SQL API.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItSqlAsynchronousApiTest extends AbstractBasicIntegrationTest {
    private static final int ROW_COUNT = 16;

    /**
     * Clear tables after each test.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        for (Table t : CLUSTER_NODES.get(0).tables().tables()) {
            sql("DROP TABLE " + t.name());
        }

        super.tearDownBase(testInfo);
    }

    @Test
    public void ddl() throws ExecutionException, InterruptedException {
        IgniteSql sql = CLUSTER_NODES.get(0).sql();
        Session ses = sql.createSession();

        // CREATE TABLE
        checkDdl(true, ses, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        checkError(
                TableAlreadyExistsException.class,
                "Table already exists [name=PUBLIC.TEST]",
                ses,
                "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)"
        );
        checkDdl(false, ses, "CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, VAL VARCHAR)");

        // ADD COLUMN
        checkDdl(true, ses, "ALTER TABLE TEST ADD COLUMN IF NOT EXISTS VAL1 VARCHAR");
        checkError(
                TableNotFoundException.class,
                "Table does not exist [name=PUBLIC.NOT_EXISTS_TABLE]",
                ses,
                "ALTER TABLE NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR"
        );
        checkDdl(false, ses, "ALTER TABLE IF EXISTS NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR");
        checkError(
                ColumnAlreadyExistsException.class,
                "Column already exists [name=VAL1]",
                ses,
                "ALTER TABLE TEST ADD COLUMN VAL1 INT"
        );
        checkDdl(false, ses, "ALTER TABLE TEST ADD COLUMN IF NOT EXISTS VAL1 INT");

        // CREATE INDEX
        checkDdl(true, ses, "CREATE INDEX TEST_IDX ON TEST(VAL0)");
        checkError(
                IndexAlreadyExistsException.class,
                "Index already exists [name=TEST_IDX]",
                ses,
                "CREATE INDEX TEST_IDX ON TEST(VAL1)"
        );
        checkDdl(false, ses, "CREATE INDEX IF NOT EXISTS TEST_IDX ON TEST(VAL1)");

        // DROP COLUMNS
        checkDdl(true, ses, "ALTER TABLE TEST DROP COLUMN VAL1");
        checkError(
                TableNotFoundException.class,
                "Table does not exist [name=PUBLIC.NOT_EXISTS_TABLE]",
                ses,
                "ALTER TABLE NOT_EXISTS_TABLE DROP COLUMN VAL1"
        );
        checkDdl(false, ses, "ALTER TABLE IF EXISTS NOT_EXISTS_TABLE DROP COLUMN VAL1");
        checkError(
                ColumnNotFoundException.class,
                "Column 'VAL1' does not exist in table '\"PUBLIC\".\"TEST\"'",
                ses,
                "ALTER TABLE TEST DROP COLUMN VAL1"
        );
        checkDdl(false, ses, "ALTER TABLE TEST DROP COLUMN IF EXISTS VAL1");

        // DROP TABLE
        checkDdl(false, ses, "DROP TABLE IF EXISTS NOT_EXISTS_TABLE");
        checkDdl(true, ses, "DROP TABLE TEST");
        checkError(
                TableNotFoundException.class,
                "Table does not exist [name=PUBLIC.TEST]",
                ses,
                "DROP TABLE TEST"
        );
    }

    @Test
    public void dml() throws ExecutionException, InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = CLUSTER_NODES.get(0).sql();
        Session ses = sql.createSession();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        checkDml(ROW_COUNT, ses, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(ROW_COUNT, ses, "DELETE FROM TEST WHERE VAL0 >= 0");
    }

    @Test
    public void select() throws ExecutionException, InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        for (int i = 0; i < ROW_COUNT; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        IgniteSql sql = CLUSTER_NODES.get(0).sql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 4).build();

        TestPageProcessor pageProc = new TestPageProcessor(4);
        ses.executeAsync(null, "SELECT * FROM TEST").thenCompose(pageProc).get();

        assertEquals(ROW_COUNT, pageProc.result().size());
    }

    private void checkDdl(boolean expectedApplied, Session ses, String sql) throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncResultSet> fut = ses.executeAsync(
                null,
                sql
        );

        AsyncResultSet asyncRes = fut.get();

        assertEquals(expectedApplied, asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(-1, asyncRes.affectedRows());

        asyncRes.closeAsync().toCompletableFuture().get();
    }

    private void checkError(Class<?> expectedException, String msg, Session ses, String sql, Object... args) throws InterruptedException {
        CompletableFuture<AsyncResultSet> fut = ses.executeAsync(
                null,
                sql,
                args
        );

        try {
            AsyncResultSet asyncRes = fut.get();

            fail("Exception isn't thrown [expectedEx=" + expectedException
                    + ", msg=" + msg + ']');
        } catch (ExecutionException e) {
            Throwable t = e;

            while (t != null && expectedException != t.getClass()) {
                t = t.getCause();
            }

            assertNotNull(t, "Not expected exception. [expectedEx=" + expectedException
                    + ", msg=" + msg
                    + ", catch=" + ExceptionUtils.readStackTrace(e)
                    + ']');

            assertThat("Not expected exception. [expectedEx=" + expectedException
                    + ", msg=" + msg
                    + ", catch=" + ExceptionUtils.readStackTrace(e)
                    + ']',
                    t.getMessage(),
                    containsString(msg)
            );
        }
    }

    private void checkDml(int expectedAffectedRows, Session ses, String sql, Object... args)
            throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncResultSet> fut = ses.executeAsync(
                null,
                sql,
                args
        );

        AsyncResultSet asyncRes = fut.get();

        assertFalse(asyncRes.wasApplied());
        assertFalse(asyncRes.hasMorePages());
        assertFalse(asyncRes.hasRowSet());
        assertEquals(expectedAffectedRows, asyncRes.affectedRows());

        asyncRes.closeAsync().toCompletableFuture().get();
    }

    static class TestPageProcessor implements
            Function<AsyncResultSet, CompletionStage<AsyncResultSet>> {
        private int expectedPages;

        private final List<SqlRow> res = new ArrayList<>();

        TestPageProcessor(int expectedPages) {
            this.expectedPages = expectedPages;
        }

        @Override
        public CompletionStage<AsyncResultSet> apply(AsyncResultSet rs) {
            expectedPages--;

            assertTrue(rs.hasRowSet());
            assertFalse(rs.wasApplied());
            assertEquals(-1L, rs.affectedRows());
            assertEquals(expectedPages > 0, rs.hasMorePages());

            rs.currentPage().forEach(res::add);

            if (rs.hasMorePages()) {
                return rs.fetchNextPage().thenCompose(this);
            }

            return CompletableFuture.completedFuture(rs);
        }

        public List<SqlRow> result() {
            return res;
        }
    }
}
