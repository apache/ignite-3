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

package org.apache.ignite.internal.runner.app.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Thin client SQL integration test.
 */
@SuppressWarnings("resource")
public class ItThinClientSqlTest extends ItAbstractThinClientTest {
    @Test
    void testExecuteAsyncSimpleSelect() {
        AsyncResultSet<SqlRow> resultSet = client().sql()
                .createSession()
                .executeAsync(null, "select 1 as num, 'hello' as str")
                .join();

        assertTrue(resultSet.hasRowSet());
        assertFalse(resultSet.wasApplied());
        assertFalse(resultSet.hasMorePages());
        assertEquals(-1, resultSet.affectedRows());
        assertEquals(1, resultSet.currentPageSize());

        SqlRow row = resultSet.currentPage().iterator().next();
        assertEquals(1, row.intValue(0));
        assertEquals("hello", row.stringValue(1));

        ResultSetMetadata metadata = resultSet.metadata();
        assertNotNull(metadata);

        List<ColumnMetadata> columns = metadata.columns();
        assertEquals(2, columns.size());
        assertEquals("NUM", columns.get(0).name());
        assertEquals("STR", columns.get(1).name());
    }

    @Test
    void testExecuteSimpleSelect() {
        ResultSet<SqlRow> resultSet = client().sql()
                .createSession()
                .execute(null, "select 1 as num, 'hello' as str");

        assertTrue(resultSet.hasRowSet());
        assertFalse(resultSet.wasApplied());
        assertEquals(-1, resultSet.affectedRows());

        SqlRow row = resultSet.next();
        assertEquals(1, row.intValue(0));
        assertEquals("hello", row.stringValue(1));

        List<ColumnMetadata> columns = resultSet.metadata().columns();
        assertEquals(2, columns.size());
        assertEquals("NUM", columns.get(0).name());
        assertEquals("STR", columns.get(1).name());
    }

    @Test
    void testTxCorrectness() throws InterruptedException {
        Session ses = client().sql().createSession();

        // Create table.
        ses.execute(null, "CREATE TABLE testExecuteDdlDml(ID INT NOT NULL PRIMARY KEY, VAL VARCHAR)");
        waitForTableOnAllNodes("testExecuteDdlDml");

        // Async
        Transaction tx = client().transactions().begin();

        ses.executeAsync(tx, "INSERT INTO testExecuteDdlDml VALUES (?, ?)",
                Integer.MAX_VALUE, "hello " + Integer.MAX_VALUE).join();

        tx.rollback();

        tx = client().transactions().begin();

        ses.executeAsync(tx, "INSERT INTO testExecuteDdlDml VALUES (?, ?)",
                100, "hello " + Integer.MAX_VALUE).join();

        tx.commit();

        // Sync
        tx = client().transactions().begin();

        ses.executeAsync(tx, "INSERT INTO testExecuteDdlDml VALUES (?, ?)",
                Integer.MAX_VALUE, "hello " + Integer.MAX_VALUE).join();

        tx.rollback();

        tx = client().transactions().begin();

        ses.execute(tx, "INSERT INTO testExecuteDdlDml VALUES (?, ?)",
                200, "hello " + Integer.MAX_VALUE);

        tx.commit();

        // Outdated tx.
        Transaction tx0 = tx;

        assertThrows(CompletionException.class, () -> {
            ses.executeAsync(tx0, "INSERT INTO testExecuteDdlDml VALUES (?, ?)",
                Integer.MAX_VALUE, "hello " + Integer.MAX_VALUE).join();
        });

        assertThrows(IgniteException.class, () -> {
            ses.execute(tx0, "INSERT INTO testExecuteDdlDml VALUES (?, ?)",
                    Integer.MAX_VALUE, "hello " + Integer.MAX_VALUE);
        });

        for (int i = 0; i < 10; i++) {
            ses.execute(null, "INSERT INTO testExecuteDdlDml VALUES (?, ?)", i, "hello " + i);
        }

        ResultSet<SqlRow> selectRes = ses.execute(null, "SELECT * FROM testExecuteDdlDml ORDER BY ID");

        var rows = new ArrayList<SqlRow>();
        selectRes.forEachRemaining(rows::add);

        assertEquals(1 + 1 + 10, rows.size());

        // Delete table.
        ses.execute(null, "DROP TABLE testExecuteDdlDml");
    }

    @Test
    void testExecuteAsyncDdlDml() throws InterruptedException {
        Session session = client().sql().createSession();

        // Create table.
        AsyncResultSet createRes = session
                .executeAsync(null, "CREATE TABLE testExecuteAsyncDdlDml(ID INT PRIMARY KEY, VAL VARCHAR)")
                .join();

        assertFalse(createRes.hasRowSet());
        assertNull(createRes.metadata());
        assertTrue(createRes.wasApplied());
        assertEquals(-1, createRes.affectedRows());
        assertThrows(NoRowSetExpectedException.class, createRes::currentPageSize);

        waitForTableOnAllNodes("testExecuteAsyncDdlDml");

        // Insert data.
        for (int i = 0; i < 10; i++) {
            AsyncResultSet insertRes = session
                    .executeAsync(null, "INSERT INTO testExecuteAsyncDdlDml VALUES (?, ?)", i, "hello " + i)
                    .join();

            assertFalse(insertRes.hasRowSet());
            assertNull(insertRes.metadata());
            assertFalse(insertRes.wasApplied());
            assertEquals(1, insertRes.affectedRows());
            assertThrows(NoRowSetExpectedException.class, insertRes::currentPage);
        }

        // Query data.
        AsyncResultSet<SqlRow> selectRes = session
                .executeAsync(null, "SELECT VAL as MYVALUE, ID, ID + 1 FROM testExecuteAsyncDdlDml ORDER BY ID")
                .join();

        assertTrue(selectRes.hasRowSet());
        assertFalse(selectRes.wasApplied());
        assertEquals(-1, selectRes.affectedRows());
        assertEquals(10, selectRes.currentPageSize());

        List<ColumnMetadata> columns = selectRes.metadata().columns();
        assertEquals(3, columns.size());
        assertEquals("MYVALUE", columns.get(0).name());
        assertEquals("ID", columns.get(1).name());
        assertEquals("ID + 1", columns.get(2).name());

        var rows = new ArrayList<SqlRow>();
        selectRes.currentPage().forEach(rows::add);

        assertEquals(10, rows.size());
        assertEquals("hello 1", rows.get(1).stringValue(0));
        assertEquals(1, rows.get(1).intValue(1));
        assertEquals(2, rows.get(1).intValue(2));

        // Update data.
        AsyncResultSet updateRes = session
                .executeAsync(null, "UPDATE testExecuteAsyncDdlDml SET VAL='upd' WHERE ID < 5").join();

        assertFalse(updateRes.wasApplied());
        assertFalse(updateRes.hasRowSet());
        assertNull(updateRes.metadata());
        assertEquals(5, updateRes.affectedRows());

        // Delete table.
        AsyncResultSet deleteRes = session.executeAsync(null, "DROP TABLE testExecuteAsyncDdlDml").join();

        assertFalse(deleteRes.hasRowSet());
        assertNull(deleteRes.metadata());
        assertTrue(deleteRes.wasApplied());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void testExecuteDdlDml() throws InterruptedException {
        Session session = client().sql().createSession();

        // Create table.
        ResultSet createRes = session.execute(
                null,
                "CREATE TABLE testExecuteDdlDml(ID INT NOT NULL PRIMARY KEY, VAL VARCHAR)");

        waitForTableOnAllNodes("testExecuteDdlDml");

        assertFalse(createRes.hasRowSet());
        assertNull(createRes.metadata());
        assertTrue(createRes.wasApplied());
        assertEquals(-1, createRes.affectedRows());

        // Insert data.
        for (int i = 0; i < 10; i++) {
            ResultSet insertRes = session.execute(
                    null,
                    "INSERT INTO testExecuteDdlDml VALUES (?, ?)", i, "hello " + i);

            assertFalse(insertRes.hasRowSet());
            assertNull(insertRes.metadata());
            assertFalse(insertRes.wasApplied());
            assertEquals(1, insertRes.affectedRows());
        }

        // Query data.
        ResultSet<SqlRow> selectRes = session
                .execute(null, "SELECT VAL as MYVALUE, ID, ID + 1 FROM testExecuteDdlDml ORDER BY ID");

        assertTrue(selectRes.hasRowSet());
        assertFalse(selectRes.wasApplied());
        assertEquals(-1, selectRes.affectedRows());

        List<ColumnMetadata> columns = selectRes.metadata().columns();
        assertEquals(3, columns.size());

        assertEquals("MYVALUE", columns.get(0).name());
        assertEquals("VAL", columns.get(0).origin().columnName());
        assertEquals("PUBLIC", columns.get(0).origin().schemaName());
        assertEquals("TESTEXECUTEDDLDML", columns.get(0).origin().tableName());
        assertTrue(columns.get(0).nullable());
        assertEquals(String.class, columns.get(0).valueClass());
        assertEquals(ColumnType.STRING, columns.get(0).type());

        assertEquals(ColumnMetadata.UNDEFINED_SCALE, columns.get(0).scale());
        assertEquals(2 << 15, columns.get(0).precision());

        assertEquals("ID", columns.get(1).name());
        assertEquals("ID", columns.get(1).origin().columnName());
        assertEquals("PUBLIC", columns.get(1).origin().schemaName());
        assertEquals("TESTEXECUTEDDLDML", columns.get(1).origin().tableName());
        assertFalse(columns.get(1).nullable());

        assertEquals("ID + 1", columns.get(2).name());
        assertNull(columns.get(2).origin());

        var rows = new ArrayList<SqlRow>();
        selectRes.forEachRemaining(rows::add);

        assertEquals(10, rows.size());
        assertEquals("hello 1", rows.get(1).stringValue(0));
        assertEquals(1, rows.get(1).intValue(1));
        assertEquals(2, rows.get(1).intValue(2));

        // Update data.
        ResultSet updateRes = session.execute(null, "UPDATE testExecuteDdlDml SET VAL='upd' WHERE ID < 5");

        assertFalse(updateRes.wasApplied());
        assertFalse(updateRes.hasRowSet());
        assertNull(updateRes.metadata());
        assertEquals(5, updateRes.affectedRows());

        // Delete table.
        ResultSet deleteRes = session.execute(null, "DROP TABLE testExecuteDdlDml");

        assertFalse(deleteRes.hasRowSet());
        assertNull(deleteRes.metadata());
        assertTrue(deleteRes.wasApplied());
    }

    @Test
    void testFetchNextPage() throws InterruptedException {
        Session session = client().sql().createSession();

        session.executeAsync(null, "CREATE TABLE testFetchNextPage(ID INT PRIMARY KEY, VAL INT)").join();
        waitForTableOnAllNodes("testFetchNextPage");

        for (int i = 0; i < 10; i++) {
            session.executeAsync(null, "INSERT INTO testFetchNextPage VALUES (?, ?)", i, i).join();
        }

        Statement statement = client().sql().statementBuilder().pageSize(4).query("SELECT ID FROM testFetchNextPage ORDER BY ID").build();

        AsyncResultSet<SqlRow> asyncResultSet = session.executeAsync(null, statement).join();

        assertEquals(4, asyncResultSet.currentPageSize());
        assertTrue(asyncResultSet.hasMorePages());
        assertEquals(0, asyncResultSet.currentPage().iterator().next().intValue(0));

        asyncResultSet.fetchNextPage().toCompletableFuture().join();

        assertEquals(4, asyncResultSet.currentPageSize());
        assertTrue(asyncResultSet.hasMorePages());
        assertEquals(4, asyncResultSet.currentPage().iterator().next().intValue(0));

        asyncResultSet.fetchNextPage().toCompletableFuture().join();

        assertEquals(2, asyncResultSet.currentPageSize());
        assertFalse(asyncResultSet.hasMorePages());
        assertEquals(8, asyncResultSet.currentPage().iterator().next().intValue(0));
    }

    @Test
    void testInvalidSqlThrowsException() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () -> client().sql().createSession().executeAsync(null, "select x from bad").join());

        var clientEx = (IgniteException) ex.getCause();

        assertThat(clientEx.getMessage(), Matchers.containsString("Object 'BAD' not found"));
    }

    @Test
    void testTransactionRollbackRevertsSqlUpdate() throws InterruptedException {
        Session session = client().sql().createSession();

        session.executeAsync(null, "CREATE TABLE testTx(ID INT PRIMARY KEY, VAL INT)").join();
        waitForTableOnAllNodes("testTx");

        session.executeAsync(null, "INSERT INTO testTx VALUES (1, 1)").join();

        Transaction tx = client().transactions().begin();
        session.executeAsync(tx, "UPDATE testTx SET VAL=2").join();
        tx.rollback();

        var res = session.executeAsync(null, "SELECT VAL FROM testTx").join();
        assertEquals(1, res.currentPage().iterator().next().intValue(0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testResultSetMapping(boolean useStatement) {
        Session session = client().sql().createSession();
        String query = "select 123 + ? as num, 'Hello!' as str";

        ResultSet<Pojo> resultSet = useStatement
                ? session.execute(null, Mapper.of(Pojo.class), client().sql().statementBuilder().query(query).build(), 10)
                : session.execute(null, Mapper.of(Pojo.class), query, 10);

        assertTrue(resultSet.hasRowSet());

        Pojo row = resultSet.next();

        assertEquals(133, row.num);
        assertEquals("Hello!", row.str);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testResultSetMappingAsync(boolean useStatement) {
        Session session = client().sql().createSession();
        String query = "select 1 as num, concat('hello ', ?) as str";

        AsyncResultSet<Pojo> resultSet = useStatement
                ? session.executeAsync(null, Mapper.of(Pojo.class), client().sql().statementBuilder().query(query).build(), "world").join()
                : session.executeAsync(null, Mapper.of(Pojo.class), query, "world").join();

        assertTrue(resultSet.hasRowSet());
        assertEquals(1, resultSet.currentPageSize());

        Pojo row = resultSet.currentPage().iterator().next();

        assertEquals(1, row.num);
        assertEquals("hello world", row.str);
    }


    @Test
    void testResultSetMappingColumnNameMismatch() {
        String query = "select 1 as foo, 2 as bar";

        ResultSet<Pojo> resultSet = client().sql().createSession().execute(null, Mapper.of(Pojo.class), query);
        Pojo row = resultSet.next();

        assertEquals(0, row.num);
        assertNull(row.str);
    }

    private void waitForTableOnAllNodes(String tableName) throws InterruptedException {
        // TODO IGNITE-18733, IGNITE-18449: remove this workaround when issues are fixed.
        // Currently newly created table is not immediately available on all nodes.
        boolean res = IgniteTestUtils.waitForCondition(() -> {
            var nodeCount = client().clusterNodes().size();

            // Do N checks - they will go to different nodes becase of request balancing.
            for (int i = 0; i < nodeCount; i++) {
                if (client().tables().table(tableName) == null) {
                    return false;
                }
            }

            return true;
        }, 3000);

        assertTrue(res);
    }

    private static class Pojo {
        public long num;

        public String str;
    }
}
