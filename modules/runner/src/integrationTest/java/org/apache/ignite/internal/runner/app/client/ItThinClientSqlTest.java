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

package org.apache.ignite.internal.runner.app.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Thin client SQL integration test.
 */
public class ItThinClientSqlTest extends ItAbstractThinClientTest {
    @Test
    void testExecuteAsyncSimpleSelect() {
        AsyncResultSet resultSet = client().sql()
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

        List<ColumnMetadata> columns = resultSet.metadata().columns();
        assertEquals(2, columns.size());
        assertEquals("NUM", columns.get(0).name());
        assertEquals("STR", columns.get(1).name());
    }

    @Test
    void testExecuteAsyncDdlDml() {
        Session session = client().sql().createSession();

        // Create table.
        AsyncResultSet createRes = session
                .executeAsync(null, "CREATE TABLE testExecuteAsyncDdlDml(ID INT PRIMARY KEY, VAL VARCHAR)")
                .join();

        assertFalse(createRes.hasRowSet());
        assertTrue(createRes.wasApplied());
        assertEquals(-1, createRes.affectedRows());
        assertThrows(NoRowSetExpectedException.class, createRes::currentPageSize);

        // Insert data.
        for (int i = 0; i < 10; i++) {
            AsyncResultSet insertRes = session
                    .executeAsync(null, "INSERT INTO testExecuteAsyncDdlDml VALUES (?, ?)", i, "hello " + i)
                    .join();

            assertFalse(insertRes.hasRowSet());
            assertFalse(insertRes.wasApplied());
            assertEquals(1, insertRes.affectedRows());
            assertThrows(NoRowSetExpectedException.class, createRes::currentPage);
        }

        // Query data.
        AsyncResultSet selectRes = session
                .executeAsync(null, "SELECT VAL, ID, ID + 1 FROM testExecuteAsyncDdlDml ORDER BY ID")
                .join();

        assertTrue(selectRes.hasRowSet());
        assertFalse(selectRes.wasApplied());
        assertEquals(-1, selectRes.affectedRows());
        assertEquals(10, selectRes.currentPageSize());

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
        assertEquals(5, updateRes.affectedRows());

        // Delete table.
        AsyncResultSet deleteRes = session.executeAsync(null, "DROP TABLE testExecuteAsyncDdlDml").join();

        assertFalse(deleteRes.hasRowSet());
        assertTrue(deleteRes.wasApplied());
    }

    @Test
    @Disabled("IGNITE-16952")
    void testFetchNextPage() {
        Session session = client().sql().createSession();

        session.executeAsync(null, "CREATE TABLE testFetchNextPage(ID INT PRIMARY KEY, VAL INT)").join();

        for (int i = 0; i < 10; i++) {
            session.executeAsync(null, "INSERT INTO testFetchNextPage VALUES (?, ?)", i, i).join();
        }

        Statement statement = client().sql().statementBuilder().pageSize(4).query("SELECT ID FROM testFetchNextPage ORDER BY ID").build();

        AsyncResultSet asyncResultSet = session.executeAsync(null, statement).join();

        assertEquals(4, asyncResultSet.currentPageSize());
        assertTrue(asyncResultSet.hasMorePages());
        assertEquals(1, asyncResultSet.currentPage().iterator().next().intValue(0));

        asyncResultSet.fetchNextPage().toCompletableFuture().join();

        assertEquals(4, asyncResultSet.currentPageSize());
        assertTrue(asyncResultSet.hasMorePages());
        assertEquals(5, asyncResultSet.currentPage().iterator().next().intValue(0));

        asyncResultSet.fetchNextPage().toCompletableFuture().join();

        assertEquals(2, asyncResultSet.currentPageSize());
        assertFalse(asyncResultSet.hasMorePages());
        assertEquals(9, asyncResultSet.currentPage().iterator().next().intValue(0));
    }

    @Test
    void testInvalidSqlThrowsException() {
        CompletionException ex = assertThrows(
                CompletionException.class,
                () -> client().sql().createSession().executeAsync(null, "select x from bad").join());

        IgniteClientException clientEx = (IgniteClientException) ex.getCause();

        assertThat(clientEx.getMessage(), Matchers.containsString("Object 'BAD' not found"));
    }

    @Test
    @Disabled("IGNITE-16952")
    void testTransactionRollbackRevertsSqlUpdate() {
        Session session = client().sql().createSession();

        session.executeAsync(null, "CREATE TABLE testTx(ID INT PRIMARY KEY, VAL INT)").join();
        session.executeAsync(null, "INSERT INTO testTx VALUES (1, 1)").join();

        Transaction tx = client().transactions().begin();
        session.executeAsync(tx, "UPDATE testTx SET VAL=2").join();
        tx.rollback();

        var res = session.executeAsync(null, "SELECT VAL FROM testTx").join();
        assertEquals(1, res.currentPage().iterator().next().intValue(0));
    }
}
