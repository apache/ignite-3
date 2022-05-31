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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
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
        assertEquals(0, resultSet.affectedRows());
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
        AsyncResultSet createRes = session.executeAsync(null, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL VARCHAR)").join();

        assertFalse(createRes.hasRowSet());
        assertTrue(createRes.wasApplied());
        assertEquals(-1, createRes.affectedRows());
        assertThrows(NoRowSetExpectedException.class, createRes::currentPageSize);

        // Insert data.
        for (int i = 0; i < 10; i++) {
            AsyncResultSet insertRes = session
                    .executeAsync(null, "INSERT INTO TEST VALUES (?, ?)", i, "hello " + i)
                    .join();

            assertFalse(insertRes.hasRowSet());
            assertFalse(insertRes.wasApplied());
            assertEquals(1, insertRes.affectedRows());
            assertThrows(NoRowSetExpectedException.class, createRes::currentPage);
        }

        // Query data.
        AsyncResultSet selectRes = session.executeAsync(null, "SELECT VAL, ID, ID + 1 FROM TEST ORDER BY ID").join();

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
        AsyncResultSet updateRes = session.executeAsync(null, "UPDATE TEST SET VAL='upd' WHERE ID < 5").join();

        assertFalse(updateRes.wasApplied());
        assertFalse(updateRes.hasRowSet());
        assertEquals(5, updateRes.affectedRows());

        // Delete table.
        AsyncResultSet deleteRes = session.executeAsync(null, "DROP TABLE TEST").join();

        assertFalse(deleteRes.hasRowSet());
        assertTrue(deleteRes.wasApplied());
    }

    @Test
    void testFetchNextPage() {
        // TODO:
        // * Paging
        // * Close
        // * File tickets for everything else (remaining API methods, test coverage) - add to the epic
    }

    @Test
    void testInvalidSqlThrowsException() {
        // TODO:
    }
}
