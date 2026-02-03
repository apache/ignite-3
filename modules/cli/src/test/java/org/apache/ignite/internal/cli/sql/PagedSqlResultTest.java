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

package org.apache.ignite.internal.cli.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Unit tests for {@link PagedSqlResult}.
 */
class PagedSqlResultTest {

    @Test
    void testFetchSinglePage() throws SQLException {
        // Create mock with 5 rows, page size 10
        Statement stmt = createMockStatement(5, 2);

        try (PagedSqlResult result = new PagedSqlResult(stmt, 10, System.currentTimeMillis())) {
            assertTrue(result.hasResultSet());

            Table<String> page1 = result.fetchNextPage();
            assertNotNull(page1);
            assertEquals(5, page1.getRowCount());
            assertFalse(page1.hasMoreRows(), "Should not have more rows when all fit in one page");

            Table<String> page2 = result.fetchNextPage();
            assertNull(page2, "Should be null when no more pages");

            assertFalse(result.hasMoreRows());
            assertEquals(5, result.getTotalRowsFetched());
        }
    }

    @Test
    void testFetchMultiplePages() throws SQLException {
        // Create mock with 25 rows, page size 10
        Statement stmt = createMockStatement(25, 2);

        try (PagedSqlResult result = new PagedSqlResult(stmt, 10, System.currentTimeMillis())) {
            assertTrue(result.hasResultSet());

            // Page 1: 10 rows
            Table<String> page1 = result.fetchNextPage();
            assertNotNull(page1);
            assertEquals(10, page1.getRowCount());
            assertTrue(page1.hasMoreRows(), "Should have more rows");
            assertEquals(10, result.getTotalRowsFetched());

            // Page 2: 10 rows
            Table<String> page2 = result.fetchNextPage();
            assertNotNull(page2);
            assertEquals(10, page2.getRowCount());
            assertTrue(page2.hasMoreRows(), "Should have more rows");
            assertEquals(20, result.getTotalRowsFetched());

            // Page 3: 5 rows (last page)
            Table<String> page3 = result.fetchNextPage();
            assertNotNull(page3);
            assertEquals(5, page3.getRowCount());
            assertFalse(page3.hasMoreRows(), "Last page should not have more rows");
            assertEquals(25, result.getTotalRowsFetched());

            // No more pages
            Table<String> page4 = result.fetchNextPage();
            assertNull(page4);
            assertFalse(result.hasMoreRows());
        }
    }

    @Test
    void testExactPageBoundary() throws SQLException {
        // Create mock with exactly 20 rows, page size 10
        Statement stmt = createMockStatement(20, 2);

        try (PagedSqlResult result = new PagedSqlResult(stmt, 10, System.currentTimeMillis())) {
            // Page 1: 10 rows
            Table<String> page1 = result.fetchNextPage();
            assertNotNull(page1);
            assertEquals(10, page1.getRowCount());
            assertTrue(page1.hasMoreRows());

            // Page 2: 10 rows (exactly at boundary)
            Table<String> page2 = result.fetchNextPage();
            assertNotNull(page2);
            assertEquals(10, page2.getRowCount());
            assertFalse(page2.hasMoreRows(), "Last page at exact boundary should not have more rows");

            // No more pages
            assertNull(result.fetchNextPage());
            assertEquals(20, result.getTotalRowsFetched());
        }
    }

    @Test
    void testSingleRowPage() throws SQLException {
        // Create mock with 3 rows, page size 1
        Statement stmt = createMockStatement(3, 2);

        try (PagedSqlResult result = new PagedSqlResult(stmt, 1, System.currentTimeMillis())) {
            Table<String> page1 = result.fetchNextPage();
            assertEquals(1, page1.getRowCount());
            assertTrue(page1.hasMoreRows());

            Table<String> page2 = result.fetchNextPage();
            assertEquals(1, page2.getRowCount());
            assertTrue(page2.hasMoreRows());

            Table<String> page3 = result.fetchNextPage();
            assertEquals(1, page3.getRowCount());
            assertFalse(page3.hasMoreRows());

            assertNull(result.fetchNextPage());
            assertEquals(3, result.getTotalRowsFetched());
        }
    }

    @Test
    void testEmptyResultSet() throws SQLException {
        // Create mock with 0 rows
        Statement stmt = createMockStatement(0, 2);

        try (PagedSqlResult result = new PagedSqlResult(stmt, 10, System.currentTimeMillis())) {
            assertTrue(result.hasResultSet());

            Table<String> page = result.fetchNextPage();
            assertNull(page, "Should return null for empty result set");

            assertEquals(0, result.getTotalRowsFetched());
            assertFalse(result.hasMoreRows());
        }
    }

    @Test
    void testUpdateCount() throws SQLException {
        Statement stmt = mock(Statement.class);
        when(stmt.getResultSet()).thenReturn(null);
        when(stmt.getUpdateCount()).thenReturn(42);

        try (PagedSqlResult result = new PagedSqlResult(stmt, 10, System.currentTimeMillis())) {
            assertFalse(result.hasResultSet(), "Update statement should not have result set");
            assertEquals(42, result.getUpdateCount());
        }
    }

    @Test
    void testColumnNames() throws SQLException {
        Statement stmt = createMockStatement(5, 3);

        try (PagedSqlResult result = new PagedSqlResult(stmt, 10, System.currentTimeMillis())) {
            List<String> columnNames = result.getColumnNames();
            assertNotNull(columnNames);
            assertEquals(3, columnNames.size());
            assertEquals("col1", columnNames.get(0));
            assertEquals("col2", columnNames.get(1));
            assertEquals("col3", columnNames.get(2));
        }
    }

    @Test
    void testDurationTracking() throws SQLException {
        long startTime = System.currentTimeMillis();
        Statement stmt = createMockStatement(5, 2);

        try (PagedSqlResult result = new PagedSqlResult(stmt, 10, startTime)) {
            // Sleep a bit to ensure time passes
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            long duration = result.getDurationMs();
            assertTrue(duration >= 10, "Duration should be at least 10ms");
        }
    }

    /**
     * Creates a mock Statement with a ResultSet containing the specified number of rows and columns.
     */
    private Statement createMockStatement(int numRows, int numCols) throws SQLException {
        Statement stmt = mock(Statement.class);
        ResultSet rs = createMockResultSet(numRows, numCols);

        when(stmt.getResultSet()).thenReturn(rs);
        when(stmt.getMoreResults()).thenReturn(false);
        when(stmt.getUpdateCount()).thenReturn(-1);

        return stmt;
    }

    /**
     * Creates a mock ResultSet with the specified number of rows and columns.
     */
    private ResultSet createMockResultSet(int numRows, int numCols) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(rs.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(numCols);

        for (int i = 1; i <= numCols; i++) {
            when(metaData.getColumnLabel(i)).thenReturn("col" + i);
        }

        // Track current row for getString calls
        final int[] currentRow = {0};
        final List<Boolean> nextResults = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            nextResults.add(true);
        }
        nextResults.add(false); // Last call returns false

        when(rs.next()).thenAnswer(new Answer<Boolean>() {
            private int callCount = 0;

            @Override
            public Boolean answer(InvocationOnMock invocation) {
                if (callCount >= nextResults.size()) {
                    return false;
                }
                boolean result = nextResults.get(callCount);
                if (result) {
                    currentRow[0]++;
                }
                callCount++;
                return result;
            }
        });

        // Mock getString to return "row{rowNum}_col{colNum}"
        when(rs.getString(org.mockito.ArgumentMatchers.anyInt())).thenAnswer(invocation -> {
            int colIndex = invocation.getArgument(0);
            return "row" + currentRow[0] + "_col" + colIndex;
        });

        return rs;
    }
}
