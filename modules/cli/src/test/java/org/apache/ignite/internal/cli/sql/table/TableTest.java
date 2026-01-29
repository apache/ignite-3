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

package org.apache.ignite.internal.cli.sql.table;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TableTest extends BaseIgniteAbstractTest {

    @Test
    public void headerTest() {
        Table<String> table = new Table<>(List.of("foo", "bar"), List.of("1", "2"));
        Assertions.assertArrayEquals(table.header(), new Object[] {"foo", "bar"});
    }

    @Test
    public void contentTest() {
        Table<String> table = new Table<>(List.of("foo", "bar"), List.of("1", "2"));
        Assertions.assertArrayEquals(table.content(), new Object[][]{new Object[]{"1", "2"}});
    }

    @Test
    public void emptyTableTest() {
        Table<String> table = new Table<>(Collections.emptyList(), Collections.emptyList());
        Assertions.assertArrayEquals(table.header(), new String[] { "EMPTY" });
        Assertions.assertArrayEquals(table.content(), new Object[0][0]);
    }

    @Test
    public void rowCountTest() {
        Table<String> table = new Table<>(List.of("col1", "col2"), List.of("a", "b", "c", "d"));
        Assertions.assertEquals(2, table.getRowCount());
        Assertions.assertFalse(table.hasMoreRows());
    }

    @Test
    public void hasMoreRowsTrueTest() {
        Table<String> table = new Table<>(List.of("col1"), List.of("a", "b", "c"), true);
        Assertions.assertEquals(3, table.getRowCount());
        Assertions.assertTrue(table.hasMoreRows());
    }

    @Test
    public void hasMoreRowsFalseTest() {
        Table<String> table = new Table<>(List.of("col1"), List.of("a", "b", "c"), false);
        Assertions.assertEquals(3, table.getRowCount());
        Assertions.assertFalse(table.hasMoreRows());
    }

    @Test
    public void fromResultSetWithoutLimit() throws SQLException {
        ResultSet rs = createMockResultSet(5, 2);

        Table<String> table = Table.fromResultSet(rs, 0);

        Assertions.assertEquals(5, table.getRowCount());
        Assertions.assertFalse(table.hasMoreRows());
        Assertions.assertArrayEquals(new String[]{"col1", "col2"}, table.header());
    }

    @Test
    public void fromResultSetWithLimitLessThanRows() throws SQLException {
        ResultSet rs = createMockResultSet(10, 2);

        Table<String> table = Table.fromResultSet(rs, 3);

        Assertions.assertEquals(3, table.getRowCount());
        Assertions.assertTrue(table.hasMoreRows(), "Should indicate more rows available");
        Assertions.assertArrayEquals(new String[]{"col1", "col2"}, table.header());
    }

    @Test
    public void fromResultSetWithLimitEqualToRows() throws SQLException {
        ResultSet rs = createMockResultSet(5, 2);

        Table<String> table = Table.fromResultSet(rs, 5);

        Assertions.assertEquals(5, table.getRowCount());
        Assertions.assertFalse(table.hasMoreRows(), "Should not indicate more rows when exactly at limit");
    }

    @Test
    public void fromResultSetWithLimitGreaterThanRows() throws SQLException {
        ResultSet rs = createMockResultSet(3, 2);

        Table<String> table = Table.fromResultSet(rs, 100);

        Assertions.assertEquals(3, table.getRowCount());
        Assertions.assertFalse(table.hasMoreRows(), "Should not indicate more rows when limit exceeds data");
    }

    @Test
    public void fromResultSetWithLimitOne() throws SQLException {
        ResultSet rs = createMockResultSet(100, 1);

        Table<String> table = Table.fromResultSet(rs, 1);

        Assertions.assertEquals(1, table.getRowCount());
        Assertions.assertTrue(table.hasMoreRows(), "Should indicate more rows available");
    }

    @Test
    public void fromResultSetEmptyResultSet() throws SQLException {
        ResultSet rs = createMockResultSet(0, 2);

        Table<String> table = Table.fromResultSet(rs, 10);

        Assertions.assertEquals(0, table.getRowCount());
        Assertions.assertFalse(table.hasMoreRows());
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

        // Track current row position
        AtomicInteger currentRow = new AtomicInteger(0);

        when(rs.next()).thenAnswer(invocation -> {
            int row = currentRow.incrementAndGet();
            return row <= numRows;
        });

        // Return cell values based on row/column
        for (int col = 1; col <= numCols; col++) {
            final int colNum = col;
            when(rs.getString(colNum)).thenAnswer(invocation -> {
                int row = currentRow.get();
                return "r" + row + "c" + colNum;
            });
        }

        return rs;
    }
}
