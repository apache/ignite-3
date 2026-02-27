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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.jetbrains.annotations.Nullable;

/**
 * A paged SQL result that allows fetching rows in batches.
 * This class manages the lifecycle of the underlying Statement and ResultSet,
 * enabling lazy loading of result pages.
 */
public class PagedSqlResult implements AutoCloseable {
    private final Statement statement;
    private final int pageSize;
    private final long startTime;

    private @Nullable ResultSet currentResultSet;
    private @Nullable List<String> columnNames;
    private int columnCount;
    private boolean exhausted;
    private int totalRowsFetched;
    /** Tracks if the cursor is already positioned on a row that hasn't been read yet. */
    private boolean hasPendingRow;

    /**
     * Constructor.
     *
     * @param statement The executed statement.
     * @param pageSize The page size for fetching rows.
     * @param startTime The query start time for duration calculation.
     */
    PagedSqlResult(Statement statement, int pageSize, long startTime) throws SQLException {
        this.statement = statement;
        this.pageSize = pageSize;
        this.startTime = startTime;
        this.currentResultSet = statement.getResultSet();
        this.exhausted = false;
        this.totalRowsFetched = 0;

        if (currentResultSet != null) {
            initColumnMetadata();
        }
    }

    private void initColumnMetadata() throws SQLException {
        ResultSetMetaData metaData = currentResultSet.getMetaData();
        columnCount = metaData.getColumnCount();
        columnNames = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            columnNames.add(metaData.getColumnLabel(i));
        }
    }

    /**
     * Checks if this result has a result set (SELECT query) or is an update result.
     *
     * @return true if this is a SELECT result with rows.
     */
    public boolean hasResultSet() {
        return currentResultSet != null;
    }

    /**
     * Gets the update count for non-SELECT queries.
     *
     * @return The update count, or -1 if not applicable.
     */
    public int getUpdateCount() throws SQLException {
        return statement.getUpdateCount();
    }

    /**
     * Fetches the next page of rows.
     *
     * @return A Table containing the next page of rows, or null if no more rows are available.
     */
    public @Nullable Table<String> fetchNextPage() throws SQLException {
        if (exhausted || currentResultSet == null) {
            return null;
        }

        List<String> content = new ArrayList<>();
        int rowsRead = 0;
        boolean hasMoreRows = false;

        while (true) {
            // Check if we have a row from a previous peek, otherwise advance the cursor
            boolean hasRow;
            if (hasPendingRow) {
                hasRow = true;
                hasPendingRow = false;
            } else {
                hasRow = currentResultSet.next();
            }

            if (!hasRow) {
                break;
            }

            if (rowsRead >= pageSize) {
                // We have a row but already read enough - mark it as pending for next call
                hasMoreRows = true;
                hasPendingRow = true;
                break;
            }

            for (int i = 1; i <= columnCount; i++) {
                content.add(currentResultSet.getString(i));
            }
            rowsRead++;
            totalRowsFetched++;
        }

        if (rowsRead == 0) {
            // No more rows in this result set, check for more result sets
            if (statement.getMoreResults()) {
                currentResultSet = statement.getResultSet();
                if (currentResultSet != null) {
                    initColumnMetadata();
                    hasPendingRow = false;
                    return fetchNextPage();
                }
            }
            exhausted = true;
            return null;
        }

        return new Table<>(columnNames, content, hasMoreRows);
    }

    /**
     * Checks if there are potentially more rows to fetch.
     *
     * @return true if there might be more rows available.
     */
    public boolean hasMoreRows() {
        return !exhausted;
    }

    /**
     * Gets the total number of rows fetched so far.
     *
     * @return The total row count.
     */
    public int getTotalRowsFetched() {
        return totalRowsFetched;
    }

    /**
     * Gets the query duration in milliseconds.
     *
     * @return Duration since query start.
     */
    public long getDurationMs() {
        return System.currentTimeMillis() - startTime;
    }

    /**
     * Gets the column names for the current result set.
     *
     * @return List of column names.
     */
    public @Nullable List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }
}
