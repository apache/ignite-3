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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.sql.table.Table;

/**
 * Composite object of sql query result.
 */
public class SqlQueryResult {
    private final List<SqlQueryResultItem> sqlQueryResultItems;
    private final long durationMs;

    private SqlQueryResult(List<SqlQueryResultItem> sqlQueryResultItems, long durationMs) {
        this.sqlQueryResultItems = sqlQueryResultItems;
        this.durationMs = durationMs;
    }

    /**
     * SQL query result provider.
     *
     * @param plain Whether to use plain formatting.
     * @param timed Whether to include execution time in output.
     * @return terminal output all items in query result.
     */
    public TerminalOutput getResult(boolean plain, boolean timed) {
        return () -> {
            String result = sqlQueryResultItems.stream()
                    .map(x -> x.decorate(plain).toTerminalString())
                    .collect(Collectors.joining(""));
            if (timed) {
                result += "Query executed in " + durationMs + "ms (client-side).\n";
            }
            return result;
        };
    }

    /**
     * SQL query result provider.
     *
     * @return terminal output all items in query result.
     */
    public TerminalOutput getResult(boolean plain) {
        return getResult(plain, false);
    }

    /**
     * Builder for {@link SqlQueryResult}.
     */
    static class SqlQueryResultBuilder {
        private final List<SqlQueryResultItem> sqlQueryResultItems = new ArrayList<>();
        private long durationMs;

        /**
         * Add table to query result.
         */
        void addTable(Table<String> table) {
            sqlQueryResultItems.add(new SqlQueryResultTable(table));
        }

        /**
         * Add message to query result.
         */
        void addMessage(String message) {
            sqlQueryResultItems.add(new SqlQueryResultMessage(message + "\n"));
        }

        /**
         * Set the query execution duration in milliseconds.
         */
        void setDurationMs(long durationMs) {
            this.durationMs = durationMs;
        }

        public SqlQueryResult build() {
            return new SqlQueryResult(sqlQueryResultItems, durationMs);
        }
    }
}
