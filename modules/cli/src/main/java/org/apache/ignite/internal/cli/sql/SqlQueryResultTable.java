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

import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.decorators.TableDecorator;
import org.apache.ignite.internal.cli.decorators.TruncationConfig;
import org.apache.ignite.internal.cli.sql.table.Table;

/**
 * A table in the SQL query result.
 */
class SqlQueryResultTable implements SqlQueryResultItem {

    private final Table<String> table;

    SqlQueryResultTable(Table<String> table) {
        this.table = table;
    }

    @Override
    public TerminalOutput decorate(boolean plain, TruncationConfig truncationConfig) {
        TerminalOutput tableOutput = new TableDecorator(plain, truncationConfig).decorate(table);

        if (table.hasMoreRows()) {
            int rowCount = table.getRowCount();
            String rowWord = rowCount == 1 ? "row" : "rows";
            return () -> tableOutput.toTerminalString()
                    + "... (showing first " + rowCount + " " + rowWord + ", more available)\n";
        }
        return tableOutput;
    }
}
