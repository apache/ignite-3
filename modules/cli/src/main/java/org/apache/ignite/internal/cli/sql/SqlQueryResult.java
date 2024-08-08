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

import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.decorators.TableDecorator;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.jetbrains.annotations.Nullable;

/**
 * Composite object of sql query result.
 */
public class SqlQueryResult {
    @Nullable
    private final Table<String> table;

    private final String message;

    /**
     * Constructor.
     *
     * @param table non null result table.
     */
    public SqlQueryResult(@Nullable Table<String> table) {
        this(table, null);
    }

    /**
     * Constructor.
     *
     * @param message non null result message.
     */
    public SqlQueryResult(String message) {
        this(null, message);
    }

    private SqlQueryResult(@Nullable Table<String> table, String message) {
        this.table = table;
        this.message = message;
    }

    /**
     * SQL query result provider.
     *
     * @param tableDecorator instance of {@link TableDecorator}.
     * @param messageDecorator decorator of message.
     * @return terminal output of non-null field of class.
     */
    public TerminalOutput getResult(TableDecorator tableDecorator,
                                    Decorator<String, TerminalOutput> messageDecorator) {
        if (table != null) {
            return tableDecorator.decorate(table);
        }
        return messageDecorator.decorate(message);
    }
}
