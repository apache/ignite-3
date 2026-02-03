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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.decorators.TruncationConfig;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.junit.jupiter.api.Test;

class SqlQueryResultTableTest {

    @Test
    void decorateWithoutTruncation() {
        Table<String> table = new Table<>(List.of("col1"), List.of("value1", "value2"), false);
        SqlQueryResultTable resultTable = new SqlQueryResultTable(table);

        TruncationConfig noTruncation = TruncationConfig.disabled();
        TerminalOutput output = resultTable.decorate(true, noTruncation);

        String result = output.toTerminalString();
        assertFalse(result.contains("rows shown"), "Should not contain truncation indicator");
    }

    @Test
    void decorateWithTruncation() {
        Table<String> table = new Table<>(List.of("col1"), List.of("value1", "value2", "value3"), true);
        SqlQueryResultTable resultTable = new SqlQueryResultTable(table);

        TruncationConfig noTruncation = TruncationConfig.disabled();
        TerminalOutput output = resultTable.decorate(true, noTruncation);

        String result = output.toTerminalString();
        assertTrue(result.contains("-- 3 rows shown (more available, use interactive mode to load more) --"),
                "Should contain truncation indicator with row count 3");
    }

    @Test
    void decorateWithSingleRowTruncation() {
        Table<String> table = new Table<>(List.of("col1"), List.of("value1"), true);
        SqlQueryResultTable resultTable = new SqlQueryResultTable(table);

        TruncationConfig noTruncation = TruncationConfig.disabled();
        TerminalOutput output = resultTable.decorate(true, noTruncation);

        String result = output.toTerminalString();
        assertTrue(result.contains("-- 1 row shown (more available, use interactive mode to load more) --"),
                "Should use singular 'row' for single row");
    }
}
