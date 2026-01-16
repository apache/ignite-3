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

package org.apache.ignite.internal.cli.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.cli.decorators.TruncationConfig;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TableTruncator}.
 */
class TableTruncatorTest {

    @Test
    void truncateDisabledReturnsOriginalTable() {
        Table<String> table = createTable(
                List.of("id", "name"),
                List.of("1", "Alice", "2", "Bob")
        );
        TruncationConfig config = TruncationConfig.disabled();

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result, sameInstance(table));
    }

    @Test
    void truncateCellWithinLimit() {
        TableTruncator truncator = new TableTruncator(TruncationConfig.disabled());

        String result = truncator.truncateCell("short", 10);

        assertThat(result, equalTo("short"));
    }

    @Test
    void truncateCellExceedsLimit() {
        TableTruncator truncator = new TableTruncator(TruncationConfig.disabled());

        String result = truncator.truncateCell("very long text that exceeds the limit", 10);

        assertThat(result, equalTo("very lo..."));
    }

    @Test
    void truncateCellExactlyAtLimit() {
        TableTruncator truncator = new TableTruncator(TruncationConfig.disabled());

        String result = truncator.truncateCell("1234567890", 10);

        assertThat(result, equalTo("1234567890"));
    }

    @Test
    void truncateCellNullValue() {
        TableTruncator truncator = new TableTruncator(TruncationConfig.disabled());

        String result = truncator.truncateCell(null, 10);

        assertThat(result, equalTo("null"));
    }

    @Test
    void truncateCellMinimumWidth() {
        TableTruncator truncator = new TableTruncator(TruncationConfig.disabled());

        String result = truncator.truncateCell("hello", 3);

        assertThat(result, equalTo("..."));
    }

    @Test
    void truncateCellWidthLessThanEllipsis() {
        TableTruncator truncator = new TableTruncator(TruncationConfig.disabled());

        String result = truncator.truncateCell("hello", 2);

        assertThat(result, equalTo(".."));
    }

    @Test
    void truncateTableAppliesMaxColumnWidth() {
        Table<String> table = createTable(
                List.of("id", "description"),
                List.of("1", "This is a very long description that should be truncated")
        );
        TruncationConfig config = new TruncationConfig(true, 20, 0);

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result.header()[1], equalTo("description"));
        assertThat(result.content()[0][1], equalTo("This is a very lo..."));
    }

    @Test
    void truncateTablePreservesShortColumns() {
        Table<String> table = createTable(
                List.of("id", "name", "description"),
                List.of("1", "Alice", "Short desc")
        );
        TruncationConfig config = new TruncationConfig(true, 20, 0);

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result.content()[0][0], equalTo("1"));
        assertThat(result.content()[0][1], equalTo("Alice"));
        assertThat(result.content()[0][2], equalTo("Short desc"));
    }

    @Test
    void truncateEmptyTableReturnsOriginal() {
        Table<String> table = createTable(List.of("id"), List.of());
        TruncationConfig config = new TruncationConfig(true, 20, 80);

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result.header().length, is(1));
        assertThat(result.content().length, is(0));
    }

    @Test
    void calculateColumnWidthsRespectsMaxColumnWidth() {
        String[] header = {"id", "description"};
        Object[][] content = {{"1", "This is a long description"}};
        TruncationConfig config = new TruncationConfig(true, 15, 0);

        int[] widths = new TableTruncator(config).calculateColumnWidths(header, content);

        assertThat(widths[0], is(3)); // "..." minimum or actual length
        assertThat(widths[1], is(15)); // capped at max column width
    }

    @Test
    void calculateColumnWidthsDistributesForTerminal() {
        String[] header = {"col1", "col2", "col3"};
        Object[][] content = {
                {"very long content 1", "very long content 2", "very long content 3"}
        };
        // Terminal width = 50, with overhead for borders
        TruncationConfig config = new TruncationConfig(true, 100, 50);

        int[] widths = new TableTruncator(config).calculateColumnWidths(header, content);

        // Total width should fit within terminal width
        int totalWidth = Arrays.stream(widths).sum();
        // Account for borders: 2 (outer) + 3 * 3 (column separators) = 11
        assertThat(totalWidth <= 50 - 11, is(true));
    }

    @Test
    void truncateTableWithMultipleRows() {
        Table<String> table = createTable(
                List.of("id", "name"),
                List.of(
                        "1", "Alice with a very long name",
                        "2", "Bob",
                        "3", "Charlie with another long name"
                )
        );
        TruncationConfig config = new TruncationConfig(true, 10, 0);

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result.content()[0][1], equalTo("Alice w..."));
        assertThat(result.content()[1][1], equalTo("Bob"));
        assertThat(result.content()[2][1], equalTo("Charlie..."));
    }

    @Test
    void truncatePreservesHeaderWhenWidthIsSmall() {
        Table<String> table = createTable(
                List.of("very_long_header_name"),
                List.of("short")
        );
        TruncationConfig config = new TruncationConfig(true, 10, 0);

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result.header()[0], equalTo("very_lo..."));
    }

    private static Table<String> createTable(List<String> headers, List<String> content) {
        return new Table<>(headers, content);
    }
}
