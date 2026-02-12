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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

import com.jakewharton.fliptables.FlipTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StreamingTableRenderer}.
 */
class StreamingTableRendererTest extends BaseIgniteAbstractTest {

    @Test
    void headerAndFooterMatchFlipTableForSingleRow() {
        assertMatchesFlipTable(
                new String[]{"ID", "NAME"},
                new String[][]{{"1", "Alice"}}
        );
    }

    @Test
    void multipleRowsMatchFlipTable() {
        assertMatchesFlipTable(
                new String[]{"ID", "NAME"},
                new String[][]{{"1", "Alice"}, {"2", "Bob"}}
        );
    }

    @Test
    void singleColumnTable() {
        assertMatchesFlipTable(
                new String[]{"VALUE"},
                new String[][]{{"hello"}, {"world"}}
        );
    }

    @Test
    void manyColumns() {
        assertMatchesFlipTable(
                new String[]{"A", "B", "C", "D"},
                new String[][]{{"1", "22", "333", "4444"}, {"a", "bb", "ccc", "dddd"}}
        );
    }

    @Test
    void cellPaddingWithVaryingWidths() {
        assertMatchesFlipTable(
                new String[]{"X", "LONG_HEADER"},
                new String[][]{{"1", "short"}}
        );
    }

    @Test
    void cellTruncationToLockedWidth() {
        int[] widths = {3, 3};
        String[] headers = {"ID", "NAM"};
        StreamingTableRenderer renderer = new StreamingTableRenderer(headers, widths);

        // Row with content wider than locked width - should be truncated by pad()
        String row = renderer.renderRow(new Object[]{"1234", "Alice"}, true);
        // "1234" truncated to "123", "Alice" truncated to "Ali"
        assertThat(row, containsString(" 123 "));
        assertThat(row, containsString(" Ali "));
    }

    @Test
    void nullCellsRenderedAsEmpty() {
        int[] widths = {2, 4};
        String[] headers = {"ID", "NAME"};
        StreamingTableRenderer renderer = new StreamingTableRenderer(headers, widths);

        String row = renderer.renderRow(new Object[]{null, "test"}, true);
        // null renders as empty string padded to width; "test" fills its 4-char column exactly
        assertThat(row, containsString(" test "));
        // The null cell should produce empty content (only spaces between borders)
        // Row format: ║ <empty padded to 2> │ test ║
        assertThat(row, containsString("║    │"));
    }

    @Test
    void rowSeparatorOnlyBetweenRows() {
        int[] widths = {2};
        String[] headers = {"ID"};
        StreamingTableRenderer renderer = new StreamingTableRenderer(headers, widths);

        String firstRow = renderer.renderRow(new Object[]{"1"}, true);
        String secondRow = renderer.renderRow(new Object[]{"2"}, false);

        // First row has no separator line
        assertThat(firstRow.split("\n").length, equalTo(1));
        assertThat(firstRow, containsString(" 1 "));

        // Second row has separator + data (2 lines)
        assertThat(secondRow.split("\n").length, equalTo(2));
        // First line is the row separator with ─ chars
        assertThat(secondRow.split("\n")[0], containsString("─"));
        // Second line is the data row
        assertThat(secondRow.split("\n")[1], containsString(" 2 "));
    }

    @Test
    void headerContainsTopBorderAndSeparator() {
        int[] widths = {2, 4};
        String[] headers = {"ID", "NAME"};
        StreamingTableRenderer renderer = new StreamingTableRenderer(headers, widths);

        String header = renderer.renderHeader();
        String[] lines = header.split("\n");

        // Header has 3 lines: top border, header row, separator
        assertThat(lines.length, equalTo(3));

        // Top border starts with ╔ and ends with ╗
        assertThat(lines[0], startsWith("╔"));
        assertThat(lines[0].charAt(lines[0].length() - 1), equalTo('╗'));

        // Header row contains column names
        assertThat(lines[1], containsString("ID"));
        assertThat(lines[1], containsString("NAME"));

        // Separator starts with ╠ and ends with ╣
        assertThat(lines[2], startsWith("╠"));
        assertThat(lines[2].charAt(lines[2].length() - 1), equalTo('╣'));
    }

    @Test
    void footerIsBottomBorder() {
        int[] widths = {2, 4};
        String[] headers = {"ID", "NAME"};
        StreamingTableRenderer renderer = new StreamingTableRenderer(headers, widths);

        String footer = renderer.renderFooter();
        String line = footer.trim();

        // Bottom border starts with ╚ and ends with ╝
        assertThat(line, startsWith("╚"));
        assertThat(line.charAt(line.length() - 1), equalTo('╝'));
    }

    @Test
    void fullTableMatchesFlipTableWithThreeRows() {
        assertMatchesFlipTable(
                new String[]{"ID", "NAME", "AGE"},
                new String[][]{
                        {"1", "Alice", "30"},
                        {"2", "Bob", "25"},
                        {"3", "Charlie", "35"}
                }
        );
    }

    /**
     * Asserts that the streaming renderer produces the same output as FlipTable
     * when using FlipTable's own column widths.
     */
    private static void assertMatchesFlipTable(String[] headers, String[][] data) {
        String flipTableOutput = FlipTable.of(headers, data);
        int[] widths = widthsFromFlipTableOutput(flipTableOutput);
        String actual = renderAll(headers, data, widths);
        assertThat(actual, equalTo(flipTableOutput));
    }

    /**
     * Extracts column widths from FlipTable's rendered output by parsing the top border line.
     *
     * <p>Example: {@code ╔════╤═══════╗} produces widths [2, 5] (segment length minus 2 padding spaces per column).
     */
    private static int[] widthsFromFlipTableOutput(String flipTableOutput) {
        String topBorder = flipTableOutput.split("\n")[0];
        List<Integer> widths = new ArrayList<>();
        int segmentLen = 0;

        for (int i = 1; i < topBorder.length(); i++) {
            char c = topBorder.charAt(i);
            if (c == '╤' || c == '╗') {
                widths.add(segmentLen - 2); // subtract 2 padding spaces
                segmentLen = 0;
            } else {
                segmentLen++;
            }
        }
        return widths.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * Renders a full table using the streaming renderer and returns the combined output.
     */
    private static String renderAll(String[] headers, String[][] data, int[] widths) {
        String[] headersCopy = Arrays.copyOf(headers, headers.length);
        StreamingTableRenderer renderer = new StreamingTableRenderer(headersCopy, widths);
        StringBuilder sb = new StringBuilder();
        sb.append(renderer.renderHeader());
        for (int i = 0; i < data.length; i++) {
            sb.append(renderer.renderRow(data[i], i == 0));
        }
        sb.append(renderer.renderFooter());
        return sb.toString();
    }
}
