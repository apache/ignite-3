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

import static org.apache.ignite.internal.cli.decorators.TruncationConfig.ELLIPSIS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.cli.decorators.TruncationConfig;
import org.apache.ignite.internal.cli.sql.table.Table;

/**
 * Truncates table columns to fit terminal width.
 */
public class TableTruncator {
    /** Minimum column width to display at least one character plus ellipsis. */
    private static final int MIN_COLUMN_WIDTH = ELLIPSIS.length() + 1;

    /**
     * Per-column overhead in FlipTables: left padding + right padding + separator/border.
     *
     * <p>Example for 2 columns (c1, c2):
     * <pre>
     * ║ c1 │ c2 ║
     * </pre>
     * Each column has: 1 space before + 1 space after + 1 border char = 3 chars overhead.
     */
    private static final int BORDER_OVERHEAD_PER_COLUMN = 3;

    /**
     * Fixed table border overhead (the extra left border character).
     *
     * <p>Total overhead calculation for N columns:
     * <pre>
     * ║ c1 │ c2 │ ... │ cN ║
     * ^    ^    ^          ^
     * 1    3    3          3  (per-column overhead includes trailing separator)
     * </pre>
     * The left border (║) is not counted in per-column overhead, so we add 1.
     * Total overhead = 1 + 3*N.
     */
    private static final int TABLE_BORDER_OVERHEAD = 1;

    private final TruncationConfig config;

    /**
     * Creates a new TableTruncator with the given configuration.
     *
     * @param config truncation configuration
     */
    public TableTruncator(TruncationConfig config) {
        this.config = config;
    }

    /**
     * Truncates table columns based on the truncation configuration.
     *
     * @param table the table to truncate
     * @return a new table with truncated values, or the original table if truncation is disabled
     */
    public Table<String> truncate(Table<String> table) {
        if (!config.isTruncateEnabled()) {
            return table;
        }

        String[] header = table.header();
        Object[][] content = table.content();

        if (header.length == 0) {
            return table;
        }

        int[] columnWidths = calculateColumnWidths(header, content);
        String[] truncatedHeader = truncateRow(header, columnWidths);
        List<String> truncatedContent = truncateContent(content, columnWidths);

        return new Table<>(Arrays.asList(truncatedHeader), truncatedContent);
    }

    /**
     * Calculates optimal column widths based on content and terminal constraints.
     *
     * @param header table header
     * @param content table content
     * @return array of column widths
     */
    int[] calculateColumnWidths(String[] header, Object[][] content) {
        int columnCount = header.length;
        int[] maxContentWidths = new int[columnCount];

        // Calculate maximum content width for each column
        for (int col = 0; col < columnCount; col++) {
            maxContentWidths[col] = Math.max(maxContentWidths[col], stringLength(header[col]));
            for (Object[] row : content) {
                maxContentWidths[col] = Math.max(maxContentWidths[col], stringLength(row[col]));
            }
        }

        int[] columnWidths = new int[columnCount];
        int maxColumnWidth = config.getMaxColumnWidth();
        int terminalWidth = config.getTerminalWidth();

        // Apply max column width constraint
        for (int col = 0; col < columnCount; col++) {
            columnWidths[col] = Math.min(maxContentWidths[col], maxColumnWidth);
            columnWidths[col] = Math.max(columnWidths[col], MIN_COLUMN_WIDTH);
        }

        // Apply terminal width constraint if set
        if (terminalWidth > 0) {
            distributeWidthsForTerminal(columnWidths, terminalWidth);
        }

        return columnWidths;
    }

    /**
     * Distributes column widths to fit within terminal width.
     *
     * @param columnWidths column widths to adjust (modified in place)
     * @param terminalWidth terminal width constraint
     */
    private static void distributeWidthsForTerminal(int[] columnWidths, int terminalWidth) {
        int columnCount = columnWidths.length;
        int totalBorderOverhead = TABLE_BORDER_OVERHEAD + (columnCount * BORDER_OVERHEAD_PER_COLUMN);
        int availableWidth = terminalWidth - totalBorderOverhead;

        if (availableWidth <= 0) {
            // Terminal too narrow, use minimum widths
            Arrays.fill(columnWidths, MIN_COLUMN_WIDTH);
            return;
        }

        int totalCurrentWidth = Arrays.stream(columnWidths).sum();

        if (totalCurrentWidth <= availableWidth) {
            // Everything fits, no adjustment needed
            return;
        }

        // Need to shrink columns proportionally
        // First, calculate how much we need to reduce
        int excessWidth = totalCurrentWidth - availableWidth;

        // Calculate total shrinkable width (columns that are above minimum)
        int[] shrinkableWidth = new int[columnCount];
        int totalShrinkable = 0;
        for (int col = 0; col < columnCount; col++) {
            shrinkableWidth[col] = columnWidths[col] - MIN_COLUMN_WIDTH;
            totalShrinkable += shrinkableWidth[col];
        }

        if (totalShrinkable <= 0) {
            // All columns at minimum, can't shrink further
            return;
        }

        // Shrink proportionally using integer division to avoid over-shrinking
        int totalReduction = 0;
        for (int col = 0; col < columnCount; col++) {
            if (shrinkableWidth[col] > 0) {
                int reduction = excessWidth * shrinkableWidth[col] / totalShrinkable;
                reduction = Math.min(reduction, shrinkableWidth[col]);
                columnWidths[col] -= reduction;
                totalReduction += reduction;
            }
        }

        // Distribute any remaining excess (due to integer truncation) one column at a time
        int remaining = excessWidth - totalReduction;
        for (int col = 0; col < columnCount && remaining > 0; col++) {
            if (columnWidths[col] > MIN_COLUMN_WIDTH) {
                columnWidths[col]--;
                remaining--;
            }
        }
    }

    /**
     * Truncates a row of values to fit the specified column widths.
     *
     * @param row row values
     * @param columnWidths column widths
     * @return truncated row values
     */
    private static String[] truncateRow(String[] row, int[] columnWidths) {
        String[] result = new String[row.length];
        for (int i = 0; i < row.length; i++) {
            int maxWidth = i < columnWidths.length ? columnWidths[i] : Integer.MAX_VALUE;
            result[i] = truncateCell(row[i], maxWidth);
        }
        return result;
    }

    /**
     * Truncates all content rows to fit the specified column widths.
     *
     * @param content table content
     * @param columnWidths column widths
     * @return flat list of truncated values
     */
    private static List<String> truncateContent(Object[][] content, int[] columnWidths) {
        List<String> result = new ArrayList<>();
        for (Object[] row : content) {
            for (int col = 0; col < row.length; col++) {
                int maxWidth = col < columnWidths.length ? columnWidths[col] : Integer.MAX_VALUE;
                result.add(truncateCell(row[col], maxWidth));
            }
        }
        return result;
    }

    /**
     * Truncates a single cell value to fit the specified width.
     *
     * @param value cell value
     * @param maxWidth maximum width
     * @return truncated value
     */
    static String truncateCell(Object value, int maxWidth) {
        if (value == null) {
            return "null";
        }

        String str = String.valueOf(value);

        if (str.length() <= maxWidth) {
            return str;
        }

        if (maxWidth <= ELLIPSIS.length()) {
            return ELLIPSIS.substring(0, maxWidth);
        }

        return str.substring(0, maxWidth - ELLIPSIS.length()) + ELLIPSIS;
    }

    /**
     * Returns the display length of a value.
     *
     * @param value value to measure
     * @return length of string representation
     */
    private static int stringLength(Object value) {
        if (value == null) {
            return 4; // "null"
        }
        return String.valueOf(value).length();
    }
}
