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

/**
 * Streaming table renderer that produces FlipTable-compatible Unicode box-drawing output in phases.
 *
 * <p>Column widths are locked at construction time. Cells wider than the locked width are truncated.
 *
 * <p>Output format:
 * <pre>
 * ╔════╤═══════╗   ← renderHeader()
 * ║ ID │ NAME  ║
 * ╠════╪═══════╣
 * ║ 1  │ Alice ║   ← renderRow()
 * ╟────┼───────╢
 * ║ 2  │ Bob   ║   ← renderRow()
 * ╚════╧═══════╝   ← renderFooter()
 * </pre>
 */
public class StreamingTableRenderer {
    // Box-drawing characters matching FlipTable output.
    private static final char TOP_LEFT = '\u2554';      // ╔
    private static final char TOP_RIGHT = '\u2557';     // ╗
    private static final char BOTTOM_LEFT = '\u255A';   // ╚
    private static final char BOTTOM_RIGHT = '\u255D';  // ╝
    private static final char HORIZONTAL_DBL = '\u2550'; // ═
    private static final char VERTICAL_DBL = '\u2551';  // ║
    private static final char TOP_TEE = '\u2564';       // ╤
    private static final char BOTTOM_TEE = '\u2567';    // ╧
    private static final char HDR_LEFT = '\u2560';      // ╠
    private static final char HDR_RIGHT = '\u2563';     // ╣
    private static final char HDR_CROSS = '\u256A';     // ╪
    private static final char ROW_LEFT = '\u255F';      // ╟
    private static final char ROW_RIGHT = '\u2562';     // ╢
    private static final char HORIZONTAL = '\u2500';    // ─
    private static final char VERTICAL = '\u2502';      // │
    private static final char ROW_CROSS = '\u253C';     // ┼

    private final String[] columnNames;
    private final int[] columnWidths;

    /**
     * Creates a new streaming table renderer.
     *
     * @param columnNames column header names (already truncated to fit widths)
     * @param columnWidths locked column widths
     */
    public StreamingTableRenderer(String[] columnNames, int[] columnWidths) {
        this.columnNames = columnNames;
        this.columnWidths = columnWidths;
    }

    /**
     * Renders the table header: top border, header row, and header separator.
     *
     * @return header string
     */
    public String renderHeader() {
        StringBuilder sb = new StringBuilder();
        // Top border: ╔════╤═══════╗
        appendBorderLine(sb, TOP_LEFT, HORIZONTAL_DBL, TOP_TEE, TOP_RIGHT);
        sb.append('\n');
        // Header row: ║ ID │ NAME  ║
        appendDataRow(sb, columnNames);
        sb.append('\n');
        // Header separator: ╠════╪═══════╣
        appendBorderLine(sb, HDR_LEFT, HORIZONTAL_DBL, HDR_CROSS, HDR_RIGHT);
        sb.append('\n');
        return sb.toString();
    }

    /**
     * Renders a single data row, with a row separator before it if it's not the first row.
     *
     * @param row cell values (already truncated to fit widths)
     * @param isFirst whether this is the first data row (no separator before it)
     * @return row string
     */
    public String renderRow(Object[] row, boolean isFirst) {
        StringBuilder sb = new StringBuilder();
        if (!isFirst) {
            // Row separator: ╟────┼───────╢
            appendBorderLine(sb, ROW_LEFT, HORIZONTAL, ROW_CROSS, ROW_RIGHT);
            sb.append('\n');
        }
        // Data row: ║ v  │ v     ║
        appendDataRow(sb, row);
        sb.append('\n');
        return sb.toString();
    }

    /**
     * Renders the table footer (bottom border).
     *
     * @return footer string
     */
    public String renderFooter() {
        StringBuilder sb = new StringBuilder();
        // Bottom border: ╚════╧═══════╝
        appendBorderLine(sb, BOTTOM_LEFT, HORIZONTAL_DBL, BOTTOM_TEE, BOTTOM_RIGHT);
        sb.append('\n');
        return sb.toString();
    }

    private void appendBorderLine(StringBuilder sb, char left, char fill, char cross, char right) {
        sb.append(left);
        for (int i = 0; i < columnWidths.length; i++) {
            if (i > 0) {
                sb.append(cross);
            }
            // Each column cell has: space + content + space = width + 2
            int cellWidth = columnWidths[i] + 2;
            for (int j = 0; j < cellWidth; j++) {
                sb.append(fill);
            }
        }
        sb.append(right);
    }

    private void appendDataRow(StringBuilder sb, Object[] row) {
        sb.append(VERTICAL_DBL);
        for (int i = 0; i < columnWidths.length; i++) {
            if (i > 0) {
                sb.append(VERTICAL);
            }
            String value = cellToString(row, i);
            sb.append(' ').append(pad(value, columnWidths[i])).append(' ');
        }
        sb.append(VERTICAL_DBL);
    }

    private static String cellToString(Object[] row, int index) {
        if (index >= row.length || row[index] == null) {
            return "";
        }
        return String.valueOf(row[index]);
    }

    private static String pad(String value, int width) {
        if (value.length() >= width) {
            return value.substring(0, width);
        }
        StringBuilder sb = new StringBuilder(width);
        sb.append(value);
        for (int i = value.length(); i < width; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }
}
