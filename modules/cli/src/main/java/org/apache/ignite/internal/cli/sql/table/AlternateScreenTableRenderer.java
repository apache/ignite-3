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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.cli.decorators.TruncationConfig;
import org.apache.ignite.internal.cli.util.TableTruncator;
import org.jline.terminal.Terminal;
import org.jline.terminal.Terminal.Signal;
import org.jline.terminal.Terminal.SignalHandler;

/**
 * Renders SQL result tables using the ANSI alternate screen buffer.
 *
 * <p>When the terminal is interactive, this renderer:
 * <ol>
 *   <li>Enters the alternate screen buffer and renders the header at the top (rows 1-3)</li>
 *   <li>Sets a scroll region below the header so the header stays fixed</li>
 *   <li>Streams data rows into the scroll region as pages arrive</li>
 *   <li>When wider data arrives, recalculates column widths and re-renders everything</li>
 *   <li>When done, shows a status line and waits for a keypress</li>
 *   <li>Exits the alternate screen buffer and provides the final table for terminal history</li>
 * </ol>
 */
public class AlternateScreenTableRenderer {
    /** ANSI: enter alternate screen buffer. */
    static final String ENTER_ALT_SCREEN = "\033[?1049h";

    /** ANSI: leave alternate screen buffer. */
    static final String LEAVE_ALT_SCREEN = "\033[?1049l";

    /** ANSI: clear entire screen. */
    static final String CLEAR_SCREEN = "\033[2J";

    /** ANSI: move cursor to home position (1,1). */
    static final String CURSOR_HOME = "\033[H";

    /** ANSI: reset scroll region to full screen. */
    static final String RESET_SCROLL_REGION = "\033[r";

    /** Number of header lines (top border + header row + separator). */
    static final int HEADER_LINES = 3;

    private final Terminal terminal;
    private final TruncationConfig truncationConfig;

    private String[] columnNames;
    private int[] currentWidths;
    private final List<Object[]> allRows = new ArrayList<>();
    private StreamingTableRenderer renderer;
    private volatile boolean active;
    private int totalRowsRendered;

    private SignalHandler previousIntHandler;
    private SignalHandler previousWinchHandler;

    /**
     * Creates a new alternate screen table renderer.
     *
     * @param terminal JLine terminal instance
     * @param truncationConfig truncation configuration
     */
    public AlternateScreenTableRenderer(Terminal terminal, TruncationConfig truncationConfig) {
        this.terminal = terminal;
        this.truncationConfig = truncationConfig;
    }

    /**
     * Enters the alternate screen buffer and renders the first page of data.
     *
     * @param cols column names
     * @param firstPageContent first page content rows
     */
    public void enter(String[] cols, Object[][] firstPageContent) {
        this.columnNames = cols;
        this.active = true;

        // Save and register signal handlers.
        previousIntHandler = terminal.handle(Signal.INT, sig -> leave());
        previousWinchHandler = terminal.handle(Signal.WINCH, sig -> handleResize());

        PrintWriter writer = terminal.writer();
        writer.print(ENTER_ALT_SCREEN);
        writer.print(CLEAR_SCREEN);
        writer.print(CURSOR_HOME);
        writer.flush();

        // Compute widths from first page.
        currentWidths = computeWidths(columnNames, firstPageContent);
        renderer = createRenderer(columnNames, currentWidths);

        // Render header at top of screen.
        writer.print(renderer.renderHeader());

        // Set scroll region below header.
        int termHeight = terminal.getHeight();
        writer.print(setScrollRegion(HEADER_LINES + 1, termHeight));
        writer.print(cursorTo(HEADER_LINES + 1, 1));
        writer.flush();

        // Accumulate and render first page rows.
        totalRowsRendered = 0;
        Collections.addAll(allRows, firstPageContent);
        for (Object[] row : firstPageContent) {
            Object[] truncated = truncateRow(row, currentWidths);
            writer.print(renderer.renderRow(truncated, totalRowsRendered == 0));
            totalRowsRendered++;
        }
        writer.flush();
    }

    /**
     * Adds a new page of data. If column widths need to increase, re-renders everything.
     *
     * @param pageContent page content rows
     */
    public void addPage(Object[][] pageContent) {
        if (!active) {
            return;
        }

        // Accumulate new rows.
        Collections.addAll(allRows, pageContent);

        // Check if any column needs to be wider.
        int[] newWidths = computeWidths(columnNames, allRows.toArray(new Object[0][]));
        boolean widthChanged = false;
        for (int i = 0; i < newWidths.length; i++) {
            if (newWidths[i] > currentWidths[i]) {
                widthChanged = true;
                break;
            }
        }

        if (widthChanged) {
            currentWidths = newWidths;
            reRenderAll();
        } else {
            // Append new rows incrementally.
            PrintWriter writer = terminal.writer();
            for (Object[] row : pageContent) {
                Object[] truncated = truncateRow(row, currentWidths);
                writer.print(renderer.renderRow(truncated, totalRowsRendered == 0));
                totalRowsRendered++;
            }
            writer.flush();
        }
    }

    /** Read timeout in milliseconds used when waiting for a keypress. */
    private static final long READ_TIMEOUT_MS = 200;

    /**
     * Finishes the display: shows footer, status, and waits for a keypress.
     *
     * @param totalRows total number of rows displayed
     * @param durationMs query duration in milliseconds
     * @param showTiming whether to show timing information
     */
    public void finish(int totalRows, long durationMs, boolean showTiming) {
        if (!active) {
            return;
        }

        PrintWriter writer = terminal.writer();
        writer.print(renderer.renderFooter());

        StringBuilder status = new StringBuilder();
        status.append(totalRows).append(" row(s)");
        if (showTiming) {
            status.append(" in ").append(durationMs).append(" ms");
        }
        status.append(". Press any key to return...");
        writer.print(status);
        writer.flush();

        // Block until keypress, EOF, or until leave() is called (e.g. via Ctrl+C handler).
        // Uses a timeout loop so that the active flag is checked periodically.
        try {
            while (active) {
                int ch = terminal.reader().read(READ_TIMEOUT_MS);
                if (ch >= 0 || ch == -1) {
                    break; // Key pressed or EOF.
                }
                // ch == -2 means timeout, loop and check active flag again.
            }
        } catch (Exception ignored) {
            // Interrupted or error - just return.
        }
    }

    /**
     * Leaves the alternate screen buffer. Idempotent.
     */
    public void leave() {
        if (!active) {
            return;
        }
        active = false;

        PrintWriter writer = terminal.writer();
        writer.print(RESET_SCROLL_REGION);
        writer.print(LEAVE_ALT_SCREEN);
        writer.flush();

        // Restore original signal handlers.
        if (previousIntHandler != null) {
            terminal.handle(Signal.INT, previousIntHandler);
        }
        if (previousWinchHandler != null) {
            terminal.handle(Signal.WINCH, previousWinchHandler);
        }
    }

    /**
     * Returns the complete table (header + all rows + footer) for printing to the main buffer after leaving alt screen.
     *
     * @return complete table string
     */
    public String renderFinalTable() {
        StreamingTableRenderer finalRenderer = createRenderer(columnNames, currentWidths);
        StringBuilder sb = new StringBuilder();

        String[] truncatedHeader = truncateRowCells(columnNames, currentWidths);
        StreamingTableRenderer headerRenderer = new StreamingTableRenderer(truncatedHeader, currentWidths);
        sb.append(headerRenderer.renderHeader());

        for (int i = 0; i < allRows.size(); i++) {
            Object[] truncated = truncateRow(allRows.get(i), currentWidths);
            sb.append(finalRenderer.renderRow(truncated, i == 0));
        }
        sb.append(finalRenderer.renderFooter());
        return sb.toString();
    }

    /**
     * Returns whether the renderer is currently active (in alt screen).
     *
     * @return true if active
     */
    public boolean isActive() {
        return active;
    }

    private void handleResize() {
        if (!active) {
            return;
        }
        // Recompute widths with new terminal width and re-render.
        currentWidths = computeWidths(columnNames, allRows.toArray(new Object[0][]));
        reRenderAll();
    }

    private void reRenderAll() {
        renderer = createRenderer(columnNames, currentWidths);

        PrintWriter writer = terminal.writer();
        writer.print(RESET_SCROLL_REGION);
        writer.print(CLEAR_SCREEN);
        writer.print(CURSOR_HOME);

        // Re-render header.
        writer.print(renderer.renderHeader());

        // Set scroll region.
        int termHeight = terminal.getHeight();
        writer.print(setScrollRegion(HEADER_LINES + 1, termHeight));
        writer.print(cursorTo(HEADER_LINES + 1, 1));

        // Re-render all accumulated rows.
        totalRowsRendered = 0;
        for (Object[] row : allRows) {
            Object[] truncated = truncateRow(row, currentWidths);
            writer.print(renderer.renderRow(truncated, totalRowsRendered == 0));
            totalRowsRendered++;
        }
        writer.flush();
    }

    private int[] computeWidths(String[] headers, Object[][] content) {
        TruncationConfig widthCalcConfig = truncationConfig.isTruncateEnabled()
                ? truncationConfig
                : new TruncationConfig(true, Integer.MAX_VALUE, 0);
        TableTruncator truncator = new TableTruncator(widthCalcConfig);
        return truncator.calculateColumnWidths(headers, content);
    }

    private StreamingTableRenderer createRenderer(String[] headers, int[] widths) {
        String[] truncatedHeader = truncateRowCells(headers, widths);
        return new StreamingTableRenderer(truncatedHeader, widths);
    }

    private static Object[] truncateRow(Object[] row, int[] widths) {
        Object[] result = new Object[row.length];
        for (int i = 0; i < row.length; i++) {
            result[i] = TableTruncator.truncateCell(row[i], widths[i]);
        }
        return result;
    }

    private static String[] truncateRowCells(String[] row, int[] widths) {
        String[] result = new String[row.length];
        for (int i = 0; i < row.length; i++) {
            result[i] = TableTruncator.truncateCell(row[i], widths[i]);
        }
        return result;
    }

    /**
     * Returns the ANSI escape sequence to set the scroll region.
     *
     * @param top first line of scroll region (1-based)
     * @param bottom last line of scroll region (1-based)
     * @return ANSI escape sequence
     */
    static String setScrollRegion(int top, int bottom) {
        return "\033[" + top + ";" + bottom + "r";
    }

    /**
     * Returns the ANSI escape sequence to move the cursor to a given position.
     *
     * @param row row (1-based)
     * @param col column (1-based)
     * @return ANSI escape sequence
     */
    static String cursorTo(int row, int col) {
        return "\033[" + row + ";" + col + "H";
    }
}
