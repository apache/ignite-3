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

package org.apache.ignite.internal.cli.commands.sql;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.function.Consumer;
import org.apache.ignite.internal.cli.call.sql.SqlQueryCall;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.exception.handler.SqlExceptionHandler;
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContextProvider;
import org.apache.ignite.internal.cli.core.repl.terminal.PagerSupport;
import org.apache.ignite.internal.cli.decorators.TruncationConfig;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.cli.sql.PagedSqlResult;
import org.apache.ignite.internal.cli.sql.SqlManager;
import org.apache.ignite.internal.cli.sql.table.AlternateScreenTableRenderer;
import org.apache.ignite.internal.cli.sql.table.StreamingTableRenderer;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.apache.ignite.internal.cli.util.TableTruncator;
import org.jline.terminal.Terminal;

/**
 * A pipeline for paged SQL execution that streams results continuously to the terminal.
 */
class PagedSqlExecutionPipeline implements CallExecutionPipeline<StringCallInput, Object> {
    private final SqlManager sqlManager;
    private final String sql;
    private final int pageSize;
    private final TruncationConfig truncationConfig;
    private final PagerSupport pagerSupport;
    private final Terminal terminal;
    private final boolean plain;
    private final boolean timed;
    private final boolean[] verbose;
    private final PrintWriter verboseErr;

    PagedSqlExecutionPipeline(
            SqlManager sqlManager,
            String sql,
            int pageSize,
            TruncationConfig truncationConfig,
            PagerSupport pagerSupport,
            Terminal terminal,
            boolean plain,
            boolean timed,
            boolean[] verbose,
            PrintWriter verboseErr
    ) {
        this.sqlManager = sqlManager;
        this.sql = sql;
        this.pageSize = pageSize;
        this.truncationConfig = truncationConfig;
        this.pagerSupport = pagerSupport;
        this.terminal = terminal;
        this.plain = plain;
        this.timed = timed;
        this.verbose = verbose;
        this.verboseErr = verboseErr;
    }

    @Override
    public int runPipeline() {
        try {
            if (verbose.length > 0) {
                CliLoggers.startOutputRedirect(verboseErr, verbose);
            }
            return runPipelineInternal();
        } finally {
            if (verbose.length > 0) {
                CliLoggers.stopOutputRedirect();
            }
        }
    }

    private int runPipelineInternal() {
        PrintWriter err = CommandLineContextProvider.getContext().err();
        PrintWriter out = CommandLineContextProvider.getContext().out();

        try (PagedSqlResult pagedResult = sqlManager.executePaged(SqlQueryCall.trimQuotes(sql), pageSize)) {
            if (!pagedResult.hasResultSet()) {
                int updateCount = pagedResult.getUpdateCount();
                out.println(updateCount >= 0 ? "Updated " + updateCount + " rows." : "OK!");
                if (timed) {
                    out.println("Query executed in " + pagedResult.getDurationMs() + " ms");
                }
                out.flush();
                return 0;
            }

            Table<String> firstPage = pagedResult.fetchNextPage();
            if (firstPage == null) {
                if (timed) {
                    out.println("Query executed in " + pagedResult.getDurationMs() + " ms, 0 rows returned");
                }
                return 0;
            }

            int totalRows;
            if (plain) {
                totalRows = streamPlain(out, pagedResult, firstPage);
            } else {
                totalRows = streamBoxDrawing(out, pagedResult, firstPage);
            }

            CliLoggers.verboseLog(1, "<-- " + totalRows + " row(s) (" + pagedResult.getDurationMs() + "ms)");

            if (timed) {
                out.println("Query executed in " + pagedResult.getDurationMs() + " ms, " + totalRows + " rows returned");
                out.flush();
            }

            return 0;
        } catch (SQLException e) {
            SqlExceptionHandler.INSTANCE.handle(ExceptionWriter.fromPrintWriter(err), e);
            return 1;
        }
    }

    private int streamPlain(PrintWriter out, PagedSqlResult pagedResult, Table<String> firstPage)
            throws SQLException {
        if (pagerSupport.isPagerEnabled() && isRealTerminal()) {
            return streamPlainPager(pagedResult, firstPage);
        } else {
            return streamPlainDirect(out, pagedResult, firstPage);
        }
    }

    private int streamPlainPager(PagedSqlResult pagedResult, Table<String> firstPage) throws SQLException {
        try (PagerSupport.StreamingPager pager = pagerSupport.openStreaming()) {
            Consumer<String> sink = pager::write;
            sink.accept(String.join("\t", firstPage.header()) + System.lineSeparator());

            int totalRows = printPlainRows(sink, firstPage.content());

            Table<String> page;
            while ((page = pagedResult.fetchNextPage()) != null) {
                totalRows += printPlainRows(sink, page.content());
            }

            return totalRows;
        }
    }

    private int streamPlainDirect(PrintWriter out, PagedSqlResult pagedResult, Table<String> firstPage)
            throws SQLException {
        Consumer<String> sink = out::print;
        sink.accept(String.join("\t", firstPage.header()) + System.lineSeparator());

        int totalRows = printPlainRows(sink, firstPage.content());
        out.flush();

        Table<String> page;
        while ((page = pagedResult.fetchNextPage()) != null) {
            totalRows += printPlainRows(sink, page.content());
            out.flush();
        }

        return totalRows;
    }

    private int printPlainRows(Consumer<String> sink, Object[][] content) {
        String lineSep = System.lineSeparator();
        for (Object[] row : content) {
            StringBuilder line = new StringBuilder();
            for (int i = 0; i < row.length; i++) {
                if (i > 0) {
                    line.append('\t');
                }
                line.append(row[i]);
            }
            line.append(lineSep);
            sink.accept(line.toString());
        }
        return content.length;
    }

    private int streamBoxDrawing(PrintWriter out, PagedSqlResult pagedResult, Table<String> firstPage)
            throws SQLException {
        if (isRealTerminal() && !pagerSupport.isPagerEnabled()) {
            return streamBoxDrawingAltScreen(out, pagedResult, firstPage);
        } else {
            return streamBoxDrawingBatch(out, pagedResult, firstPage);
        }
    }

    private int streamBoxDrawingAltScreen(PrintWriter out, PagedSqlResult pagedResult, Table<String> firstPage)
            throws SQLException {
        AlternateScreenTableRenderer alt = new AlternateScreenTableRenderer(terminal, truncationConfig);
        try {
            alt.enter(firstPage.header(), firstPage.content());
            int totalRows = firstPage.content().length;

            Table<String> page;
            while ((page = pagedResult.fetchNextPage()) != null) {
                alt.addPage(page.content());
                totalRows += page.content().length;
            }

            alt.finish(totalRows, pagedResult.getDurationMs(), timed);

            // Print final table to main buffer for terminal history.
            out.print(alt.renderFinalTable());
            out.flush();

            return totalRows;
        } finally {
            alt.leave();
        }
    }

    private int streamBoxDrawingBatch(PrintWriter out, PagedSqlResult pagedResult, Table<String> firstPage)
            throws SQLException {
        String[] columnNames = firstPage.header();

        // Lock column widths from first page only so we can start streaming immediately.
        TruncationConfig widthCalcConfig = truncationConfig.isTruncateEnabled()
                ? truncationConfig
                : new TruncationConfig(true, Integer.MAX_VALUE, 0);
        TableTruncator truncator = new TableTruncator(widthCalcConfig);
        int[] lockedWidths = truncator.calculateColumnWidths(columnNames, firstPage.content());

        String[] truncatedHeader = truncateRowCells(columnNames, lockedWidths);
        StreamingTableRenderer renderer = new StreamingTableRenderer(truncatedHeader, lockedWidths);

        if (pagerSupport.isPagerEnabled() && isRealTerminal()) {
            return streamBoxDrawingBatchPager(renderer, lockedWidths, pagedResult, firstPage);
        } else {
            return streamBoxDrawingBatchDirect(out, renderer, lockedWidths, pagedResult, firstPage);
        }
    }

    private int streamBoxDrawingBatchPager(StreamingTableRenderer renderer, int[] lockedWidths,
            PagedSqlResult pagedResult, Table<String> firstPage) throws SQLException {
        try (PagerSupport.StreamingPager pager = pagerSupport.openStreaming()) {
            Consumer<String> sink = pager::write;
            sink.accept(renderer.renderHeader());

            int rowOffset = renderPageRows(sink, renderer, firstPage.content(), lockedWidths, 0);

            Table<String> page;
            while ((page = pagedResult.fetchNextPage()) != null) {
                rowOffset = renderPageRows(sink, renderer, page.content(), lockedWidths, rowOffset);
            }

            sink.accept(renderer.renderFooter());

            return rowOffset;
        }
    }

    private int streamBoxDrawingBatchDirect(PrintWriter out, StreamingTableRenderer renderer, int[] lockedWidths,
            PagedSqlResult pagedResult, Table<String> firstPage) throws SQLException {
        Consumer<String> sink = out::print;
        sink.accept(renderer.renderHeader());

        int rowOffset = renderPageRows(sink, renderer, firstPage.content(), lockedWidths, 0);
        out.flush();

        Table<String> page;
        while ((page = pagedResult.fetchNextPage()) != null) {
            rowOffset = renderPageRows(sink, renderer, page.content(), lockedWidths, rowOffset);
            out.flush();
        }

        sink.accept(renderer.renderFooter());
        out.flush();

        return rowOffset;
    }

    private boolean isRealTerminal() {
        String type = terminal.getType();
        return type != null && !Terminal.TYPE_DUMB.equals(type) && !Terminal.TYPE_DUMB_COLOR.equals(type);
    }

    private int renderPageRows(Consumer<String> sink, StreamingTableRenderer renderer,
            Object[][] content, int[] lockedWidths, int rowOffset) {
        for (int r = 0; r < content.length; r++) {
            Object[] truncatedRow = new Object[content[r].length];
            for (int c = 0; c < content[r].length; c++) {
                truncatedRow[c] = TableTruncator.truncateCell(content[r][c], lockedWidths[c]);
            }
            sink.accept(renderer.renderRow(truncatedRow, rowOffset + r == 0));
        }
        return rowOffset + content.length;
    }

    private String[] truncateRowCells(String[] row, int[] columnWidths) {
        String[] result = new String[row.length];
        for (int i = 0; i < row.length; i++) {
            result[i] = TableTruncator.truncateCell(row[i], columnWidths[i]);
        }
        return result;
    }
}
