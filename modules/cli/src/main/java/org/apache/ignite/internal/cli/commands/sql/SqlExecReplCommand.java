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

import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_KEY;
import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.MAX_COL_WIDTH_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.MAX_COL_WIDTH_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NO_TRUNCATE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NO_TRUNCATE_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.SCRIPT_FILE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.SCRIPT_FILE_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.TIMED_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.TIMED_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.Parser.isTreeSitterParserAvailable;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.Constants.DEFAULT_SQL_DISPLAY_PAGE_SIZE;
import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.ansi;
import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.fg;

import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.ignite.internal.cli.call.sql.SqlQueryCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.sql.help.IgniteSqlCommandCompleter;
import org.apache.ignite.internal.cli.commands.treesitter.highlighter.SqlAttributedStringHighlighter;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.CallExecutionPipelineProvider;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.handler.SqlExceptionHandler;
import org.apache.ignite.internal.cli.core.repl.Repl;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContextProvider;
import org.apache.ignite.internal.cli.core.repl.executor.RegistryCommandExecutor;
import org.apache.ignite.internal.cli.core.repl.executor.ReplExecutorProvider;
import org.apache.ignite.internal.cli.core.repl.terminal.PagerSupport;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;
import org.apache.ignite.internal.cli.decorators.TruncationConfig;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.cli.sql.PagedSqlResult;
import org.apache.ignite.internal.cli.sql.SqlManager;
import org.apache.ignite.internal.cli.sql.SqlSchemaProvider;
import org.apache.ignite.internal.cli.sql.table.AlternateScreenTableRenderer;
import org.apache.ignite.internal.cli.sql.table.StreamingTableRenderer;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.apache.ignite.internal.cli.util.TableTruncator;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.jline.reader.EOFError;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;
import org.jline.reader.impl.DefaultHighlighter;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command for sql execution in REPL mode.
 */
@Command(name = "sql", description = "Executes SQL query")
public class SqlExecReplCommand extends BaseCommand implements Runnable {
    private static final IgniteLogger LOG = CliLoggers.forClass(SqlExecReplCommand.class);

    @Option(names = JDBC_URL_OPTION, required = true, descriptionKey = JDBC_URL_KEY, description = JDBC_URL_OPTION_DESC)
    private String jdbc;

    @Option(names = PLAIN_OPTION, description = PLAIN_OPTION_DESC)
    private boolean plain;

    @Option(names = TIMED_OPTION, description = TIMED_OPTION_DESC)
    private boolean timed;

    @Option(names = MAX_COL_WIDTH_OPTION, description = MAX_COL_WIDTH_OPTION_DESC)
    private Integer maxColWidth;

    @Option(names = NO_TRUNCATE_OPTION, description = NO_TRUNCATE_OPTION_DESC)
    private boolean noTruncate;

    @ArgGroup
    private ExecOptions execOptions;

    private static class ExecOptions {
        @Parameters(index = "0", description = "SQL query to execute", defaultValue = Option.NULL_VALUE)
        private String command;

        @Option(names = SCRIPT_FILE_OPTION, description = SCRIPT_FILE_OPTION_DESC, defaultValue = Option.NULL_VALUE)
        private File file;
    }

    @Inject
    private ReplExecutorProvider replExecutorProvider;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    @Inject
    private Session session;

    @Inject
    private ApiClientFactory clientFactory;

    @Inject
    private Terminal terminal;

    private static String extract(File file) {
        try {
            return String.join("\n", Files.readAllLines(file.toPath(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IgniteCliException("File [" + file.getAbsolutePath() + "] not found");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try (SqlManager sqlManager = new SqlManager(jdbc)) {
            // When passing white space to this command, picocli will treat it as a positional argument
            if (execOptions == null || (StringUtils.nullOrBlank(execOptions.command) && execOptions.file == null)) {
                SqlSchemaProvider schemaProvider = new SqlSchemaProvider(sqlManager::getMetadata);
                schemaProvider.initStateAsync();

                SqlCompleter sqlCompleter = new SqlCompleter(schemaProvider);
                IgniteSqlCommandCompleter sqlCommandCompleter = new IgniteSqlCommandCompleter();

                replExecutorProvider.get().execute(Repl.builder()
                        .withPromptProvider(() -> ansi(fg(Color.GREEN).mark("sql-cli> ")))
                        .withCompleter(new AggregateCompleter(sqlCommandCompleter, sqlCompleter))
                        .withCommandClass(SqlReplTopLevelCliCommand.class)
                        .withCallExecutionPipelineProvider(provider(sqlManager))
                        .withHistoryFileName("sqlhistory")
                        .withAutosuggestionsWidgets()
                        .withHighlighter(highlightingEnabled() ? new HighlighterImpl() : new DefaultHighlighter())
                        .withParser(multilineSupported() ? new MultilineParser() : new DefaultParser())
                        .build());
            } else {
                String executeCommand = execOptions.file != null ? extract(execOptions.file) : execOptions.command;
                createPagedSqlExecPipeline(sqlManager, executeCommand).runPipeline();
            }
        } catch (SQLException e) {
            String url = session.info() == null ? null : session.info().nodeUrl();

            ExceptionWriter exceptionWriter = ExceptionWriter.fromPrintWriter(spec.commandLine().getErr());
            try {
                if (url != null) {
                    new ClusterManagementApi(clientFactory.getClient(url)).clusterState();
                }

                SqlExceptionHandler.INSTANCE.handle(exceptionWriter, e);
            } catch (ApiException apiE) {
                new ClusterNotInitializedExceptionHandler("Failed to start sql repl mode", "cluster init")
                        .handle(exceptionWriter, new IgniteCliApiException(apiE, url));
            }
        }
    }

    private boolean multilineSupported() {
        return Boolean.parseBoolean(configManagerProvider.get().getCurrentProperty(CliConfigKeys.SQL_MULTILINE.value()));
    }

    private boolean highlightingEnabled() {
        return isTreeSitterParserAvailable()
                && Boolean.parseBoolean(configManagerProvider.get().getCurrentProperty(CliConfigKeys.SYNTAX_HIGHLIGHTING.value()));
    }

    /**
     * Multiline parser, expects ";" at the end of the line.
     */
    private static final class MultilineParser implements Parser {

        private static final Parser DEFAULT_PARSER = new DefaultParser();

        @Override
        public ParsedLine parse(String line, int cursor, Parser.ParseContext context) throws SyntaxError {
            if ((ParseContext.UNSPECIFIED == context || ParseContext.ACCEPT_LINE == context)
                    && !line.trim().endsWith(";")) {
                throw new EOFError(-1, cursor, "Missing semicolon (;)");
            }

            return DEFAULT_PARSER.parse(line, cursor, context);
        }
    }

    private static class HighlighterImpl implements Highlighter {

        @Override
        public AttributedString highlight(LineReader lineReader, String s) {
            return SqlAttributedStringHighlighter.highlight(s);
        }

        @Override
        public void setErrorPattern(Pattern pattern) {
        }

        @Override
        public void setErrorIndex(int i) {
        }
    }

    private CallExecutionPipelineProvider provider(SqlManager sqlManager) {
        return (executor, exceptionHandlers, line) -> executor.hasCommand(dropSemicolon(line))
                ? createInternalCommandPipeline(executor, exceptionHandlers, line)
                : createPagedSqlExecPipeline(sqlManager, line);
    }

    private CallExecutionPipeline<?, ?> createPagedSqlExecPipeline(SqlManager sqlManager, String line) {
        TruncationConfig truncationConfig = TruncationConfig.fromConfig(
                configManagerProvider,
                terminal::getWidth,
                maxColWidth,
                noTruncate,
                plain
        );

        int pageSize = getPageSize();

        PagerSupport pagerSupport = new PagerSupport(terminal, configManagerProvider);

        // Return a pipeline that fetches SQL results in pages and streams them to the terminal
        return new PagedSqlExecutionPipeline(sqlManager, line, pageSize, truncationConfig, pagerSupport);
    }

    private int getPageSize() {
        String configValue = configManagerProvider.get().getCurrentProperty(CliConfigKeys.SQL_DISPLAY_PAGE_SIZE.value());
        if (configValue != null && !configValue.isEmpty()) {
            try {
                int pageSize = Integer.parseInt(configValue);
                if (pageSize <= 0) {
                    LOG.warn("SQL display page size must be positive, got: {}, using default: {}",
                            pageSize, DEFAULT_SQL_DISPLAY_PAGE_SIZE);
                    return DEFAULT_SQL_DISPLAY_PAGE_SIZE;
                }
                return pageSize;
            } catch (NumberFormatException e) {
                LOG.warn("Invalid SQL display page size in config '{}', using default: {}",
                        configValue, DEFAULT_SQL_DISPLAY_PAGE_SIZE);
            }
        }
        return DEFAULT_SQL_DISPLAY_PAGE_SIZE;
    }

    /**
     * A custom pipeline for paged SQL execution that streams results continuously to the terminal.
     */
    private class PagedSqlExecutionPipeline implements CallExecutionPipeline<StringCallInput, Object> {
        private final SqlManager sqlManager;
        private final String sql;
        private final int pageSize;
        private final TruncationConfig truncationConfig;
        private final PagerSupport pagerSupport;

        PagedSqlExecutionPipeline(SqlManager sqlManager, String sql, int pageSize, TruncationConfig truncationConfig,
                PagerSupport pagerSupport) {
            this.sqlManager = sqlManager;
            this.sql = sql;
            this.pageSize = pageSize;
            this.truncationConfig = truncationConfig;
            this.pagerSupport = pagerSupport;
        }

        @Override
        public int runPipeline() {
            try {
                if (verbose.length > 0) {
                    CliLoggers.startOutputRedirect(spec.commandLine().getErr(), verbose);
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

    private CallExecutionPipeline<?, ?> createInternalCommandPipeline(RegistryCommandExecutor call,
            ExceptionHandlers exceptionHandlers,
            String line) {
        return CallExecutionPipeline.builder(call)
                .inputProvider(() -> new StringCallInput(dropSemicolon(line)))
                .output(CommandLineContextProvider.getContext().out())
                .errOutput(CommandLineContextProvider.getContext().err())
                .exceptionHandlers(exceptionHandlers)
                .verbose(verbose)
                .build();
    }

    private static String dropSemicolon(String line) {
        if (line.trim().endsWith(";")) {
            line = line.substring(0, line.length() - 1);
        }
        return line;
    }
}
