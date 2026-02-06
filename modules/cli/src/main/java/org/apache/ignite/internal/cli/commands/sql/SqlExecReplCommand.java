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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
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
import org.apache.ignite.internal.cli.decorators.TableDecorator;
import org.apache.ignite.internal.cli.decorators.TruncationConfig;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.cli.sql.PagedSqlResult;
import org.apache.ignite.internal.cli.sql.SqlManager;
import org.apache.ignite.internal.cli.sql.SqlSchemaProvider;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.jline.reader.EOFError;
import org.jline.reader.EndOfFileException;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;
import org.jline.reader.UserInterruptException;
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

        // Return a pipeline that executes paged SQL with interactive "load more" functionality
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
     * A custom pipeline for paged SQL execution with interactive "load more" prompts.
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
            PrintWriter err = CommandLineContextProvider.getContext().err();

            // Force auto-flush for real-time output
            PrintWriter autoFlushOut = new PrintWriter(terminal.output(), true);

            try (PagedSqlResult pagedResult = sqlManager.executePaged(SqlQueryCall.trimQuotes(sql), pageSize)) {
                if (!pagedResult.hasResultSet()) {
                    // Non-SELECT query (INSERT, UPDATE, DELETE, etc.)
                    int updateCount = pagedResult.getUpdateCount();
                    autoFlushOut.println(updateCount >= 0 ? "Updated " + updateCount + " rows." : "OK!");
                    if (timed) {
                        autoFlushOut.println("Query executed in " + pagedResult.getDurationMs() + " ms");
                    }
                    return 0;
                }

                // SELECT query - fetch and display pages
                int totalRows = 0;
                List<String> allContent = new ArrayList<>();
                String[] columnNames = null;

                while (true) {
                    Table<String> page = pagedResult.fetchNextPage();
                    if (page == null) {
                        break;
                    }

                    totalRows += page.getRowCount();

                    if (columnNames == null) {
                        columnNames = page.header();
                    }

                    // Accumulate row content - flatten the 2D array to list
                    Object[][] pageContent = page.content();
                    for (Object[] row : pageContent) {
                        for (Object cell : row) {
                            allContent.add(cell != null ? cell.toString() : null);
                        }
                    }

                    if (page.hasMoreRows()) {
                        if (!pagerSupport.isPagerEnabled()) {
                            // Render and display what we have so far as a single table
                            Table<String> displayTable = new Table<>(Arrays.asList(columnNames),
                                    new ArrayList<>(allContent), false);
                            TerminalOutput tableOutput = new TableDecorator(plain, truncationConfig).decorate(displayTable);
                            autoFlushOut.print(tableOutput.toTerminalString());
                            allContent.clear();

                            // More rows available - prompt user
                            autoFlushOut.println("-- " + totalRows + " rows shown. Press Enter to load more, or 'q' to stop --");

                            try {
                                String input = readUserInput();
                                if (input == null || input.equalsIgnoreCase("q")) {
                                    autoFlushOut.println("-- Stopped at " + totalRows + " rows --");
                                    break;
                                }
                            } catch (UserInterruptException | EndOfFileException e) {
                                autoFlushOut.println("-- Stopped at " + totalRows + " rows --");
                                break;
                            }
                        }
                        // If pager is enabled, continue accumulating rows
                    }
                }

                // Display final output as a single continuous table
                if (!allContent.isEmpty() && columnNames != null) {
                    Table<String> finalTable = new Table<>(Arrays.asList(columnNames), allContent, false);
                    TerminalOutput tableOutput = new TableDecorator(plain, truncationConfig).decorate(finalTable);

                    if (pagerSupport.isPagerEnabled()) {
                        pagerSupport.write(tableOutput.toTerminalString());
                    } else {
                        autoFlushOut.print(tableOutput.toTerminalString());
                    }
                }

                if (timed) {
                    autoFlushOut.println("Query executed in " + pagedResult.getDurationMs() + " ms, " + totalRows + " rows returned");
                }

                return 0;
            } catch (SQLException e) {
                SqlExceptionHandler.INSTANCE.handle(ExceptionWriter.fromPrintWriter(err), e);
                return 1;
            }
        }

        private String readUserInput() {
            try {
                StringBuilder sb = new StringBuilder();
                while (true) {
                    int c = terminal.reader().read();
                    if (c == -1) {
                        return "q";
                    }
                    if (c == '\n' || c == '\r') {
                        return sb.toString().trim().equalsIgnoreCase("q") ? "q" : "";
                    }
                    sb.append((char) c);
                }
            } catch (IOException e) {
                return "q";
            }
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
