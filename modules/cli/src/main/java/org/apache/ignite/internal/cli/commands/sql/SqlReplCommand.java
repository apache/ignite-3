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
import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.SCRIPT_FILE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.SCRIPT_FILE_OPTION_SHORT;
import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.ansi;
import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.fg;

import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import org.apache.ignite.internal.cli.call.sql.SqlQueryCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.sql.help.IgniteSqlCommandCompleter;
import org.apache.ignite.internal.cli.core.CallExecutionPipelineProvider;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite.internal.cli.core.exception.handler.SqlExceptionHandler;
import org.apache.ignite.internal.cli.core.repl.EventListeningActivationPoint;
import org.apache.ignite.internal.cli.core.repl.Repl;
import org.apache.ignite.internal.cli.core.repl.executor.RegistryCommandExecutor;
import org.apache.ignite.internal.cli.core.repl.executor.ReplExecutorProvider;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;
import org.apache.ignite.internal.cli.decorators.SqlQueryResultDecorator;
import org.apache.ignite.internal.cli.sql.SqlManager;
import org.apache.ignite.internal.cli.sql.SqlSchemaProvider;
import org.apache.ignite.internal.util.StringUtils;
import org.jline.reader.impl.completer.AggregateCompleter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command for sql execution in REPL mode.
 */
@Command(name = "sql", description = "Executes SQL query")
public class SqlReplCommand extends BaseCommand implements Runnable {
    @Option(names = {JDBC_URL_OPTION, JDBC_URL_OPTION_SHORT}, required = true,
            descriptionKey = JDBC_URL_KEY, description = JDBC_URL_OPTION_DESC)
    private String jdbc;

    @Option(names = PLAIN_OPTION, description = PLAIN_OPTION_DESC)
    private boolean plain;

    @ArgGroup
    private ExecOptions execOptions;

    private static class ExecOptions {
        @Parameters(index = "0", description = "SQL query to execute", defaultValue = Option.NULL_VALUE)
        private String command;

        @Option(names = {SCRIPT_FILE_OPTION, SCRIPT_FILE_OPTION_SHORT}, description = SCRIPT_FILE_OPTION_SHORT,
                defaultValue = Option.NULL_VALUE)
        private File file;
    }

    @Inject
    private ReplExecutorProvider replExecutorProvider;

    @Inject
    private EventListeningActivationPoint eventListeningActivationPoint;

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
                        .withEventSubscriber(eventListeningActivationPoint)
                        .build());
            } else {
                String executeCommand = execOptions.file != null ? extract(execOptions.file) : execOptions.command;
                if (executeCommand != null) {
                    createSqlExecPipeline(sqlManager, executeCommand).runPipeline();
                }
            }
        } catch (SQLException e) {
            new SqlExceptionHandler().handle(ExceptionWriter.fromPrintWriter(spec.commandLine().getErr()), e);
        }
    }

    private CallExecutionPipelineProvider provider(SqlManager sqlManager) {
        return (executor, exceptionHandlers, line) -> executor.hasCommand(line)
                ? createInternalCommandPipeline(executor, exceptionHandlers, line)
                : createSqlExecPipeline(sqlManager, line);
    }

    private CallExecutionPipeline<?, ?> createSqlExecPipeline(SqlManager sqlManager, String line) {
        return CallExecutionPipeline.builder(new SqlQueryCall(sqlManager))
                .inputProvider(() -> new StringCallInput(line))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new SqlQueryResultDecorator(plain))
                .verbose(verbose)
                .build();
    }

    private CallExecutionPipeline<?, ?> createInternalCommandPipeline(RegistryCommandExecutor call,
            ExceptionHandlers exceptionHandlers,
            String line) {
        return CallExecutionPipeline.builder(call)
                .inputProvider(() -> new StringCallInput(line))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .exceptionHandlers(exceptionHandlers)
                .verbose(verbose)
                .build();
    }
}
