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
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RESULT_LIMIT_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RESULT_LIMIT_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.SCRIPT_FILE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.SCRIPT_FILE_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.TIMED_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.TIMED_OPTION_DESC;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.Constants.DEFAULT_SQL_RESULT_LIMIT;

import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.sql.SqlQueryCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite.internal.cli.core.exception.handler.SqlExceptionHandler;
import org.apache.ignite.internal.cli.decorators.SqlQueryResultDecorator;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.cli.sql.SqlManager;
import org.apache.ignite.internal.logger.IgniteLogger;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command for sql execution.
 */
@Command(name = "sql", description = "Executes SQL query")
public class SqlExecCommand extends BaseCommand implements Callable<Integer> {
    private static final IgniteLogger LOG = CliLoggers.forClass(SqlExecCommand.class);

    @Option(names = JDBC_URL_OPTION, required = true, descriptionKey = JDBC_URL_KEY, description = JDBC_URL_OPTION_DESC)
    private String jdbc;

    @Option(names = PLAIN_OPTION, description = PLAIN_OPTION_DESC)
    private boolean plain;

    @Option(names = TIMED_OPTION, description = TIMED_OPTION_DESC)
    private boolean timed;

    @Option(names = RESULT_LIMIT_OPTION, description = RESULT_LIMIT_OPTION_DESC)
    private Integer resultLimit;

    @ArgGroup(multiplicity = "1")
    private ExecOptions execOptions;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    private static class ExecOptions {
        @Parameters(index = "0", description = "SQL query to execute")
        private String command;

        @Option(names = SCRIPT_FILE_OPTION, description = SCRIPT_FILE_OPTION_DESC)
        private File file;
    }

    private static String extract(File file) {
        try {
            return String.join("\n", Files.readAllLines(file.toPath(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IgniteCliException("File [" + file.getAbsolutePath() + "] not found");
        }
    }

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        try (SqlManager sqlManager = new SqlManager(jdbc)) {
            String executeCommand = execOptions.file != null ? extract(execOptions.file) : execOptions.command;
            return runPipeline(CallExecutionPipeline.builder(new SqlQueryCall(sqlManager, getResultLimit()))
                    .inputProvider(() -> new StringCallInput(executeCommand))
                    .exceptionHandler(SqlExceptionHandler.INSTANCE)
                    .decorator(new SqlQueryResultDecorator(plain, timed))
            );
        } catch (SQLException e) {
            ExceptionWriter exceptionWriter = ExceptionWriter.fromPrintWriter(spec.commandLine().getErr());
            return SqlExceptionHandler.INSTANCE.handle(exceptionWriter, e);
        }
    }

    private int getResultLimit() {
        if (resultLimit != null) {
            return resultLimit;
        }
        String configValue = configManagerProvider.get().getCurrentProperty(CliConfigKeys.SQL_RESULT_LIMIT.value());
        if (configValue != null && !configValue.isEmpty()) {
            try {
                return Integer.parseInt(configValue);
            } catch (NumberFormatException e) {
                LOG.warn("Invalid SQL result limit in config '{}', using default: {}",
                        configValue, DEFAULT_SQL_RESULT_LIMIT);
            }
        }
        return DEFAULT_SQL_RESULT_LIMIT;
    }
}
