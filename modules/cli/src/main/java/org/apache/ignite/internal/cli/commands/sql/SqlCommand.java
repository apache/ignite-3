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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.sql.SqlQueryCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.exception.handler.SqlExceptionHandler;
import org.apache.ignite.internal.cli.decorators.PlainTableDecorator;
import org.apache.ignite.internal.cli.decorators.SqlQueryResultDecorator;
import org.apache.ignite.internal.cli.decorators.TableDecorator;
import org.apache.ignite.internal.cli.deprecated.IgniteCliException;
import org.apache.ignite.internal.cli.sql.SqlManager;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command for sql execution.
 */
@Command(name = "sql", description = "Executes SQL query")
public class SqlCommand extends BaseCommand implements Callable<Integer> {
    @Option(names = {"-u", "--jdbc-url"}, required = true,
            descriptionKey = "ignite.jdbc-url", description = "JDBC url to ignite cluster")
    private String jdbc;

    @Option(names = "--plain", description = "Display output with plain formatting")
    private boolean plain;

    @ArgGroup(multiplicity = "1")
    private ExecOptions execOptions;

    private static class ExecOptions {
        @Parameters(index = "0", description = "SQL query to execute")
        private String command;

        @Option(names = {"-f", "--script-file"}, description = "Path to file with SQL commands to execute")
        private File file;
    }

    private static String extract(File file) {
        try {
            return String.join("\n", Files.readAllLines(file.toPath(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IgniteCliException("File with command not found");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer call() {
        try (SqlManager sqlManager = new SqlManager(jdbc)) {
            String executeCommand = execOptions.file != null ? extract(execOptions.file) : execOptions.command;
            TableDecorator tableDecorator = plain ? new PlainTableDecorator() : new TableDecorator();
            return CallExecutionPipeline.builder(new SqlQueryCall(sqlManager))
                    .inputProvider(() -> new StringCallInput(executeCommand))
                    .output(spec.commandLine().getOut())
                    .errOutput(spec.commandLine().getErr())
                    .decorator(new SqlQueryResultDecorator(tableDecorator))
                    .build().runPipeline();
        } catch (SQLException e) {
            return new SqlExceptionHandler().handle(ExceptionWriter.fromPrintWriter(spec.commandLine().getErr()), e);
        }
    }
}
