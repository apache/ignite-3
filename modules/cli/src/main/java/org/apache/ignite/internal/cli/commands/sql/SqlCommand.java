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

import static org.apache.ignite.internal.cli.commands.CommandConstants.FOOTER_HEADING;
import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_KEY;
import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.SCRIPT_FILE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.SCRIPT_FILE_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.TIMED_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.TIMED_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION_SHORT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.sql.planner.SqlPlannerCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IFactory;
import picocli.CommandLine.Option;
import picocli.CommandLine.Unmatched;

/**
 * Command for sql operations.
 *
 * <p>The class describes subcommands and redirect calls to default subcommand {@link SqlExecCommand} if no subcommand was specified.
 *
 * @see SqlExecCommand
 */
@Command(name = "sql",
        subcommands = {
                SqlPlannerCommand.class
        },
        description = {
                "Executes SQL queries against an Ignite cluster.",
                "Provide a query as an argument or use --file to execute SQL from a file."
        },
        footerHeading = FOOTER_HEADING,
        footer = {
                "  Execute a SQL query:",
                "    ignite3 sql --jdbc-url jdbc:ignite:thin://127.0.0.1:10800 \"SELECT * FROM t\"",
                "",
                "  Execute SQL from a file:",
                "    ignite3 sql --jdbc-url jdbc:ignite:thin://127.0.0.1:10800 --file=script.sql",
                "",
                "  Execute with plain formatting (useful for piping):",
                "    ignite3 sql --jdbc-url jdbc:ignite:thin://127.0.0.1:10800 --plain \"SELECT * FROM t\"",
                ""}
)
public class SqlCommand extends BaseCommand implements Callable<Integer> {
    // These options are documented here for --help display but are actually processed by SqlExecCommand.
    // All args are passed through to SqlExecCommand via @Unmatched.

    @Option(names = JDBC_URL_OPTION, descriptionKey = JDBC_URL_KEY, description = JDBC_URL_OPTION_DESC)
    private String jdbc;

    @Option(names = PLAIN_OPTION, description = PLAIN_OPTION_DESC)
    private boolean plain;

    @Option(names = TIMED_OPTION, description = TIMED_OPTION_DESC)
    private boolean timed;

    @Option(names = SCRIPT_FILE_OPTION, description = SCRIPT_FILE_OPTION_DESC)
    private String file;

    @Unmatched
    private String[] args;

    @Override
    public Integer call() throws Exception {
        // Picocli lack flexibility parameter validation for subcommands + parser can't distinct positional parameter and subcommand in
        // some cases. That leads to unexpected behavior.
        //
        // With RunLast strategy (see IExecutionStrategy) is used and all parent parameters have Scope.LOCAL,
        // we don't expect parent command parameters be validated when running a subcommand (this just make no sense).
        // To overcome this issues, we implement command in separate class (see SqlExecCommand) and redirect call to it.
        IFactory factory = spec.commandLine().getFactory();
        CommandLine commandLine = new CommandLine(factory.create(SqlExecCommand.class), factory)
                .setErr(spec.commandLine().getErr())
                .setOut(spec.commandLine().getOut())
                .setDefaultValueProvider(spec.defaultValueProvider())
                .setExecutionExceptionHandler(spec.commandLine().getExecutionExceptionHandler());

        return commandLine.execute(buildArgs());
    }

    private String[] buildArgs() {
        List<String> result = new ArrayList<>();
        // Add unmatched args first - they may contain positional parameters that need to be
        // parsed before options for proper ArgGroup mutual exclusion detection in SqlExecCommand.
        if (args != null) {
            Collections.addAll(result, args);
        }
        if (jdbc != null) {
            result.add(JDBC_URL_OPTION + "=" + jdbc);
        }
        if (plain) {
            result.add(PLAIN_OPTION);
        }
        if (timed) {
            result.add(TIMED_OPTION);
        }
        if (file != null) {
            result.add(SCRIPT_FILE_OPTION + "=" + file);
        }
        for (int i = 0; i < verbose.length; i++) {
            result.add(VERBOSE_OPTION_SHORT);
        }
        if (resultLimit != null) {
            result.add(RESULT_LIMIT_OPTION + "=" + resultLimit);
        }
        return result.toArray(new String[0]);
    }
}
