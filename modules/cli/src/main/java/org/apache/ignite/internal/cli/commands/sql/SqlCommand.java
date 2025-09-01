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

import io.micronaut.configuration.picocli.MicronautFactory;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.sql.planner.SqlPlannerCommand;
import org.apache.ignite.internal.cli.config.ConfigDefaultValueProvider;
import org.apache.ignite.internal.cli.core.exception.handler.PicocliExecutionExceptionHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Unmatched;

/**
 * Command for sql component management.
 */
@Command(name = "sql",
        subcommands = {
                SqlExecCommand.class,
                SqlPlannerCommand.class
        },
        description = "SQL query engine operations."
)
public class SqlCommand extends BaseCommand  implements Callable<Integer> {
        @Unmatched
        private String[] args;

        @Override
        public Integer call() throws Exception {
                // Redirect call to subcommand.
                try (MicronautFactory micronautFactory = new MicronautFactory()) {
                        SqlExecCommand cmd = micronautFactory.create(SqlExecCommand.class);
                        SqlExecCommand command = CommandLine.populateCommand(cmd, args);

                        CommandLine commandLine = new CommandLine(command, micronautFactory)
                                .setErr(spec.commandLine().getErr())
                                .setOut(spec.commandLine().getOut())
                                .setDefaultValueProvider(micronautFactory.create(ConfigDefaultValueProvider.class))
                                .setExecutionExceptionHandler(new PicocliExecutionExceptionHandler());

                        return commandLine.execute(args);
                }
        }
}
