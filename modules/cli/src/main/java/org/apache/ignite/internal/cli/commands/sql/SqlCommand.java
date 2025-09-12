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

import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.sql.planner.SqlPlannerCommand;
import org.apache.ignite.internal.util.ArrayUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IFactory;
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
        description = "SQL query engine operations."
)
public class SqlCommand extends BaseCommand implements Callable<Integer> {
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

        return commandLine.execute(args == null ?  ArrayUtils.STRING_EMPTY_ARRAY : args);
    }
}
