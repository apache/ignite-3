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

import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.sql.planner.PlanCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IModelTransformer;
import picocli.CommandLine.Model.CommandSpec;

/**
 * Command for sql component management.
 */
@Command(name = "sql-query",
        subcommands = {
                SqlCommand.class,
                PlanCommand.class
        },
        modelTransformer = SqlQueryCommand.Transformer.class,
        description = "SQL query engine operations."
)
public class SqlQueryCommand extends BaseCommand {
    // Rename 'SqlCommand' subcommand and make it visible.
    static class Transformer implements IModelTransformer {
        @Override
        public CommandSpec transform(CommandSpec commandSpec) {
            CommandLine sqlSubcommand = commandSpec.removeSubcommand("sql");
            commandSpec.usageMessage().hidden(false);
            commandSpec.name("exec");
            commandSpec.addSubcommand("exec", sqlSubcommand);
            return commandSpec;
        }
    }
}
