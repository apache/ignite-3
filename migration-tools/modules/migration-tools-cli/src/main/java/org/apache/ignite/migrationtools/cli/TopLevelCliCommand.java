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

package org.apache.ignite.migrationtools.cli;

import static org.apache.ignite3.internal.cli.commands.CommandConstants.VERSION_OPTION_ORDER;
import static org.apache.ignite3.internal.cli.commands.Options.Constants.VERSION_OPTION;
import static org.apache.ignite3.internal.cli.commands.Options.Constants.VERSION_OPTION_DESC;

import org.apache.ignite.migrationtools.cli.configs.commands.ConfigurationConverterCmd;
import org.apache.ignite.migrationtools.cli.persistence.commands.PersistenceBaseCmd;
import org.apache.ignite.migrationtools.cli.sql.commands.SqlDdlGeneratorCmd;
import org.apache.ignite3.internal.cli.VersionProvider;
import org.apache.ignite3.internal.cli.commands.BaseCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Top-level command that prints help and declares subcommands.
 */
@Command(name = "migration-tools",
        versionProvider = VersionProvider.class,
        description = {
                "Welcome to Ignite Migration Tools."
        },
        subcommands = {
                CommandLine.HelpCommand.class,
                ConfigurationConverterCmd.class,
                SqlDdlGeneratorCmd.class,
                PersistenceBaseCmd.class
        })
public class TopLevelCliCommand extends BaseCommand {
    @SuppressWarnings("PMD.UnusedPrivateField")
    @Option(names = VERSION_OPTION, versionHelp = true, description = VERSION_OPTION_DESC, order = VERSION_OPTION_ORDER)
    private boolean versionRequested;
}
