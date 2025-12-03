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

package org.apache.ignite.internal.cli.commands;

import static org.apache.ignite.internal.cli.commands.CommandConstants.VERSION_OPTION_ORDER;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERSION_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERSION_OPTION_DESC;

import org.apache.ignite.internal.cli.VersionProvider;
import org.apache.ignite.internal.cli.commands.cliconfig.CliCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterCommand;
import org.apache.ignite.internal.cli.commands.connect.ConnectCommand;
import org.apache.ignite.internal.cli.commands.node.NodeCommand;
import org.apache.ignite.internal.cli.commands.recovery.RecoveryCommand;
import org.apache.ignite.internal.cli.commands.sql.SqlCommand;
import org.apache.ignite.internal.cli.commands.zone.ZoneCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Top-level command that prints help and declares subcommands.
 */
@Command(name = "ignite",
        versionProvider = VersionProvider.class,
        description = {
                "Welcome to Ignite Shell alpha.",
                "Run without command to enter interactive mode.",
                ""},
        subcommands = {
                SqlCommand.class,
                CommandLine.HelpCommand.class,
                CliCommand.class,
                ConnectCommand.class,
                NodeCommand.class,
                ClusterCommand.class,
                ZoneCommand.class,
                RecoveryCommand.class
        })
public class TopLevelCliCommand extends BaseCommand {
    @SuppressWarnings("PMD.UnusedPrivateField")
    @Option(names = VERSION_OPTION, versionHelp = true, description = VERSION_OPTION_DESC, order = VERSION_OPTION_ORDER)
    private boolean versionRequested;
}
