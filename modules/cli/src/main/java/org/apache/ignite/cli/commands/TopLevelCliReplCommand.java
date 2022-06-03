/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.commands;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.commands.cliconfig.CliCommand;
import org.apache.ignite.cli.commands.configuration.ConfigReplCommand;
import org.apache.ignite.cli.commands.connect.ConnectCommand;
import org.apache.ignite.cli.commands.connect.DisconnectCommand;
import org.apache.ignite.cli.commands.sql.SqlCommand;
import org.apache.ignite.cli.commands.status.StatusCommand;
import org.apache.ignite.cli.commands.version.VersionCommand;
import org.apache.ignite.cli.deprecated.spec.BootstrapIgniteCommandSpec;
import org.apache.ignite.cli.deprecated.spec.ClusterReplCommandSpec;
import org.apache.ignite.cli.deprecated.spec.NodeCommandSpec;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;

/**
 * Top-level command that just prints help.
 */
@CommandLine.Command(name = "",
        footer = {"", "Press Ctrl-D to exit."},
        subcommands = {
                SqlCommand.class,
                PicocliCommands.ClearScreen.class,
                CommandLine.HelpCommand.class,
                ConfigReplCommand.class,
                VersionCommand.class,
                StatusCommand.class,
                CliCommand.class,
                BootstrapIgniteCommandSpec.class,
                NodeCommandSpec.class,
                ClusterReplCommandSpec.class,
                ConnectCommand.class,
                DisconnectCommand.class
        })
@Singleton
public class TopLevelCliReplCommand {
}
