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

import static org.apache.ignite.internal.cli.commands.CommandConstants.ABBREVIATE_SYNOPSIS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.COMMAND_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.DESCRIPTION_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.OPTION_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.PARAMETER_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.REQUIRED_OPTION_MARKER;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SORT_OPTIONS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SORT_SYNOPSIS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SYNOPSIS_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.USAGE_HELP_AUTO_WIDTH;

import org.apache.ignite.internal.cli.commands.cliconfig.CliReplCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterReplCommand;
import org.apache.ignite.internal.cli.commands.connect.ConnectReplCommand;
import org.apache.ignite.internal.cli.commands.connect.DisconnectCommand;
import org.apache.ignite.internal.cli.commands.node.NodeReplCommand;
import org.apache.ignite.internal.cli.commands.recovery.RecoveryReplCommand;
import org.apache.ignite.internal.cli.commands.sql.SqlReplCommand;
import org.apache.ignite.internal.cli.commands.version.VersionCommand;
import org.apache.ignite.internal.cli.commands.zone.ZoneReplCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.shell.jline3.PicocliCommands;

/**
 * Top-level command that just prints help.
 */
@Command(name = "",
        footer = {"", "Press Ctrl-D to exit."},
        subcommands = {
                SqlReplCommand.class,
                PicocliCommands.ClearScreen.class,
                CommandLine.HelpCommand.class,
                ExitCommand.class,
                VersionCommand.class,
                CliReplCommand.class,
                ConnectReplCommand.class,
                DisconnectCommand.class,
                NodeReplCommand.class,
                ClusterReplCommand.class,
                ZoneReplCommand.class,
                RecoveryReplCommand.class
        },

        descriptionHeading = DESCRIPTION_HEADING,
        optionListHeading = OPTION_LIST_HEADING,
        synopsisHeading = SYNOPSIS_HEADING,
        requiredOptionMarker = REQUIRED_OPTION_MARKER,
        usageHelpAutoWidth = USAGE_HELP_AUTO_WIDTH,
        sortOptions = SORT_OPTIONS,
        sortSynopsis = SORT_SYNOPSIS,
        abbreviateSynopsis = ABBREVIATE_SYNOPSIS,
        commandListHeading = COMMAND_LIST_HEADING,
        parameterListHeading = PARAMETER_LIST_HEADING

)
public class TopLevelCliReplCommand {
}
