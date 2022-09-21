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

import org.apache.ignite.internal.cli.VersionProvider;
import org.apache.ignite.internal.cli.commands.cliconfig.CliCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterCommand;
import org.apache.ignite.internal.cli.commands.node.NodeCommand;
import org.apache.ignite.internal.cli.commands.sql.SqlCommand;
import org.apache.ignite.internal.cli.deprecated.spec.BootstrapIgniteCommandSpec;
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
                BootstrapIgniteCommandSpec.class,
                NodeCommand.class,
                ClusterCommand.class
        })
public class TopLevelCliCommand extends BaseCommand {
    @SuppressWarnings("PMD.UnusedPrivateField")
    @Option(names = {"--version"}, versionHelp = true, description = "Print version information and exit")
    private boolean versionRequested;
}
