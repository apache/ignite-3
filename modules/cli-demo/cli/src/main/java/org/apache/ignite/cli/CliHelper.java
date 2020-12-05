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

package org.apache.ignite.cli;

import java.util.Arrays;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.ColorScheme;
import picocli.CommandLine.Model.UsageMessageSpec;

public class CliHelper {
    public static void initCli(CommandLine cli) {
        cli.setColorScheme(new ColorScheme.Builder()
            .commands(Ansi.Style.fg_green)
            .options(Ansi.Style.fg_yellow)
            .optionParams(Ansi.Style.fg_cyan)
            .parameters(Ansi.Style.fg_yellow)
            .errors(Ansi.Style.fg_red, Ansi.Style.bold)
            .build());

        cli.setHelpSectionKeys(Arrays.asList(
            UsageMessageSpec.SECTION_KEY_HEADER,
            UsageMessageSpec.SECTION_KEY_SYNOPSIS_HEADING,
            UsageMessageSpec.SECTION_KEY_SYNOPSIS,
            UsageMessageSpec.SECTION_KEY_COMMAND_LIST_HEADING,
            UsageMessageSpec.SECTION_KEY_COMMAND_LIST,
            UsageMessageSpec.SECTION_KEY_OPTION_LIST_HEADING,
            UsageMessageSpec.SECTION_KEY_OPTION_LIST,
            UsageMessageSpec.SECTION_KEY_PARAMETER_LIST_HEADING,
            UsageMessageSpec.SECTION_KEY_PARAMETER_LIST
        ));

        boolean hasCommands = !cli.getCommandSpec().subcommands().isEmpty();
        boolean hasOptions = !cli.getCommandSpec().options().isEmpty();
        boolean hasParameters = !cli.getCommandSpec().positionalParameters().isEmpty();

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_SYNOPSIS_HEADING,
            help -> Ansi.AUTO.string("@|bold USAGE|@\n"));

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_SYNOPSIS,
            help -> {
                StringBuilder sb = new StringBuilder();

                sb.append("  ");
                sb.append(help.colorScheme().commandText(help.commandSpec().qualifiedName()));

                if (hasCommands) {
                    sb.append(help.colorScheme().parameterText(" [COMMAND]"));
                }
                else {
                    if (hasOptions)
                        sb.append(help.colorScheme().parameterText(" [OPTIONS]"));

                    if (hasParameters)
                        sb.append(help.colorScheme().parameterText(" [PARAMETERS]"));
                }

                sb.append("\n\n");

                return sb.toString();
            });

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_COMMAND_LIST_HEADING,
            help -> hasCommands ? Ansi.AUTO.string("@|bold COMMANDS|@\n") : "");

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_OPTION_LIST_HEADING,
            help -> hasOptions ? Ansi.AUTO.string("@|bold OPTIONS|@\n") : "");

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_PARAMETER_LIST_HEADING,
            help -> hasParameters ? Ansi.AUTO.string("@|bold PARAMETERS|@\n") : "");
    }
}
