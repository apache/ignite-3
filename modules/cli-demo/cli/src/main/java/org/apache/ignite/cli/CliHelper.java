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
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import picocli.CommandLine;
import picocli.CommandLine.Help;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.Ansi.Text;
import picocli.CommandLine.Help.ColorScheme;
import picocli.CommandLine.Help.Column;
import picocli.CommandLine.Help.TextTable;
import picocli.CommandLine.IHelpSectionRenderer;
import picocli.CommandLine.Model.PositionalParamSpec;
import picocli.CommandLine.Model.UsageMessageSpec;

public class CliHelper {
    public static void initCli(CommandLine cli) {
        cli.setColorScheme(new ColorScheme.Builder()
            .commands(Ansi.Style.fg_green)
            .options(Ansi.Style.fg_yellow)
            .parameters(Ansi.Style.fg_cyan)
            .errors(Ansi.Style.fg_red, Ansi.Style.bold)
            .build());

        cli.setHelpSectionKeys(Arrays.asList(
            UsageMessageSpec.SECTION_KEY_DESCRIPTION,
            UsageMessageSpec.SECTION_KEY_SYNOPSIS_HEADING,
            UsageMessageSpec.SECTION_KEY_SYNOPSIS,
            UsageMessageSpec.SECTION_KEY_PARAMETER_LIST_HEADING,
            UsageMessageSpec.SECTION_KEY_PARAMETER_LIST,
            UsageMessageSpec.SECTION_KEY_OPTION_LIST_HEADING,
            UsageMessageSpec.SECTION_KEY_OPTION_LIST,
            UsageMessageSpec.SECTION_KEY_COMMAND_LIST_HEADING,
            UsageMessageSpec.SECTION_KEY_COMMAND_LIST
        ));

        boolean hasCommands = !cli.getCommandSpec().subcommands().isEmpty();
        boolean hasOptions = !cli.getCommandSpec().options().isEmpty();
        boolean hasParameters = !cli.getCommandSpec().positionalParameters().isEmpty();

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_DESCRIPTION,
            help -> Ansi.AUTO.string("@|bold,green " + help.commandSpec().qualifiedName() +
                "|@\n  " + help.description() + "\n"));

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_SYNOPSIS_HEADING,
            help -> Ansi.AUTO.string("@|bold USAGE|@\n"));

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_SYNOPSIS,
            help -> {
                StringBuilder sb = new StringBuilder();

                sb.append("  ");
                sb.append(help.colorScheme().commandText(help.commandSpec().qualifiedName()));

                if (hasCommands) {
                    sb.append(help.colorScheme().commandText(" <COMMAND>"));
                }
                else {
                    if (hasOptions)
                        sb.append(help.colorScheme().optionText(" [OPTIONS]"));

                    if (hasParameters) {
                        for (PositionalParamSpec parameter : cli.getCommandSpec().positionalParameters())
                            sb.append(' ').append(help.colorScheme().parameterText(parameter.paramLabel()));
                    }
                }

                sb.append("\n\n");

                return sb.toString();
            });

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_PARAMETER_LIST_HEADING,
            help -> hasParameters ? Ansi.AUTO.string("@|bold REQUIRED PARAMETERS|@\n") : "");

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_PARAMETER_LIST, new TableRenderer<>(
            h -> h.commandSpec().positionalParameters(),
            p -> p.paramLabel().length(),
            (h, p) -> h.colorScheme().parameterText(p.paramLabel()),
            (h, p) -> h.colorScheme().text(p.description()[0])));

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_OPTION_LIST_HEADING,
            help -> hasOptions ? Ansi.AUTO.string("@|bold OPTIONS|@\n") : "");

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_OPTION_LIST, new TableRenderer<>(
            h -> h.commandSpec().options(),
            o -> o.shortestName().length() + o.paramLabel().length() + 1,
            (h, o) -> h.colorScheme().optionText(o.shortestName()).concat("=").concat(o.paramLabel()),
            (h, o) -> h.colorScheme().text(o.description()[0])));

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_COMMAND_LIST_HEADING,
            help -> hasCommands ? Ansi.AUTO.string("@|bold COMMANDS|@\n") : "");

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_COMMAND_LIST, new TableRenderer<>(
            h -> h.subcommands().values(),
            c -> c.commandSpec().name().length(),
            (h, c) -> h.colorScheme().commandText(c.commandSpec().name()),
            (h, c) -> h.colorScheme().text(c.description().stripTrailing())));
    }

    private static class TableRenderer<T> implements IHelpSectionRenderer {
        private final Function<Help, Collection<T>> itemsFunc;
        private final ToIntFunction<T> nameLenFunc;
        private final BiFunction<Help, T, Text> nameFunc;
        private final BiFunction<Help, T, Text> descriptionFunc;

        TableRenderer(Function<Help, Collection<T>> itemsFunc, ToIntFunction<T> nameLenFunc,
                      BiFunction<Help, T, Text> nameFunc, BiFunction<Help, T, Text> descriptionFunc) {
            this.itemsFunc = itemsFunc;
            this.nameLenFunc = nameLenFunc;
            this.nameFunc = nameFunc;
            this.descriptionFunc = descriptionFunc;
        }

        @Override public String render(Help help) {
            Collection<T> items = itemsFunc.apply(help);

            if (items.isEmpty())
                return "";

            int len = 2 + items.stream().mapToInt(nameLenFunc).max().getAsInt();

            TextTable table = TextTable.forColumns(help.colorScheme(),
                new Column(len, 2, Column.Overflow.SPAN),
                new Column(160 - len, 4, Column.Overflow.WRAP));

            for (T item : items)
                table.addRowValues(nameFunc.apply(help, item), descriptionFunc.apply(help, item));

            return table.toString() + '\n';
        }
    }
}
