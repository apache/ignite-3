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

package org.apache.ignite.cli.spec;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ServiceLoader;
import io.micronaut.context.ApplicationContext;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.CommandFactory;
import org.apache.ignite.cli.ErrorHandler;
import org.apache.ignite.cli.VersionProvider;
import org.apache.ignite.cli.builtins.SystemPathResolver;
import org.apache.ignite.cli.common.IgniteCommand;
import org.jline.reader.LineReader;
import org.jline.reader.impl.LineReaderImpl;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.ColorScheme;
import picocli.CommandLine.Model.UsageMessageSpec;

/**
 *
 */
@CommandLine.Command(
    name = "ignite",
    header = {
        "                        ___                         __",
        "                       /   |   ____   ____ _ _____ / /_   ___",
        "   @|red,bold       ⣠⣶⣿|@          / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
        "   @|red,bold      ⣿⣿⣿⣿|@         / ___ | / /_/ // /_/ // /__ / / / //  __/",
        "   @|red,bold  ⢠⣿⡏⠈⣿⣿⣿⣿⣷|@       /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
        "   @|red,bold ⢰⣿⣿⣿⣧⠈⢿⣿⣿⣿⣿⣦|@            /_/",
        "   @|red,bold ⠘⣿⣿⣿⣿⣿⣦⠈⠛⢿⣿⣿⣿⡄|@       ____               _  __           _____",
        "   @|red,bold  ⠈⠛⣿⣿⣿⣿⣿⣿⣦⠉⢿⣿⡟|@      /  _/____ _ ____   (_)/ /_ ___     |__  /",
        "   @|red,bold ⢰⣿⣶⣀⠈⠙⠿⣿⣿⣿⣿ ⠟⠁|@      / / / __ `// __ \\ / // __// _ \\     /_ <",
        "   @|red,bold ⠈⠻⣿⣿⣿⣿⣷⣤⠙⢿⡟|@       _/ / / /_/ // / / // // /_ /  __/   ___/ /",
        "   @|red,bold       ⠉⠉⠛⠏⠉|@      /___/ \\__, //_/ /_//_/ \\__/ \\___/   /____/",
        "                         /____/\n",
        "Apache Ignite CLI ver. %s\n"
    },
    footer = "\n2020 Copyright(C) Apache Software Foundation\n",
    versionProvider = VersionProvider.class,
    synopsisHeading = "@|bold USAGE|@\n",
    commandListHeading = "@|bold COMMANDS|@\n",
    subcommands = {
        InitIgniteCommandSpec.class,
        ModuleCommandSpec.class,
        NodeCommandSpec.class,
        ConfigCommandSpec.class,
    }
)
public class IgniteCliSpec implements Runnable {
    private LineReaderImpl reader;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    public static void main(String... args) {
        ApplicationContext applicationContext = ApplicationContext.run();
        CommandLine.IFactory factory = applicationContext.createBean(CommandFactory.class);
        CommandLine cli = new CommandLine(IgniteCliSpec.class, factory)
            .setExecutionExceptionHandler(new ErrorHandler())
            .addSubcommand(applicationContext.createBean(ShellCommandSpec.class));

        applicationContext.createBean(CliPathsConfigLoader.class)
            .loadIgnitePathsConfig()
            .ifPresent(ignitePaths -> loadSubcommands(
                cli,
                ignitePaths.cliLibsDir()
            ));

        System.exit(cli.execute(args));
    }

    @Override public void run() {
        CommandLine cli = spec.commandLine();

        cli.setColorScheme(new ColorScheme.Builder().commands(Ansi.Style.fg_green).build());

        cli.setHelpSectionKeys(Arrays.asList(
            UsageMessageSpec.SECTION_KEY_HEADER,
            UsageMessageSpec.SECTION_KEY_SYNOPSIS_HEADING,
            UsageMessageSpec.SECTION_KEY_SYNOPSIS,
            UsageMessageSpec.SECTION_KEY_COMMAND_LIST_HEADING,
            UsageMessageSpec.SECTION_KEY_COMMAND_LIST,
            UsageMessageSpec.SECTION_KEY_FOOTER
        ));

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_HEADER, help -> help.header(spec.version()[0]));

        cli.getHelpSectionMap().put(UsageMessageSpec.SECTION_KEY_SYNOPSIS,
            help ->
                Ansi.AUTO.string("  @|green " + spec.name() + "|@ @|yellow [COMMAND] [PARAMETERS]|@\n" +
                    "  Or type @|green ignite|@ @|yellow -i|@ to enter interactive mode.\n\n"));

        cli.usage(cli.getOut());
    }

    public void setReader(LineReader reader){
        this.reader = (LineReaderImpl) reader;
    }

    public static void loadSubcommands(CommandLine commandLine, Path cliLibsDir) {
        URL[] urls = SystemPathResolver.list(cliLibsDir);
        ClassLoader classLoader = new URLClassLoader(urls,
            IgniteCliSpec.class.getClassLoader());
        ServiceLoader<IgniteCommand> loader = ServiceLoader.load(IgniteCommand.class, classLoader);
        loader.reload();
        for (IgniteCommand igniteCommand: loader) {
            commandLine.addSubcommand(igniteCommand);
        }
    }
}
