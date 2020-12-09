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

import java.io.PrintWriter;
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

import static org.apache.ignite.cli.spec.HelpFactoryImpl.SECTION_KEY_SYNOPSIS_EXTENSION;

/**
 *
 */
@CommandLine.Command(
    name = "ignite",
    description = "Entry point.",
    subcommands = {
        InitIgniteCommandSpec.class,
        ModuleCommandSpec.class,
        NodeCommandSpec.class,
        ConfigCommandSpec.class,
    }
)
public class IgniteCliSpec extends AbstractCommandSpec {

    @CommandLine.Option(names = "-i", hidden = true, required = false)
    boolean interactive;

    @Override public void run() {
        spec.usageMessage().sectionMap().put(SECTION_KEY_SYNOPSIS_EXTENSION,
            help -> " Or type " + help.colorScheme().commandText(spec.qualifiedName()) +
                    ' ' + help.colorScheme().parameterText("-i") + " to enter interactive mode.\n\n");

        CommandLine cli = spec.commandLine();

        if (interactive)
            new InteractiveWrapper().run(cli);
        else
            cli.usage(cli.getOut());
    }

    public static CommandLine initCli(ApplicationContext applicationContext) {
        CommandLine.IFactory factory = applicationContext.createBean(CommandFactory.class);
        ErrorHandler errorHandler = new ErrorHandler();
        CommandLine cli = new CommandLine(IgniteCliSpec.class, factory)
            .setExecutionExceptionHandler(errorHandler)
            .setParameterExceptionHandler(errorHandler);

        cli.setHelpFactory(new HelpFactoryImpl());

        cli.setColorScheme(new CommandLine.Help.ColorScheme.Builder()
            .commands(CommandLine.Help.Ansi.Style.fg_green)
            .options(CommandLine.Help.Ansi.Style.fg_yellow)
            .parameters(CommandLine.Help.Ansi.Style.fg_cyan)
            .errors(CommandLine.Help.Ansi.Style.fg_red, CommandLine.Help.Ansi.Style.bold)
            .build());

        applicationContext.createBean(CliPathsConfigLoader.class)
            .loadIgnitePathsConfig()
            .ifPresent(ignitePaths -> loadSubcommands(
                cli,
                ignitePaths.cliLibsDir()
            ));
        return cli;
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
