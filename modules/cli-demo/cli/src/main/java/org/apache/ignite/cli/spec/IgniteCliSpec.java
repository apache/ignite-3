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
import org.apache.ignite.cli.builtins.SystemPathResolver;
import org.apache.ignite.cli.common.IgniteCommand;
import org.jline.reader.LineReader;
import org.jline.reader.impl.LineReaderImpl;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Model.UsageMessageSpec;

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
    public static final String[] BANNER = new String[] {
        "                       ___                         __",
        "                      /   |   ____   ____ _ _____ / /_   ___",
        "  @|red,bold       ⣠⣶⣿|@          / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
        "  @|red,bold      ⣿⣿⣿⣿|@         / ___ | / /_/ // /_/ // /__ / / / //  __/",
        "  @|red,bold  ⢠⣿⡏⠈⣿⣿⣿⣿⣷|@       /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
        "  @|red,bold ⢰⣿⣿⣿⣧⠈⢿⣿⣿⣿⣿⣦|@            /_/",
        "  @|red,bold ⠘⣿⣿⣿⣿⣿⣦⠈⠛⢿⣿⣿⣿⡄|@       ____               _  __           _____",
        "  @|red,bold  ⠈⠛⣿⣿⣿⣿⣿⣿⣦⠉⢿⣿⡟|@      /  _/____ _ ____   (_)/ /_ ___     |__  /",
        "  @|red,bold ⢰⣿⣶⣀⠈⠙⠿⣿⣿⣿⣿ ⠟⠁|@      / / / __ `// __ \\ / // __// _ \\     /_ <",
        "  @|red,bold ⠈⠻⣿⣿⣿⣿⣷⣤⠙⢿⡟|@       _/ / / /_/ // / / // // /_ /  __/   ___/ /",
        "  @|red,bold       ⠉⠉⠛⠏⠉|@      /___/ \\__, //_/ /_//_/ \\__/ \\___/   /____/",
        "                        /____/\n"};

    private LineReaderImpl reader;

    public static void main(String... args) {
        ApplicationContext applicationContext = ApplicationContext.run();
        CommandLine.IFactory factory = applicationContext.createBean(CommandFactory.class);
        ErrorHandler errorHandler = new ErrorHandler();
        CommandLine cli = new CommandLine(IgniteCliSpec.class, factory)
            .setExecutionExceptionHandler(errorHandler)
            .setParameterExceptionHandler(errorHandler)
            .addSubcommand(applicationContext.createBean(ShellCommandSpec.class));

        applicationContext.createBean(CliPathsConfigLoader.class)
            .loadIgnitePathsConfig()
            .ifPresent(ignitePaths -> loadSubcommands(
                cli,
                ignitePaths.cliLibsDir()
            ));

        PrintWriter out = cli.getOut();

        Arrays.stream(BANNER).map(Ansi.AUTO::string).forEach(out::println);

        out.println(String.format("Apache Ignite CLI ver. %s\n",
            ((AbstractCommandSpec)cli.getCommand()).getVersion()[0]));

        System.exit(cli.execute(args));
    }

    @Override protected void doRun() {
        CommandLine cli = spec.commandLine();

        cli.getHelpSectionMap().compute(UsageMessageSpec.SECTION_KEY_SYNOPSIS, (key, renderer) ->
            help -> renderer.render(help).stripTrailing() +
                    "\n  Or type " + help.colorScheme().commandText(spec.qualifiedName()) +
                    ' ' + help.colorScheme().parameterText("-i") + " to enter interactive mode.\n\n");

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
