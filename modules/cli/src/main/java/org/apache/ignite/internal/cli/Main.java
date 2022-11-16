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

package org.apache.ignite.internal.cli;

import static org.apache.ignite.internal.cli.config.ConfigConstants.IGNITE_CLI_LOGS_DIR;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.ApplicationContextBuilder;
import io.micronaut.context.env.Environment;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.commands.TopLevelCliCommand;
import org.apache.ignite.internal.cli.commands.TopLevelCliReplCommand;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.config.ConfigDefaultValueProvider;
import org.apache.ignite.internal.cli.config.StateFolderProvider;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.apache.ignite.internal.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.internal.cli.core.exception.handler.PicocliExecutionExceptionHandler;
import org.apache.ignite.internal.cli.core.repl.Repl;
import org.apache.ignite.internal.cli.core.repl.SessionDefaultValueProvider;
import org.apache.ignite.internal.cli.core.repl.executor.ReplExecutorProvider;
import org.apache.ignite.internal.cli.core.repl.prompt.PromptProvider;
import org.fusesource.jansi.AnsiConsole;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;


/**
 * Ignite cli entry point.
 */
public class Main {
    /**
     * Entry point.
     *
     * @param args ignore.
     */
    public static void main(String[] args) {
        initJavaLoggerProps();

        int exitCode = 0;
        ApplicationContextBuilder builder = ApplicationContext.builder(Environment.CLI).deduceEnvironment(false);
        try (MicronautFactory micronautFactory = new MicronautFactory(builder.start())) {
            AnsiConsole.systemInstall();
            if (args.length != 0 || !isatty()) { // do not enter REPL if input or output is redirected
                try {
                    exitCode = executeCommand(args, micronautFactory);
                } catch (Exception e) {
                    System.err.println("Error occurred during command execution");
                }
            } else {
                try {
                    enterRepl(micronautFactory);
                } catch (Exception e) {
                    System.err.println("Error occurred during REPL initialization");
                }
            }
        } finally {
            AnsiConsole.systemUninstall();
        }
        System.exit(exitCode);
    }

    private static boolean isatty() {
        return System.console() != null;
    }

    private static void enterRepl(MicronautFactory micronautFactory) throws Exception {
        ReplExecutorProvider replExecutorProvider = micronautFactory.create(ReplExecutorProvider.class);
        replExecutorProvider.injectFactory(micronautFactory);
        HashMap<String, String> aliases = new HashMap<>();
        aliases.put("zle", "widget");
        aliases.put("bindkey", "keymap");

        SessionDefaultValueProvider defaultValueProvider = micronautFactory.create(SessionDefaultValueProvider.class);

        VersionProvider versionProvider = micronautFactory.create(VersionProvider.class);
        System.out.println(banner(versionProvider));

        ConnectToClusterQuestion question = micronautFactory.create(ConnectToClusterQuestion.class);

        replExecutorProvider.get().execute(Repl.builder()
                .withPromptProvider(micronautFactory.create(PromptProvider.class))
                .withAliases(aliases)
                .withCommandClass(TopLevelCliReplCommand.class)
                .withDefaultValueProvider(defaultValueProvider)
                .withCallExecutionPipelineProvider((executor, exceptionHandlers, line) ->
                        CallExecutionPipeline.builder(executor)
                                .inputProvider(() -> new StringCallInput(line))
                                .output(System.out)
                                .errOutput(System.err)
                                .exceptionHandlers(new DefaultExceptionHandlers())
                                .exceptionHandlers(exceptionHandlers)
                                .build())
                .withOnStart(question::askQuestionOnReplStart)
                .withHistoryFileName("history")
                .withTailTipWidgets()
                .build());
    }

    private static int executeCommand(String[] args, MicronautFactory micronautFactory) throws Exception {
        CommandLine cmd = new CommandLine(TopLevelCliCommand.class, micronautFactory);
        cmd.setExecutionExceptionHandler(new PicocliExecutionExceptionHandler());
        cmd.setDefaultValueProvider(micronautFactory.create(ConfigDefaultValueProvider.class));
        return cmd.execute(args);
    }

    private static final String[] BANNER = new String[]{
            "",
            "  @|red,bold          #|@              ___                         __",
            "  @|red,bold        ###|@             /   |   ____   ____ _ _____ / /_   ___",
            "  @|red,bold    #  #####|@           / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
            "  @|red,bold  ###  ######|@         / ___ | / /_/ // /_/ // /__ / / / // ___/",
            "  @|red,bold #####  #######|@      /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
            "  @|red,bold #######  ######|@            /_/",
            "  @|red,bold   ########  ####|@        ____               _  __           @|red,bold _____|@",
            "  @|red,bold  #  ########  ##|@       /  _/____ _ ____   (_)/ /_ ___     @|red,bold |__  /|@",
            "  @|red,bold ####  #######  #|@       / / / __ `// __ \\ / // __// _ \\     @|red,bold /_ <|@",
            "  @|red,bold  #####  #####|@        _/ / / /_/ // / / // // /_ / ___/   @|red,bold ___/ /|@",
            "  @|red,bold    ####  ##|@         /___/ \\__, //_/ /_//_/ \\__/ \\___/   @|red,bold /____/|@",
            "  @|red,bold      ##|@                  /____/\n"
    };

    private static String banner(VersionProvider versionProvider) {
        String banner = Arrays
                .stream(BANNER)
                .map(Ansi.AUTO::string)
                .collect(Collectors.joining("\n"));

        return '\n' + banner + '\n' + " ".repeat(22) + versionProvider.getVersion()[0] + "\n\n";
    }

    /**
     * This is a temporary solution to hide unnecessary java util logs that are produced by ivy. ConsoleHandler.level should be set to
     * SEVERE.
     * TODO: https://issues.apache.org/jira/browse/IGNITE-15713
     */
    @Deprecated
    private static void initJavaLoggerProps() {
        InputStream propsFile = Main.class.getResourceAsStream("/cli.java.util.logging.properties");

        if (propsFile != null) {
            try {
                LogManager.getLogManager().updateConfiguration(propsFile, s -> {
                    // Merge default configuration with configuration read from propsFile
                    // and append the path to logs to the file pattern if propsFile have the corresponding key
                    if (s.equals("java.util.logging.FileHandler.pattern")) {
                        return (o, n) -> {
                            if (n == null) {
                                return o;
                            }
                            try {
                                return getLogsDir() + "/" + n;
                            } catch (IOException e) {
                                return n;
                            }
                        };
                    }
                    return (o, n) -> n == null ? o : n;
                });
            } catch (IOException ignored) {
                // No-op.
            }
        }
    }

    private static String getLogsDir() throws IOException {
        String envLogsDir = System.getenv(IGNITE_CLI_LOGS_DIR);
        String logsDir = envLogsDir != null ? envLogsDir : StateFolderProvider.getStateFile("logs").getAbsolutePath();
        File logsDirFile = new File(logsDir);
        if (!logsDirFile.exists()) {
            if (!logsDirFile.mkdirs()) {
                throw new IOException("Failed to create directory " + logsDir);
            }
        }

        if (logsDirFile.isDirectory()) {
            return logsDir;
        } else {
            throw new IOException(logsDir + " is not a directory");
        }
    }
}
