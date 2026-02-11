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
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.commands.TopLevelCliCommand;
import org.apache.ignite.internal.cli.config.ConfigDefaultValueProvider;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.config.StateFolderProvider;
import org.apache.ignite.internal.cli.core.exception.handler.PicocliExecutionExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.question.JlineQuestionWriterReaderFactory;
import org.apache.ignite.internal.cli.core.flow.question.QuestionAskerFactory;
import org.apache.ignite.internal.cli.core.repl.executor.ReplExecutorProviderImpl;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport;
import org.apache.ignite.internal.cli.core.style.ColorScheme;
import org.fusesource.jansi.AnsiConsole;
import org.jline.terminal.Terminal;
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

        // Determine if we're entering interactive REPL mode.
        // REPL mode is only entered when no args AND stdin/stdout are terminals.
        boolean interactiveMode = args.length == 0 && isatty();

        int exitCode = 0;
        ApplicationContextBuilder builder = ApplicationContext.builder(Environment.CLI).deduceEnvironment(false);
        try (MicronautFactory micronautFactory = new MicronautFactory(builder.start())) {
            initColorScheme(micronautFactory);
            if (interactiveMode) {
                // REPL mode: full initialization with Jansi ANSI console and JLine terminal.
                AnsiConsole.systemInstall();
                initReplExecutor(micronautFactory);
                initQuestionAsker(micronautFactory);
                enterRepl(micronautFactory);
            } else {
                // Non-interactive mode: skip JLine terminal initialization for faster startup.
                // Only install ANSI console if stdout is a terminal (for colored output).
                if (isatty()) {
                    AnsiConsole.systemInstall();
                }
                try {
                    exitCode = executeCommand(args, micronautFactory);
                } catch (Exception e) {
                    System.err.println("Error occurred during command execution: " + e.getMessage());
                    exitCode = 1;
                }
            }
        } catch (Exception e) {
            System.err.println("Error occurred during initialization: " + e.getMessage());
            exitCode = 1;
        } finally {
            if (AnsiConsole.isInstalled()) {
                AnsiConsole.systemUninstall();
            }
        }
        System.exit(exitCode);
    }

    private static boolean isatty() {
        return System.console() != null;
    }

    /** Initializes the color scheme provider to read from configuration dynamically. */
    private static void initColorScheme(MicronautFactory micronautFactory) throws Exception {
        ConfigManagerProvider configProvider = micronautFactory.create(ConfigManagerProvider.class);
        // Set a provider that reads from config each time, so changes take effect immediately
        AnsiStringSupport.setColorSchemeProvider(() -> {
            String schemeName = configProvider.get().getCurrentProperty("ignite.cli.color-scheme");
            ColorScheme scheme = ColorScheme.fromString(schemeName);
            ColorScheme result = scheme != null ? scheme : ColorScheme.SOLARIZED_DARK;
            return result;
        });
    }

    /** Needed for immediate REPL mode and for running a command which will stay in REPL mode so we need to init it once. */
    private static void initReplExecutor(MicronautFactory micronautFactory) throws Exception {
        ReplExecutorProviderImpl replExecutorProvider = micronautFactory.create(ReplExecutorProviderImpl.class);
        replExecutorProvider.injectFactory(micronautFactory);
    }

    /** Creates an instance of the terminal and sets the question asker factory. */
    private static void initQuestionAsker(MicronautFactory micronautFactory) throws Exception {
        Terminal terminal = micronautFactory.create(Terminal.class);
        QuestionAskerFactory.setWriterReaderFactory(new JlineQuestionWriterReaderFactory(terminal));
    }

    private static void enterRepl(MicronautFactory micronautFactory) throws Exception {
        VersionProvider versionProvider = micronautFactory.create(VersionProvider.class);
        System.out.println(banner(versionProvider));

        ReplManager replManager = micronautFactory.create(ReplManager.class);
        replManager.subscribe();
        replManager.startReplMode();
    }

    private static int executeCommand(String[] args, MicronautFactory micronautFactory) throws Exception {
        CommandLine cmd = new CommandLine(TopLevelCliCommand.class, micronautFactory);
        cmd.setExecutionExceptionHandler(new PicocliExecutionExceptionHandler());
        cmd.setDefaultValueProvider(micronautFactory.create(ConfigDefaultValueProvider.class));
        cmd.setTrimQuotes(true);
        cmd.setCaseInsensitiveEnumValuesAllowed(true);
        return cmd.execute(args);
    }

    private static final String[] BANNER = {
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
     */
    private static void initJavaLoggerProps() {
        try (InputStream propsFile = Main.class.getResourceAsStream("/cli.java.util.logging.properties")) {
            if (propsFile != null) {
                LogManager.getLogManager().updateConfiguration(propsFile, configurationKey -> {
                    // Merge default configuration with configuration read from propsFile
                    // and append the path to logs to the file pattern if propsFile have the corresponding key
                    if (configurationKey.equals("java.util.logging.FileHandler.pattern")) {
                        return (oldConfigValue, newConfigValue) -> {
                            if (newConfigValue == null) {
                                return oldConfigValue;
                            }
                            try {
                                return getLogsDir() + "/" + newConfigValue;
                            } catch (IOException e) {
                                return newConfigValue;
                            }
                        };
                    }
                    return (o, n) -> n == null ? o : n;
                });
            }
        } catch (IOException ignored) {
            // No-op
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
