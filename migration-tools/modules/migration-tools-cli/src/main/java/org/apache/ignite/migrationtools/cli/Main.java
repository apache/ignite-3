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

package org.apache.ignite.migrationtools.cli;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.logging.LogManager;
import org.apache.ignite3.internal.cli.config.CachedConfigManagerProvider;
import org.apache.ignite3.internal.cli.config.ConfigDefaultValueProvider;
import org.apache.ignite3.internal.cli.core.exception.handler.PicocliExecutionExceptionHandler;
import picocli.CommandLine;

/** Main. */
public class Main {
    private static final String LOGS_DIR_ENV = "IGNITE_MIGRATION_TOOLS_LOGS_DIR";

    private static final String LOCAL_FOLDER = ".ignite-migration-tools";

    /**
     * Entry point.
     *
     * @param args ignore.
     */
    public static void main(String[] args) {
        initJavaLoggerProps();

        int exitCode = 0;
        try {
            exitCode = executeCommand(args);
        } catch (Exception e) {
            System.err.println("Error occurred during command execution");
        }

        System.exit(exitCode);
    }

    /**
     * Executes the builds and executes the picocli command.
     *
     * @param args Command line arguments.
     * @return The status code of the command.
     */
    public static int executeCommand(String[] args) {
        CommandLine cmd = new CommandLine(TopLevelCliCommand.class);
        cmd.setExecutionExceptionHandler(new PicocliExecutionExceptionHandler());
        cmd.setDefaultValueProvider(new ConfigDefaultValueProvider(new CachedConfigManagerProvider()));
        cmd.setTrimQuotes(true);
        cmd.setCaseInsensitiveEnumValuesAllowed(true);
        return cmd.execute(args);
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
                    } else if (configurationKey.equals("java.util.logging.FileHandler.formatter")) {
                        return (oldConfigValue, newConfigValue) -> {
                            String pkgName = "org.apache.ignite";
                            String target = (newConfigValue == null) ? oldConfigValue : newConfigValue;
                            if (target.startsWith(pkgName)) {
                                return pkgName + "3" + target.substring(pkgName.length());
                            } else {
                                return target;
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
        String envLogsDir = System.getenv(LOGS_DIR_ENV);
        Path logsDir = envLogsDir != null ? Path.of(envLogsDir) :  Path.of(System.getProperty("user.home"), LOCAL_FOLDER, "logs");
        File logsDirFile = logsDir.toFile();
        if (!logsDirFile.exists()) {
            if (!logsDirFile.mkdirs()) {
                throw new IOException("Failed to create directory " + logsDir);
            }
        }

        if (logsDirFile.isDirectory()) {
            return logsDir.toAbsolutePath().toString();
        } else {
            throw new IOException(logsDir + " is not a directory");
        }
    }
}
