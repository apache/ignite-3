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

package org.apache.ignite.internal.cli.core.repl.terminal;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.jline.terminal.Terminal;

/**
 * Handles pager support in CLI REPL mode.
 *
 * <p>Provides functionality to pipe large outputs through a pager (like `less` or `more`)
 * when the output exceeds the terminal height.
 */
public class PagerSupport {

    /** Default pager command. Uses less with flags: -R (ANSI colors), -F (quit if fits), -X (no clear). */
    public static final String DEFAULT_PAGER = "less -RFX";

    /** Default pager command for Windows. */
    public static final String DEFAULT_PAGER_WINDOWS = "more";

    /** Number of lines to reserve for prompt and status. */
    private static final int TERMINAL_MARGIN = 2;

    private final boolean pagerEnabled;
    private final String pagerCommand;
    private final Terminal terminal;

    /**
     * Creates PagerSupport for production use.
     *
     * @param terminal JLine terminal instance
     * @param configManagerProvider configuration manager provider
     */
    public PagerSupport(Terminal terminal, ConfigManagerProvider configManagerProvider) {
        this.terminal = terminal;
        this.pagerEnabled = readPagerEnabled(configManagerProvider);
        this.pagerCommand = readPagerCommand(configManagerProvider);
    }

    /**
     * Creates PagerSupport for testing.
     *
     * @param terminal JLine terminal (can be null for testing)
     * @param pagerEnabled whether pager is enabled
     * @param pagerCommand pager command (null for default)
     */
    PagerSupport(Terminal terminal, boolean pagerEnabled, String pagerCommand) {
        this.terminal = terminal;
        this.pagerEnabled = pagerEnabled;
        this.pagerCommand = resolveCommand(pagerCommand);
    }

    /**
     * Writes output to terminal, using pager if output exceeds terminal height.
     *
     * @param output the output to write
     */
    public void write(String output) {
        if (output == null || output.isEmpty()) {
            return;
        }
        if (shouldUsePager(output)) {
            pipeToPage(output);
        } else {
            terminal.writer().print(output);
            terminal.writer().flush();
        }
    }

    /**
     * Checks if the pager should be used for the given output.
     *
     * @param output the output to check
     * @return true if the output exceeds terminal height and pager is enabled
     */
    boolean shouldUsePager(String output) {
        if (!pagerEnabled || terminal == null) {
            return false;
        }
        int lineCount = countLines(output);
        // Query terminal height dynamically to handle window resizing
        int threshold = terminal.getHeight() - TERMINAL_MARGIN;
        return lineCount > threshold;
    }

    /**
     * Pipes the output through the configured pager.
     *
     * @param output the output to display in the pager
     */
    public void pipeToPage(String output) {
        String command = getPagerCommand();
        try {
            // Pause JLine terminal to release control to the pager process
            if (terminal != null) {
                terminal.pause();
            }

            ProcessBuilder pb = createPagerProcess(command);
            Process process = pb.start();

            try (OutputStream os = process.getOutputStream()) {
                os.write(output.getBytes(StandardCharsets.UTF_8));
            }

            process.waitFor();
        } catch (IOException | InterruptedException e) {
            // Fallback: print directly if pager fails
            if (terminal != null) {
                terminal.writer().print(output);
                terminal.writer().flush();
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            // Resume JLine terminal control
            if (terminal != null) {
                try {
                    terminal.resume();
                } catch (Exception ignored) {
                    // Ignore resume errors
                }
            }
        }
    }

    /**
     * Returns the pager command to use.
     *
     * @return the pager command
     */
    public String getPagerCommand() {
        return pagerCommand;
    }

    /**
     * Returns whether the pager is enabled.
     *
     * @return true if pager is enabled
     */
    public boolean isPagerEnabled() {
        return pagerEnabled;
    }

    /**
     * Counts the number of lines in the output.
     *
     * @param output the output to count lines in
     * @return the number of lines
     */
    static int countLines(String output) {
        if (output == null || output.isEmpty()) {
            return 0;
        }
        // Normalize Windows line endings
        String normalized = output.replace("\r", "");
        // Remove trailing newline for accurate count
        if (normalized.endsWith("\n")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        if (normalized.isEmpty()) {
            return 0;
        }
        return (int) normalized.chars().filter(c -> c == '\n').count() + 1;
    }

    /** Creates the process builder for the pager. */
    private static ProcessBuilder createPagerProcess(String command) {
        ProcessBuilder pb;
        if (isWindows()) {
            pb = new ProcessBuilder("cmd", "/c", command);
        } else {
            pb = new ProcessBuilder("sh", "-c", command);
        }
        pb.inheritIO();
        pb.redirectInput(ProcessBuilder.Redirect.PIPE);
        return pb;
    }

    private static boolean readPagerEnabled(ConfigManagerProvider configManagerProvider) {
        String value = configManagerProvider.get()
                .getCurrentProperty(CliConfigKeys.PAGER_ENABLED.value());
        if (value == null || value.isEmpty()) {
            // Default to false on Windows (more command doesn't support ANSI colors well)
            // Default to true on other platforms
            return !isWindows();
        }
        return Boolean.parseBoolean(value);
    }

    private static String readPagerCommand(ConfigManagerProvider configManagerProvider) {
        String configured = configManagerProvider.get()
                .getCurrentProperty(CliConfigKeys.PAGER_COMMAND.value());
        return resolveCommand(configured);
    }

    private static String resolveCommand(String configured) {
        // Priority: configured > $PAGER env > default
        if (configured != null && !configured.isEmpty()) {
            return configured;
        }

        String envPager = System.getenv("PAGER");
        if (envPager != null && !envPager.isEmpty()) {
            return envPager;
        }

        return isWindows() ? DEFAULT_PAGER_WINDOWS : DEFAULT_PAGER;
    }

    static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }
}
