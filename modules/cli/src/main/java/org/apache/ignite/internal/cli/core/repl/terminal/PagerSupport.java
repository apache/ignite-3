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

    private final int terminalHeight;
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
        this.terminalHeight = terminal.getHeight();
        this.pagerEnabled = readPagerEnabled(configManagerProvider);
        this.pagerCommand = readPagerCommand(configManagerProvider);
    }

    /**
     * Creates PagerSupport for testing.
     *
     * @param terminal JLine terminal (can be null for testing)
     * @param terminalHeight terminal height in lines
     * @param pagerEnabled whether pager is enabled
     * @param pagerCommand pager command (null for default)
     */
    PagerSupport(Terminal terminal, int terminalHeight, boolean pagerEnabled, String pagerCommand) {
        this.terminal = terminal;
        this.terminalHeight = terminalHeight;
        this.pagerEnabled = pagerEnabled;
        this.pagerCommand = resolveCommand(pagerCommand);
    }

    /**
     * Checks if the pager should be used for the given output.
     *
     * @param output the output to check
     * @return true if the output exceeds terminal height and pager is enabled
     */
    public boolean shouldUsePager(String output) {
        if (!pagerEnabled) {
            return false;
        }
        int lineCount = countLines(output);
        int threshold = terminalHeight - TERMINAL_MARGIN;
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
    int countLines(String output) {
        if (output == null || output.isEmpty()) {
            return 0;
        }
        // Remove trailing newline for accurate count
        String normalized = output.endsWith("\n") ? output.substring(0, output.length() - 1) : output;
        if (normalized.isEmpty()) {
            return 0;
        }
        int count = 1;
        for (int i = 0; i < normalized.length(); i++) {
            if (normalized.charAt(i) == '\n') {
                count++;
            }
        }
        return count;
    }

    /**
     * Creates the process builder for the pager.
     * Package-private for testing.
     */
    ProcessBuilder createPagerProcess(String command) {
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

    private boolean readPagerEnabled(ConfigManagerProvider configManagerProvider) {
        String value = configManagerProvider.get()
                .getCurrentProperty(CliConfigKeys.PAGER_ENABLED.value());
        // Default to true if not set
        return value == null || value.isEmpty() || Boolean.parseBoolean(value);
    }

    private String readPagerCommand(ConfigManagerProvider configManagerProvider) {
        String configured = configManagerProvider.get()
                .getCurrentProperty(CliConfigKeys.PAGER_COMMAND.value());
        return resolveCommand(configured);
    }

    private String resolveCommand(String configured) {
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

    private boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }
}
