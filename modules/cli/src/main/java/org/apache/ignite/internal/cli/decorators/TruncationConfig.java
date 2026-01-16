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

package org.apache.ignite.internal.cli.decorators;

import java.util.function.IntSupplier;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;

/**
 * Configuration for table column truncation.
 */
public class TruncationConfig {
    /** Default maximum column width. */
    public static final int DEFAULT_MAX_COLUMN_WIDTH = 50;

    /** Truncation indicator. */
    public static final String ELLIPSIS = "...";

    /** Disabled truncation config instance. */
    private static final TruncationConfig DISABLED = new TruncationConfig(false, DEFAULT_MAX_COLUMN_WIDTH, () -> 0);

    private final boolean truncateEnabled;
    private final int maxColumnWidth;
    private final IntSupplier terminalWidthSupplier;

    /**
     * Creates a new TruncationConfig with a fixed terminal width.
     *
     * @param truncateEnabled whether truncation is enabled
     * @param maxColumnWidth maximum column width
     * @param terminalWidth terminal width (0 means no terminal width constraint)
     */
    public TruncationConfig(boolean truncateEnabled, int maxColumnWidth, int terminalWidth) {
        this(truncateEnabled, maxColumnWidth, () -> terminalWidth);
    }

    /**
     * Creates a new TruncationConfig with a dynamic terminal width supplier.
     *
     * @param truncateEnabled whether truncation is enabled
     * @param maxColumnWidth maximum column width
     * @param terminalWidthSupplier supplier for terminal width (evaluated on each call)
     */
    public TruncationConfig(boolean truncateEnabled, int maxColumnWidth, IntSupplier terminalWidthSupplier) {
        this.truncateEnabled = truncateEnabled;
        this.maxColumnWidth = maxColumnWidth;
        this.terminalWidthSupplier = terminalWidthSupplier;
    }

    /**
     * Returns a disabled truncation config.
     *
     * @return disabled config instance
     */
    public static TruncationConfig disabled() {
        return DISABLED;
    }

    /**
     * Creates a TruncationConfig from configuration and command-line overrides.
     *
     * @param configManagerProvider configuration manager provider
     * @param terminalWidthSupplier supplier for terminal width (evaluated dynamically)
     * @param maxColWidthOverride command-line override for max column width (null to use config)
     * @param noTruncateFlag command-line flag to disable truncation
     * @param plainFlag command-line flag for plain output (implies no truncation)
     * @return configured TruncationConfig
     */
    public static TruncationConfig fromConfig(
            ConfigManagerProvider configManagerProvider,
            IntSupplier terminalWidthSupplier,
            Integer maxColWidthOverride,
            boolean noTruncateFlag,
            boolean plainFlag
    ) {
        // Plain output implies no truncation
        if (noTruncateFlag || plainFlag) {
            return DISABLED;
        }

        boolean truncateEnabled = readTruncateEnabled(configManagerProvider);
        if (!truncateEnabled) {
            return DISABLED;
        }

        int maxColumnWidth = maxColWidthOverride != null
                ? maxColWidthOverride
                : readMaxColumnWidth(configManagerProvider);

        return new TruncationConfig(true, maxColumnWidth, terminalWidthSupplier);
    }

    /**
     * Returns whether truncation is enabled.
     *
     * @return true if truncation is enabled
     */
    public boolean isTruncateEnabled() {
        return truncateEnabled;
    }

    /**
     * Returns the maximum column width.
     *
     * @return maximum column width
     */
    public int getMaxColumnWidth() {
        return maxColumnWidth;
    }

    /**
     * Returns the terminal width. This value is evaluated dynamically
     * to support terminal resize during a session.
     *
     * @return terminal width (0 means no constraint)
     */
    public int getTerminalWidth() {
        return terminalWidthSupplier.getAsInt();
    }

    private static boolean readTruncateEnabled(ConfigManagerProvider configManagerProvider) {
        String value = configManagerProvider.get()
                .getCurrentProperty(CliConfigKeys.OUTPUT_TRUNCATE.value());
        // Default to true if not set
        return value == null || value.isEmpty() || Boolean.parseBoolean(value);
    }

    private static int readMaxColumnWidth(ConfigManagerProvider configManagerProvider) {
        String value = configManagerProvider.get()
                .getCurrentProperty(CliConfigKeys.OUTPUT_MAX_COLUMN_WIDTH.value());
        if (value == null || value.isEmpty()) {
            return DEFAULT_MAX_COLUMN_WIDTH;
        }
        try {
            int width = Integer.parseInt(value);
            return width > 0 ? width : DEFAULT_MAX_COLUMN_WIDTH;
        } catch (NumberFormatException e) {
            return DEFAULT_MAX_COLUMN_WIDTH;
        }
    }
}
