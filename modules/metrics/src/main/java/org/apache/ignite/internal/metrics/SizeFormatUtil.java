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

package org.apache.ignite.internal.metrics;

import java.text.DecimalFormat;
import java.text.ParseException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Number formatter utilities.
 */
public class SizeFormatUtil {
    /** Logger. */
    private static IgniteLogger LOG = Loggers.forClass(SizeFormatUtil.class);

    /** The number of bytes in a kilobyte. */
    public static final long ONE_KB = 1024;

    /** The number of bytes in a megabyte. */
    public static final long ONE_MB = ONE_KB * ONE_KB;

    /** The number of bytes in a gigabyte. */
    public static final long ONE_GB = ONE_KB * ONE_MB;

    /** The number of bytes in a terabyte. */
    public static final long ONE_TB = ONE_KB * ONE_GB;

    /** The number of bytes in a petabyte. */
    public static final long ONE_PB = ONE_KB * ONE_TB;

    /** The number of bytes in an exabyte. */
    public static final long ONE_EB = ONE_KB * ONE_PB;

    private static final DecimalFormat DEC_FORMAT = new DecimalFormat("#.##");

    /**
     * Returns a human-readable version of the size, where the input represents a specific number of bytes.
     *
     * @param size the number of bytes
     * @return a human-readable display value (includes units - EB, PB, TB, GB, MB, KB or bytes)
     */
    public static String byteCountToDisplaySize(final long size) {
        final String displaySize;

        if (size / ONE_EB > 0) {
            displaySize = formatSize(size, ONE_EB, "EB");
        } else if (size / ONE_PB > 0) {
            displaySize = formatSize(size, ONE_PB, "PB");
        } else if (size / ONE_TB > 0) {
            displaySize = formatSize(size, ONE_TB, "TB");
        } else if (size / ONE_GB > 0) {
            displaySize = formatSize(size, ONE_GB, "GB");
        } else if (size / ONE_MB > 0) {
            displaySize = formatSize(size, ONE_MB, "MB");
        } else if (size / ONE_KB > 0) {
            displaySize = formatSize(size, ONE_KB, "KB");
        } else {
            displaySize = size + " bytes";
        }
        return displaySize;
    }

    /**
     * Parses a string representation of size and returns a long value.
     *
     * @param size Size as string.
     * @return Parsed value.
     */
    public static long parsSize(String size) {
        long result = 0;

        try {
            if (size.contains("EB")) {
                result += (long) (DEC_FORMAT.parse(size.substring(0, size.indexOf("EB") - 1)).doubleValue() * ONE_EB);
                size = size.substring(size.indexOf("EB") + 2);
            }

            if (size.contains("PB")) {
                result += (long) (DEC_FORMAT.parse(size.substring(0, size.indexOf("PB") - 1)).doubleValue() * ONE_PB);
                size = size.substring(size.indexOf("PB") + 2);
            }

            if (size.contains("TB")) {
                result += (long) (DEC_FORMAT.parse(size.substring(0, size.indexOf("TB") - 1)).doubleValue() * ONE_TB);
                size = size.substring(size.indexOf("TB") + 2);
            }

            if (size.contains("GB")) {
                result += (long) (DEC_FORMAT.parse(size.substring(0, size.indexOf("GB") - 1)).doubleValue() * ONE_GB);
                size = size.substring(size.indexOf("GB") + 2);
            }

            if (size.contains("MB")) {
                result += (long) (DEC_FORMAT.parse(size.substring(0, size.indexOf("MB") - 1)).doubleValue() * ONE_MB);
                size = size.substring(size.indexOf("MB") + 2);
            }

            if (size.contains("KB")) {
                result += (long) (DEC_FORMAT.parse(size.substring(0, size.indexOf("KB") - 1)).doubleValue() * ONE_KB);
                size = size.substring(size.indexOf("KB") + 2);
            }

            if (size.contains("bytes")) {
                result += Long.parseLong(size.substring(0, size.indexOf("bytes") - 1));
            }
        } catch (ParseException e) {
            LOG.info("Size string parsing exception captain [size={}]", e, size);
        }

        return result;
    }

    private static String formatSize(long size, long divider, String unitName) {
        return DEC_FORMAT.format((double) size / divider) + " " + unitName;
    }
}