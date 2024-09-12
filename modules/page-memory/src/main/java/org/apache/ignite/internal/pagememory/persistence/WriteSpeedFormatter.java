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

package org.apache.ignite.internal.pagememory.persistence;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import org.apache.ignite.internal.util.Constants;

/**
 * Util class that encapsulates write speed formatting. Ported from {@code Ignite 2.x}.
 */
public class WriteSpeedFormatter {
    private static final DecimalFormatSymbols SEPARATOR = new DecimalFormatSymbols();

    static {
        SEPARATOR.setDecimalSeparator('.');
    }

    /** Format for speed > 10 MB/sec. */
    private static final DecimalFormat HIGH_SPEED_FORMAT = new DecimalFormat("#", SEPARATOR);

    /** Format for speed in range 1-10 MB/sec. */
    private static final DecimalFormat MEDIUM_SPEED_FORMAT = new DecimalFormat("#.##", SEPARATOR);

    /**
     * Format for speed < 1 MB/sec For cases when user deployed Grid to inappropriate HW, e.g. AWS EFS, where throughput is elastic and can
     * degrade to near-zero values.
     */
    private static final DecimalFormat LOW_SPEED_FORMAT = new DecimalFormat("#.####", SEPARATOR);

    /** Constructor. */
    private WriteSpeedFormatter() {
        // no-op
    }

    /**
     * Format write speed in MB/sec.
     *
     * @param avgWriteSpeedInBytes Write speed in bytes.
     * @return Formatted write speed.
     */
    public static String formatWriteSpeed(float avgWriteSpeedInBytes) {
        float speedInMbs = avgWriteSpeedInBytes / Constants.MiB;

        if (speedInMbs >= 10.0) {
            synchronized (HIGH_SPEED_FORMAT) {
                return HIGH_SPEED_FORMAT.format(speedInMbs);
            }
        }

        if (speedInMbs >= 0.1) {
            synchronized (MEDIUM_SPEED_FORMAT) {
                return MEDIUM_SPEED_FORMAT.format(speedInMbs);
            }
        }

        synchronized (LOW_SPEED_FORMAT) {
            return LOW_SPEED_FORMAT.format(speedInMbs);
        }
    }
}
