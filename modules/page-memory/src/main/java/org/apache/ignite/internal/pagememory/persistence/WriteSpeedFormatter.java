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

import java.util.Locale;
import org.apache.ignite.internal.util.Constants;

/** Util class that encapsulates write speed formatting. */
public class WriteSpeedFormatter {
    /**
     * Formats write speed in MB/sec.
     *
     * @param bytes Total number of bytes.
     * @param durationInNanos Duration in nanoseconds.
     * @return Formatted write speed.
     */
    public static String formatWriteSpeed(long bytes, long durationInNanos) {
        if (bytes == 0 || durationInNanos == 0) {
            return "0";
        }

        double durationInSeconds = durationInNanos / 1_000_000_000.0;
        double speedInBs = bytes / durationInSeconds;
        double speedInMbs = speedInBs / Constants.MiB;

        if (speedInMbs >= 10.0) {
            return String.format(Locale.US, "%.0f", speedInMbs);
        } else if (speedInMbs >= 0.1) {
            return String.format(Locale.US, "%.2f", speedInMbs);
        }

        return String.format(Locale.US, "%.4f", speedInMbs);
    }
}
