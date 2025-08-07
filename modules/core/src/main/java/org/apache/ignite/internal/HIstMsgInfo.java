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

package org.apache.ignite.internal;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.util.FastTimestamps;

/**
 * A class for managing and logging message histograms with timestamps.
 */
public class HIstMsgInfo {
    /** Date format for thread dumps. */
    private final DateTimeFormatter THREAD_DUMP_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    /** Map to store message counts. */
    private final HashMap<String, Integer> map = new HashMap();

    /** Timestamp of the last log. */
    private long timestamp;

    /** Logger instance for logging messages. */
    private final IgniteLogger log;

    /**
     * Constructs an instance of `HIstMsgInfo`.
     *
     * @param log The logger to be used for logging message histograms.
     */
    public HIstMsgInfo(IgniteLogger log) {
        this.log = log;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Adds a message to the histogram, incrementing its count.
     *
     * @param msg The message to be added.
     */
    public void add(String msg) {
        map.compute(msg, (k, v) -> v == null ? 1 : v + 1);
    }

    /**
     * Logs the message histogram if the timestamp exceeds 10 seconds, and clears the histogram afterwards.
     */
    public void println() {
        if (FastTimestamps.coarseCurrentTimeMillis() - timestamp > 30_000) {
            log.info("PVD:: Message histogram: {} messages, last timestamp: {}", map,
                    THREAD_DUMP_FMT.format(Instant.ofEpochMilli(timestamp)));

            timestamp = System.currentTimeMillis();
            map.clear();
        }
    }
}
