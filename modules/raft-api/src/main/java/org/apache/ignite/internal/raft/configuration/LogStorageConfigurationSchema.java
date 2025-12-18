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

package org.apache.ignite.internal.raft.configuration;

import static org.apache.ignite.internal.util.Constants.KiB;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/** Configuration of the Raft log storage. */
@Config
public class LogStorageConfigurationSchema {
    public static final int DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE = 10;

    public static final int DEFAULT_SEGMENT_FILE_SIZE_BYTES = Integer.MAX_VALUE;

    public static final int UNSPECIFIED_MAX_LOG_ENTRY_SIZE = -1;

    /**
     * Maximum size of the log storage checkpoint queue.
     */
    @Value(hasDefault = true)
    @Range(min = 1)
    public int maxCheckpointQueueSize = DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE;

    /**
     * Size of a segment file in bytes.
     */
    @Value(hasDefault = true)
    @Range(min = 4 * KiB, max = Integer.MAX_VALUE)
    public long segmentFileSizeBytes = DEFAULT_SEGMENT_FILE_SIZE_BYTES;

    /**
     * Maximum allowed size of a log entry in bytes.
     */
    @Value(hasDefault = true)
    public int maxLogEntrySizeBytes = UNSPECIFIED_MAX_LOG_ENTRY_SIZE;

    /**
     * Computes the default maximum log entry size based on the segment file size.
     *
     * <p>Should be used to calculate the maximum log entry size when it is equal to {@link #UNSPECIFIED_MAX_LOG_ENTRY_SIZE}.
     */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static int computeDefaultMaxLogEntrySizeBytes(long segmentFileSizeBytes) {
        // We set the max entry size to 90% of the segment file to leave some space for metadata overhead.
        long maxAllowedEntrySize = (long) (segmentFileSizeBytes * 0.9);

        return (int) Math.min(Integer.MAX_VALUE, maxAllowedEntrySize);
    }
}
