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

package org.apache.ignite.internal.storage.pagememory.configuration.schema;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.pagememory.configuration.CheckpointConfiguration;

/**
 * Checkpoint configuration schema for persistent page memory.
 */
@Config
public class PageMemoryCheckpointConfigurationSchema {
    /** Interval between checkpoints in milliseconds. */
    @Range(min = 0)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "interval")
    public long intervalMillis = CheckpointConfiguration.DEFAULT_CHECKPOINT_INTERVAL;

    /**
     * Max deviation (in percent) of intervals between checkpoints. If this is 20 and {@link #intervalMillis} is 1000, then the effective
     * checkpoint interval values will be between 900 and 1100.
     */
    @Range(min = 0, max = 100)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "intervalDeviation")
    public int intervalDeviationPercent = CheckpointConfiguration.DEFAULT_CHECKPOINT_INTERVAL_DEVIATION;

    /** Delay before executing a checkpoint triggered by RAFT. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public int checkpointDelayMillis = 200;

    /** Number of checkpoint threads. */
    @Range(min = 1)
    @Value(hasDefault = true)
    public int checkpointThreads = CheckpointConfiguration.DEFAULT_CHECKPOINT_THREADS;

    /** Number of threads to compact delta files. */
    @Range(min = 1)
    @Value(hasDefault = true)
    public int compactionThreads = CheckpointConfiguration.DEFAULT_COMPACTION_THREADS;

    /** Timeout for checkpoint read lock acquisition in milliseconds. */
    @Range(min = 0)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "readLockTimeout")
    public long readLockTimeoutMillis = CheckpointConfiguration.DEFAULT_CHECKPOINT_READ_LOCK_TIMEOUT;

    /** Threshold for logging (if greater than zero) read lock holders in milliseconds. */
    @Range(min = 0)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "logReadLockThresholdTimeout")
    public long logReadLockThresholdTimeoutMillis = CheckpointConfiguration.DEFAULT_CHECKPOINT_LOG_READ_LOCK_THRESHOLD_TIMEOUT;
}
