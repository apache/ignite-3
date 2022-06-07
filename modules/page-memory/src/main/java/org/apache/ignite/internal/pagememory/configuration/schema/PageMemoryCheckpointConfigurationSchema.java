/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagememory.configuration.schema;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWriteOrder;

/**
 * Checkpoint configuration schema for persistent page memory.
 */
@Config
public class PageMemoryCheckpointConfigurationSchema {
    /** See description of {@link CheckpointWriteOrder#RANDOM}. */
    public static final String RANDOM_WRITE_ORDER = "RANDOM";

    /** See description of {@link CheckpointWriteOrder#SEQUENTIAL}. */
    public static final String SEQUENTIAL_WRITE_ORDER = "SEQUENTIAL";

    /** Checkpoint frequency in milliseconds. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public long frequency = 180_000;

    /** Checkpoint frequency deviation. */
    @Range(min = 0, max = 100)
    @Value(hasDefault = true)
    public int frequencyDeviation = 40;

    /** Number of checkpoint threads. */
    @Range(min = 1)
    @Value(hasDefault = true)
    public int threads = 4;

    /** Checkpoint write order. */
    @OneOf({RANDOM_WRITE_ORDER, SEQUENTIAL_WRITE_ORDER})
    @Value(hasDefault = true)
    public String writeOrder = SEQUENTIAL_WRITE_ORDER;

    /**
     * Starting from this number of dirty pages in checkpoint, they will be sorted in parallel in case of {@link #SEQUENTIAL_WRITE_ORDER}.
     */
    @Value(hasDefault = true)
    public int parallelSortThreshold = 512 * 1024;

    /** Timeout for checkpoint read lock acquisition in milliseconds. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public long readLockTimeout = 10_000;

    /** Threshold for logging (if greater than zero) read lock holders in milliseconds. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public long logReadLockThresholdTimeout = 0;

    /** Use an asynchronous file I/O operations provider. */
    @Value(hasDefault = true)
    public boolean useAsyncFileIoFactory = true;
}
