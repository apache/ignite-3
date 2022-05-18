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
import org.apache.ignite.configuration.validation.Max;
import org.apache.ignite.configuration.validation.Min;
import org.apache.ignite.configuration.validation.OneOf;

/**
 * Checkpoint configuration schema for persistent page memory.
 */
@Config
public class PageMemoryCheckpointConfigurationSchema {
    /** Pages are written in order provided by checkpoint pages collection iterator (which is basically a hashtable). */
    public static final String RANDOM_WRITE_ORDER = "RANDOM";

    /**
     * All checkpoint pages are collected into single list and sorted by page index.
     *
     * <p>Provides almost sequential disk writes, which can be much faster on some SSD models.
     */
    public static final String SEQUENTIAL_WRITE_ORDER = "SEQUENTIAL";

    /** Checkpoint frequency in milliseconds. */
    @Min(0)
    @Value(hasDefault = true)
    public long frequency = 180_000;

    /** Checkpoint frequency deviation. */
    @Min(0)
    @Max(100)
    @Value(hasDefault = true)
    public int frequencyDeviation = 40;

    /** Number of checkpoint threads. */
    @Min(1)
    @Value(hasDefault = true)
    public int threads = 4;

    /** Checkpoint write order. */
    @OneOf({RANDOM_WRITE_ORDER, SEQUENTIAL_WRITE_ORDER})
    @Value(hasDefault = true)
    public String writeOrder = SEQUENTIAL_WRITE_ORDER;

    /** Timeout for checkpoint read lock acquisition in milliseconds. */
    @Min(0)
    @Value(hasDefault = true)
    public long readLockTimeout = 10_000;
}
