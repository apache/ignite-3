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

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.PowerOfTwo;
import org.apache.ignite.configuration.validation.Range;

/** Configuration schema for RAFT disruptor's. */
@Config
public class DisruptorConfigurationSchema {
    /** Default value for {@link #stripes}. */
    static int DEFAULT_STRIPES_COUNT = Runtime.getRuntime().availableProcessors();

    /** Default value for {@link #logManagerStripes}. */
    static int DEFAULT_LOG_MANAGER_STRIPES_COUNT = 4;

    /** Size of queue in entries for disruptor's. */
    @PowerOfTwo
    @Value(hasDefault = true)
    public int queueSize = 16_384;

    /** Amount of disruptor's that will handle the RAFT server. */
    @Range(min = 1)
    @Value(hasDefault = true)
    public int stripes = DEFAULT_STRIPES_COUNT;

    /** Amount of disruptor's for RAFT log manager. */
    @Range(min = 1)
    @Value(hasDefault = true)
    public int logManagerStripes = DEFAULT_LOG_MANAGER_STRIPES_COUNT;
}
