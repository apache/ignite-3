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

package org.apache.ignite.internal.failure.configuration;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.failure.handlers.configuration.FailureHandlerConfigurationSchema;

/**
 * Failure processor configuration schema.
 */
@Config
public class FailureProcessorConfigurationSchema {
    /**
     * Amount of memory reserved in the heap at node start in bytes, which can be dropped
     * to increase the chances of success when handling OutOfMemoryError.
     */
    @Value(hasDefault = true)
    @Range(min = 0)
    @PublicName(legacyNames = "oomBufferSizeBites")
    public int oomBufferSizeBytes = 16 * 1024;

    /** Enables threads dumping on critical node failure. */
    @Value(hasDefault = true)
    public boolean dumpThreadsOnFailure = true;

    /**
     * Throttling time out for thread dump generation during failure handling in milliseconds.
     * The default is 10 seconds. The {@code 0} value means that throttling is disabled.
     */
    @Value(hasDefault = true)
    public long dumpThreadsThrottlingTimeoutMillis = 10_000;

    @ConfigValue
    public FailureHandlerConfigurationSchema handler;
}
