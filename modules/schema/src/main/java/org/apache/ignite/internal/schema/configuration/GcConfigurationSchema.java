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

package org.apache.ignite.internal.schema.configuration;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;

/**
 * Garbage collector configuration schema.
 */
@ConfigurationRoot(rootName = "gc", type = ConfigurationType.DISTRIBUTED)
public class GcConfigurationSchema {
    /** Number of garbage collector threads. */
    @Value(hasDefault = true)
    public int threads = Runtime.getRuntime().availableProcessors();

    /** The maximal number of entries in the storage to be garbage collected in a single batch. */
    @Value(hasDefault = true)
    public int batchSize = 5;

    /** Low watermark configuration. */
    @ConfigValue
    public LowWatermarkConfigurationSchema lowWatermark;
}
