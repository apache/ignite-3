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

package org.apache.ignite.internal.metastorage.configuration;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/**
 * Configuration schema for the Meta Storage module.
 */
@ConfigurationRoot(rootName = "metaStorage", type = ConfigurationType.DISTRIBUTED)
public class MetaStorageConfigurationSchema {
    /**
     * Duration (in milliseconds) used to determine how often to issue time sync commands when the Meta Storage is idle
     * (no writes are being issued).
     *
     * <p>Making this value too small increases the network load, while making this value too large can lead to increased latency of
     * Meta Storage reads.
     */
    @Value(hasDefault = true)
    @Range(min = 1)
    public long idleSyncTimeInterval = 250;//
}
