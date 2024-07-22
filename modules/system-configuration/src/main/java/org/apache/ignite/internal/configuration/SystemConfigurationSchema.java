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

package org.apache.ignite.internal.configuration;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;

/**
 * System Configuration schema.
 */
@ConfigurationRoot(rootName = "system", type = ConfigurationType.LOCAL)
public class SystemConfigurationSchema {

    /**
     * Directory where partition data is stored. By default "partitions" subfolder of data storage path is used.
     *
     * <pre>
     * The internal directory structure contains:
     * - `log` directory - raft log directory.
     * - `meta` directory - raft meta directory (raft log metadata and snapshots).
     * - `db` directory - persistent storage directory.
     * </pre>
     */
    @Value(hasDefault = true)
    public String partitionsBasePath = "";
}
