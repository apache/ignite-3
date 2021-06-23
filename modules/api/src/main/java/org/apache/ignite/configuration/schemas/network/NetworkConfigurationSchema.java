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

package org.apache.ignite.configuration.schemas.network;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Max;
import org.apache.ignite.configuration.validation.Min;

/**
 * Configuration schema for network endpoint subtree.
 */
@ConfigurationRoot(rootName = "network", type = ConfigurationType.LOCAL)
public class NetworkConfigurationSchema {
    /** Network port. */
    @Min(1024)
    @Max(0xFFFF)
    @Value(hasDefault = true)
    public final int port = 47500;

    /** Cluster nodes. */
    @Value(hasDefault = true)
    public final String[] netClusterNodes = new String[0];
}
