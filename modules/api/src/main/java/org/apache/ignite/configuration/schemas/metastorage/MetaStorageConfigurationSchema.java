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

package org.apache.ignite.configuration.schemas.metastorage;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Min;

/**
 * Configuration for the Meta Storage component.
 */
@ConfigurationRoot(rootName = "metastorage", type = ConfigurationType.LOCAL)
public class MetaStorageConfigurationSchema {
    /**
     * This is a temporary property that specifies names of the nodes that host the Meta Storage. It must be removed
     * as soon as cluster-wide "init" command is implemented.
     */
    @Value(hasDefault = true)
    public final String[] metastorageNodes = new String[0];

    /**
     * Startup poll interval in milliseconds.
     * <p>
     * When the Meta Storage component starts, it polls other nodes for some information regarding the cluster
     * Meta Storage configuration. This parameter specifies the interval between unsuccessful polling attempts.
     * <p>
     * Default is 1 second.
     */
    @Value(hasDefault = true)
    @Min(1)
    public final long startupPollIntervalMillis = 1000;
}
