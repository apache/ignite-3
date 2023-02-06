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

package org.apache.ignite.internal.cluster.management.configuration;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/**
 * Cluster management configuration schema.
 */
@ConfigurationRoot(rootName = "cluster", type = ConfigurationType.LOCAL)
public class ClusterManagementConfigurationSchema {
    /** Invoke timeout used by Cluster Management module (ms). */
    @Value(hasDefault = true)
    @Range(min = 1)
    public long networkInvokeTimeout = 500;

    /**
     * Delay between a moment a node drops out from the physical topology and when it gets removed from the logical topology (ms).
     */
    @Value(hasDefault = true)
    @Range(min = 0)
    public long failoverTimeout = TimeUnit.SECONDS.toMillis(30);

    /** Maximum amount of time a validated node that has not yet completed the join is allowed to remain validated (ms). */
    @Value(hasDefault = true)
    @Range(min = 1)
    public long incompleteJoinTimeout = TimeUnit.HOURS.toMillis(1);
}
