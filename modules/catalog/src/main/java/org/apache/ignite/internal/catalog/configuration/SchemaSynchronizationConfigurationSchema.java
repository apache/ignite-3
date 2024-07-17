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

package org.apache.ignite.internal.catalog.configuration;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.configuration.validation.Range;

/**
 * Configuration for Schema Synchronization.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-98%3A+Schema+Synchronization">Schema Synchronization IEP</a>
 */
@ConfigurationRoot(rootName = "schemaSync", type = ConfigurationType.DISTRIBUTED)
public class SchemaSynchronizationConfigurationSchema {
    /** Delay Duration (ms), see the spec for details. */
    @Value(hasDefault = true)
    @Range(min = 1)
    @Immutable
    public long delayDuration = 500;

    /**
     * Max physical clock skew (ms) that is tolerated by the cluster. If difference between physical clocks of 2 nodes of a cluster
     * exceeds this value, the cluster might demonstrate abnormal behavior.
     */
    @Value(hasDefault = true)
    @Range(min = 1)
    @Immutable
    public long maxClockSkew = 500;
}
