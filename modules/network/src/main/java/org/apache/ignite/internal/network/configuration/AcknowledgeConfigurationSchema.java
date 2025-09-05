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

package org.apache.ignite.internal.network.configuration;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/**
 * Configuration schema for acknowledgment settings in the network module.
 * This class defines the configuration properties related to acknowledgment thresholds and delays.
 */
@Config
public class AcknowledgeConfigurationSchema {
    /**
     * The threshold for determining when acknowledgements should be sent synchronously.
     * When the number of unacknowledged messages exceeds this value,
     * acknowledgements will be sent synchronously to prevent excessive message buildup.
     */
    @Range(min = 0)
    @Value(hasDefault = true)
    public final long syncAckThreshold = 10_000;

    /**
     * Delay in milliseconds before sending acknowledgement batch.
     * Allows batching multiple acknowledgements for better network efficiency.
     */
    @Range(min = 0)
    @Value(hasDefault = true)
    @PublicName(legacyNames = "postponeAck")
    public final long postponeAckMillis = 200;
}
