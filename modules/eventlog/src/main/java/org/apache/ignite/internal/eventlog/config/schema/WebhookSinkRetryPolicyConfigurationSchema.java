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

package org.apache.ignite.internal.eventlog.config.schema;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/** Retry policy configuration schema. */
@Config
public class WebhookSinkRetryPolicyConfigurationSchema {
    /** Maximum retry attempt count. */
    @Value(hasDefault = true)
    @Range(min = 0)
    public final int maxAttempts = 2;

    /** Base delay between retry attempt, in milliseconds. */
    @Value(hasDefault = true)
    @Range(min = 1)
    @PublicName(legacyNames = "initBackoff")
    public final long initBackoffMillis = TimeUnit.SECONDS.toMillis(1L);

    /** Maximum delay between retry attempt, in milliseconds. */
    @Value(hasDefault = true)
    @Range(min = 1)
    @PublicName(legacyNames = "maxBackoff")
    public final long maxBackoffMillis = TimeUnit.SECONDS.toMillis(5L);

    /** Delay multiplier. */
    @Value(hasDefault = true)
    @Range(min = 1)
    public final long backoffMultiplier = 2L;
}
