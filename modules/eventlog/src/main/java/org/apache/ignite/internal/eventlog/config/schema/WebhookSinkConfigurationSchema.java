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

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Endpoint;
import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.network.configuration.SslConfigurationSchema;
import org.apache.ignite.internal.network.configuration.SslConfigurationValidator;

/** Configuration schema for webhook sink. */
@PolymorphicConfigInstance(WebhookSinkConfigurationSchema.POLYMORPHIC_ID)
public class WebhookSinkConfigurationSchema extends SinkConfigurationSchema {
    public static final String POLYMORPHIC_ID = "webhook";

    /** String in "host:port" format. */
    @Endpoint
    @Value
    public String endpoint;

    /** The protocol name of this endpoint. */
    @OneOf("http/json")
    @Value(hasDefault = true)
    public String protocol = "http/json";

    /**
     * When size of a batch is greater than {@link #batchSize} or its lifetime is greater than the given value batch will be sent
     * to a webhook, in milliseconds.
     */
    @Value(hasDefault = true)
    @Range(min = 1)
    @PublicName(legacyNames = "batchSendFrequency")
    public long batchSendFrequencyMillis = 10_000;

    /** Maximum batch size for packet with events. */
    @Value(hasDefault = true)
    @Range(min = 1)
    public int batchSize = 1_000;

    /** Maximum queue size of the event queue. */
    @Value(hasDefault = true)
    @Range(min = 1)
    public int queueSize = 10_000;

    /** SSL configuration schema. */
    @ConfigValue
    @SslConfigurationValidator
    public SslConfigurationSchema ssl;

    /** Retry policy configuration schema. */
    @ConfigValue
    public WebhookSinkRetryPolicyConfigurationSchema retryPolicy;
}
