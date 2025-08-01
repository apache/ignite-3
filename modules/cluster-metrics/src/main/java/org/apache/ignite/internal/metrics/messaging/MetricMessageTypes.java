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

package org.apache.ignite.internal.metrics.messaging;

import static org.apache.ignite.internal.metrics.messaging.MetricMessageTypes.GROUP_TYPE;

import org.apache.ignite.internal.metrics.message.MetricDisableRequest;
import org.apache.ignite.internal.metrics.message.MetricDisableResponse;
import org.apache.ignite.internal.metrics.message.MetricEnableRequest;
import org.apache.ignite.internal.metrics.message.MetricEnableResponse;
import org.apache.ignite.internal.metrics.message.MetricSourcesRequest;
import org.apache.ignite.internal.metrics.message.MetricSourcesResponse;
import org.apache.ignite.internal.network.annotations.MessageGroup;

/**
 * Message types for the metrics.
 */
@MessageGroup(groupType = GROUP_TYPE, groupName = "MetricMessages")
public interface MetricMessageTypes {
    /** Message group type. */
    short GROUP_TYPE = 16;

    /** Type for {@link MetricEnableRequest}. */
    short METRIC_ENABLE_REQUEST = 0;

    /** Type for {@link MetricEnableResponse}. */
    short METRIC_ENABLE_RESPONSE = 1;

    /** Type for {@link MetricDisableRequest}. */
    short METRIC_DISABLE_REQUEST = 2;

    /** Type for {@link MetricDisableResponse}. */
    short METRIC_DISABLE_RESPONSE = 3;

    /** Type for {@link MetricSourcesRequest}. */
    short METRIC_SOURCES_REQUEST = 4;

    /** Type for {@link MetricSourcesResponse}. */
    short METRIC_SOURCES_RESPONSE = 5;
}
