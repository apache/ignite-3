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

package org.apache.ignite.internal.network;

import org.apache.ignite.internal.metrics.AtomicLongMetric;

class DefaultMessagingServiceMetrics {
    private final AtomicLongMetric sendFailures;

    private final AtomicLongMetric respondFailures;

    private final AtomicLongMetric invokeFailures;

    private final AtomicLongMetric messageSerializationFailures;

    private final AtomicLongMetric messageDeserializationFailures;

    private final AtomicLongMetric connectionFailures;

    private final AtomicLongMetric requestProcessingFailures;

    private final AtomicLongMetric invocationTimeouts;

    private final AtomicLongMetric slowRequests;

    DefaultMessagingServiceMetrics(DefaultMessagingServiceMetricSource source) {
        sendFailures = source.addMetric(new AtomicLongMetric(
                "sendFailures",
                "Total number of failed message sends."
        ));

        respondFailures = source.addMetric(new AtomicLongMetric(
                "respondFailures",
                "Total number of failed responses."
        ));

        invokeFailures = source.addMetric(new AtomicLongMetric(
                "invokeFailures",
                "Total number of failed invokes."
        ));

        messageSerializationFailures = source.addMetric(new AtomicLongMetric(
                "messageSerializationFailures",
                "Total number of failed message serializations."
        ));

        messageDeserializationFailures = source.addMetric(new AtomicLongMetric(
                "messageDeserializationFailures",
                "Total number of failed message deserializations."
        ));

        connectionFailures = source.addMetric(new AtomicLongMetric(
                "connectionFailures",
                "Total number of failed connection attempts."
        ));

        requestProcessingFailures = source.addMetric(new AtomicLongMetric(
                "requestProcessingFailures",
                "Total number of failed request processing attempts."
        ));

        invocationTimeouts = source.addMetric(new AtomicLongMetric(
                "invocationTimeouts",
                "Total number of invocation timeouts."
        ));

        slowRequests = source.addMetric(new AtomicLongMetric(
                "slowRequests",
                "Total number of requests that took long to process (> 100ms)."
        ));
    }

    void incrementSendFailures() {
        sendFailures.increment();
    }

    void incrementRespondFailures() {
        respondFailures.increment();
    }

    void incrementInvokeFailures() {
        invokeFailures.increment();
    }

    void incrementMessageSerializationFailures() {
        messageSerializationFailures.increment();
    }

    void incrementMessageDeserializationFailures() {
        messageDeserializationFailures.increment();
    }

    void incrementConnectionFailures() {
        connectionFailures.increment();
    }

    void incrementRequestProcessingFailures() {
        requestProcessingFailures.increment();
    }

    void incrementInvocationTimeouts() {
        invocationTimeouts.increment();
    }

    void incrementSlowRequestsHandledCount() {
        slowRequests.increment();
    }
}
