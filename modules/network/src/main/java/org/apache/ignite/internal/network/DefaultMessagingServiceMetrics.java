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
    private final AtomicLongMetric messageSendFailures;

    private final AtomicLongMetric messageRecipientNotFound;

    private final AtomicLongMetric invokeRequestFailures;

    private final AtomicLongMetric invokeResponseFailures;

    private final AtomicLongMetric messageSerializationFailures;

    private final AtomicLongMetric messageDeserializationFailures;

    private final AtomicLongMetric connectionFailures;

    private final AtomicLongMetric invokeTimeouts;

    private final AtomicLongMetric slowResponses;

    DefaultMessagingServiceMetrics(DefaultMessagingServiceMetricSource source) {
        messageSendFailures = source.addMetric(new AtomicLongMetric(
                "messageSendFailures",
                "Total number of failed outgoing messages."
        ));

        messageRecipientNotFound = source.addMetric(new AtomicLongMetric(
                "messageRecipientNotFound",
                "Total number of message recipient resolution failures."
        ));

        invokeRequestFailures = source.addMetric(new AtomicLongMetric(
                "invokeRequestFailures",
                "Total number of failed outgoing request invocations."
        ));

        invokeResponseFailures = source.addMetric(new AtomicLongMetric(
                "invokeResponseFailures",
                "Total number of failed outgoing responses to invoked requests."
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

        invokeTimeouts = source.addMetric(new AtomicLongMetric(
                "invokeTimeouts",
                "Total number of invocation timeouts."
        ));

        slowResponses = source.addMetric(new AtomicLongMetric(
                "slowResponses",
                "Total number of responses that took long to generate (> 100ms)."
        ));
    }

    void incrementMessageSendFailures() {
        messageSendFailures.increment();
    }

    void incrementMessageRecipientNotFound() {
        messageRecipientNotFound.increment();
    }

    void incrementInvokeRequestFailures() {
        invokeRequestFailures.increment();
    }

    void  incrementInvokeResponseFailures() {
        invokeResponseFailures.increment();
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

    void incrementInvokeTimeouts() {
        invokeTimeouts.increment();
    }

    void incrementSlowResponses() {
        slowResponses.increment();
    }
}
