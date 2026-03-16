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

class MessagingServiceMetrics {
    private final AtomicLongMetric messageHandlingFailures;

    private final AtomicLongMetric messageRecipientNotFound;

    private final AtomicLongMetric invokeTimeouts;

    private final AtomicLongMetric slowResponses;

    MessagingServiceMetrics(MessagingServiceMetricSource source) {
        messageHandlingFailures = source.addMetric(new AtomicLongMetric(
                "messageHandlingFailures",
                "Total number of message handling failures."
        ));

        messageRecipientNotFound = source.addMetric(new AtomicLongMetric(
                "messageRecipientNotFound",
                "Total number of message recipient resolution failures."
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

    void incrementMessageHandlingFailures() {
        messageHandlingFailures.increment();
    }

    void incrementMessageRecipientNotFound() {
        messageRecipientNotFound.increment();
    }

    void incrementInvokeTimeouts() {
        invokeTimeouts.increment();
    }

    void incrementSlowResponses() {
        slowResponses.increment();
    }
}
