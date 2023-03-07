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

package org.apache.ignite.client.handler;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

public class ClientHandlerMetricSource implements MetricSource {
    /** Source name. */
    private static final String SOURCE_NAME = "client-handler";

    /*
        SessionsActive +
        SessionsAccepted +
        SessionsRejected +
        SessionsRejectedTls +
        SessionsRejectedTimeout
        RequestsProcessed
        RequestsFailed
        RequestsActive (async processing in progress)
        TransactionsActive
        CursorsActive
        BytesSent
        BytesReceived
     */

    private final AtomicLongMetric connectionsInitiated =
            new AtomicLongMetric("connections.initiated", "Total initiated connections");

    private final AtomicLongMetric sessionsAccepted =
            new AtomicLongMetric("sessions.accepted", "Total accepted sessions");

    private final AtomicLongMetric sessionsActive =
            new AtomicLongMetric("sessions.active", "Active sessions");

    private final AtomicLongMetric sessionsRejected =
            new AtomicLongMetric("sessions.rejected", "Total sessions rejected due to handshake errors");

    private final AtomicLongMetric sessionsRejectedTls =
            new AtomicLongMetric("sessions.rejected.tls", "Total sessions rejected due to TLS handshake errors");

    private final AtomicLongMetric sessionsRejectedTimeout =
            new AtomicLongMetric("sessions.rejected.timeout", "Total sessions rejected by timeout");

    private final List<Metric> metrics = Arrays.asList(
            connectionsInitiated,
            sessionsAccepted,
            sessionsActive,
            sessionsRejected,
            sessionsRejectedTls
    );

    private boolean enabled;

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    public AtomicLongMetric connectionsInitiated() {
        return connectionsInitiated;
    }

    public AtomicLongMetric sessionsAccepted() {
        return sessionsAccepted;
    }

    public AtomicLongMetric sessionsActive() {
        return sessionsActive;
    }

    public AtomicLongMetric sessionsRejected() {
        return sessionsRejected;
    }

    public AtomicLongMetric sessionsRejectedTls() {
        return sessionsRejectedTls;
    }

    public AtomicLongMetric sessionsRejectedTimeout() {
        return sessionsRejectedTimeout;
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        var metricSet = new HashMap<String, Metric>(metrics.size());

        for (var metric : metrics) {
            metricSet.put(metric.name(), metric);
        }

        enabled = true;

        return new MetricSet(SOURCE_NAME, metricSet);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void disable() {
        enabled = false;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized boolean enabled() {
        return enabled;
    }
}
