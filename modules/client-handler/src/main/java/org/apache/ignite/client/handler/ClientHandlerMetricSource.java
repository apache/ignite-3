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
import java.util.HashMap;
import java.util.List;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Server-side client handler metrics.
 */
public class ClientHandlerMetricSource implements MetricSource {
    private static final String SOURCE_NAME = "client-handler";

    private volatile @Nullable ClientHandlerMetricSource.Holder holder;

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    /**
     * Gets total initiated connections.
     *
     * @return Total initiated connections.
     */
    public long connectionsInitiated() {
        Holder h = holder;

        return h == null ? 0 : h.connectionsInitiated.value();
    }

    void connectionsInitiatedIncrement() {
        Holder h = holder;

        if (h != null) {
            h.connectionsInitiated.increment();
        }
    }

    /**
     * Gets total accepted sessions.
     *
     * @return Total accepted sessions.
     */
    public long sessionsAccepted() {
        Holder h = holder;

        return h == null ? 0 : h.sessionsAccepted.value();
    }

    void sessionsAcceptedIncrement() {
        Holder h = holder;

        if (h != null) {
            h.sessionsAccepted.increment();
        }
    }

    /**
     * Gets active sessions.
     *
     * @return Active sessions.
     */
    public long sessionsActive() {
        Holder h = holder;

        return h == null ? 0 : h.sessionsActive.value();
    }

    void sessionsActiveIncrement() {
        Holder h = holder;

        if (h != null) {
            h.sessionsActive.increment();
        }
    }

    void sessionsActiveDecrement() {
        Holder h = holder;

        if (h != null) {
            h.sessionsActive.decrement();
        }
    }

    /**
     * Gets total rejected sessions.
     *
     * @return Total rejected sessions.
     */
    public long sessionsRejected() {
        Holder h = holder;

        return h == null ? 0 : h.sessionsRejected.value();
    }

    void sessionsRejectedIncrement() {
        Holder h = holder;

        if (h != null) {
            h.sessionsRejected.increment();
        }
    }

    /**
     * Gets sessions rejected due to TLS errors.
     *
     * @return Sessions rejected due to TLS errors.
     */
    public long sessionsRejectedTls() {
        Holder h = holder;

        return h == null ? 0 : h.sessionsRejectedTls.value();
    }

    void sessionsRejectedTlsIncrement() {
        Holder h = holder;

        if (h != null) {
            h.sessionsRejectedTls.increment();
        }
    }

    /**
     * Gets sent bytes.
     *
     * @return Sent bytes.
     */
    public long bytesSent() {
        Holder h = holder;

        return h == null ? 0 : h.bytesSent.value();
    }

    void bytesSentAdd(long bytes) {
        Holder h = holder;

        if (h != null) {
            h.bytesSent.add(bytes);
        }
    }

    /**
     * Gets received bytes.
     *
     * @return Received bytes.
     */
    public long bytesReceived() {
        Holder h = holder;

        return h == null ? 0 : h.bytesReceived.value();
    }

    void bytesReceivedAdd(long bytes) {
        Holder h = holder;

        if (h != null) {
            h.bytesReceived.add(bytes);
        }
    }

    /**
     * Gets sessions rejected due to a timeout.
     *
     * @return Sessions rejected due to a timeout.
     */
    public long sessionsRejectedTimeout() {
        Holder h = holder;

        return h == null ? 0 : h.sessionsRejectedTimeout.value();
    }

    void sessionsRejectedTimeoutIncrement() {
        Holder h = holder;

        if (h != null) {
            h.sessionsRejectedTimeout.increment();
        }
    }

    /**
     * Gets active requests.
     *
     * @return Active requests.
     */
    public long requestsActive() {
        Holder h = holder;

        return h == null ? 0 : h.requestsActive.value();
    }

    void requestsActiveIncrement() {
        Holder h = holder;

        if (h != null) {
            h.requestsActive.increment();
        }
    }

    void requestsActiveDecrement() {
        Holder h = holder;

        if (h != null) {
            h.requestsActive.decrement();
        }
    }

    /**
     * Gets processed requests.
     *
     * @return Processed requests.
     */
    public long requestsProcessed() {
        Holder h = holder;

        return h == null ? 0 : h.requestsProcessed.value();
    }

    void requestsProcessedIncrement() {
        Holder h = holder;

        if (h != null) {
            h.requestsProcessed.increment();
        }
    }

    /**
     * Gets failed requests.
     *
     * @return Failed requests.
     */
    public long requestsFailed() {
        Holder h = holder;

        return h == null ? 0 : h.requestsFailed.value();
    }

    void requestsFailedIncrement() {
        Holder h = holder;

        if (h != null) {
            h.requestsFailed.increment();
        }
    }

    /**
     * Gets active transactions.
     *
     * @return Active transactions.
     */
    public long transactionsActive() {
        Holder h = holder;

        return h == null ? 0 : h.transactionsActive.value();
    }

    /**
     * Increments active transactions.
     */
    public void transactionsActiveIncrement() {
        Holder h = holder;

        if (h != null) {
            h.transactionsActive.increment();
        }
    }

    /**
     * Decrements active transactions.
     */
    public void transactionsActiveDecrement() {
        Holder h = holder;

        if (h != null) {
            h.transactionsActive.decrement();
        }
    }

    /**
     * Gets active cursors.
     *
     * @return Active cursors.
     */
    public long cursorsActive() {
        Holder h = holder;

        return h == null ? 0 : h.cursorsActive.value();
    }

    /**
     * Increments active cursors.
     */
    public void cursorsActiveIncrement() {
        Holder h = holder;

        if (h != null) {
            h.cursorsActive.increment();
        }
    }

    /**
     * Decrements active cursors.
     */
    public void cursorsActiveDecrement() {
        Holder h = holder;

        if (h != null) {
            h.cursorsActive.decrement();
        }
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        Holder h = holder;

        if (h == null) {
            h = new Holder();
            holder = h;
        }

        return h.metricSet;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void disable() {
        holder = null;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized boolean enabled() {
        return holder != null;
    }

    private static class Holder {
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

        private final AtomicLongMetric bytesSent = new AtomicLongMetric("bytes.sent", "Total bytes sent");

        private final AtomicLongMetric bytesReceived = new AtomicLongMetric("bytes.received", "Total bytes received");

        private final AtomicLongMetric requestsActive = new AtomicLongMetric("requests.active", "Requests in progress");

        private final AtomicLongMetric requestsProcessed = new AtomicLongMetric("requests.processed", "Total processed requests");

        private final AtomicLongMetric requestsFailed = new AtomicLongMetric("requests.failed", "Total failed requests");

        private final AtomicLongMetric transactionsActive = new AtomicLongMetric("transactions.active", "Active transactions");

        private final AtomicLongMetric cursorsActive = new AtomicLongMetric("cursors.active", "Active cursors");

        final List<Metric> metrics = Arrays.asList(
                connectionsInitiated,
                sessionsAccepted,
                sessionsActive,
                sessionsRejected,
                sessionsRejectedTls,
                sessionsRejectedTimeout,
                bytesSent,
                bytesReceived,
                requestsActive,
                requestsProcessed,
                requestsFailed,
                transactionsActive,
                cursorsActive
        );

        private final MetricSet metricSet;

        private Holder() {
            var set = new HashMap<String, Metric>(metrics.size());

            for (var metric : metrics) {
                set.put(metric.name(), metric);
            }

            this.metricSet = new MetricSet(SOURCE_NAME, set);
        }
    }
}
