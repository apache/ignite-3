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

    private @Nullable ClientHandlerMetricSource.Holder holder;

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    public long connectionsInitiated() {
        return holder == null ? 0 : holder.connectionsInitiated.value();
    }

    public void connectionsInitiatedIncrement() {
        if (holder != null)
            holder.connectionsInitiated.increment();
    }

    public long sessionsAccepted() {
        return holder == null ? 0 : holder.sessionsAccepted.value();
    }

    public void sessionsAcceptedIncrement() {
        if (holder != null)
            holder.sessionsAccepted.increment();
    }

    public long sessionsActive() {
        return holder == null ? 0 : holder.sessionsActive.value();
    }

    public void sessionsActiveIncrement() {
        if (holder != null)
            holder.sessionsActive.increment();
    }

    public void sessionsActiveDecrement() {
        if (holder != null)
            holder.sessionsActive.decrement();
    }

    public long sessionsRejected() {
        return holder == null ? 0 : holder.sessionsRejected.value();
    }

    public void sessionsRejectedIncrement() {
        if (holder != null)
            holder.sessionsRejected.increment();
    }

    public long sessionsRejectedTls() {
        return holder == null ? 0 : holder.sessionsRejectedTls.value();
    }

    public void sessionsRejectedTlsIncrement() {
        if (holder != null)
            holder.sessionsRejectedTls.increment();
    }

    public long bytesSent() {
        return holder == null ? 0 : holder.bytesSent.value();
    }

    public void bytesSentAdd(long bytes) {
        if (holder != null)
            holder.bytesSent.add(bytes);
    }

    public long bytesReceived() {
        return holder == null ? 0 : holder.bytesReceived.value();
    }

    public void bytesReceivedAdd(long bytes) {
        if (holder != null)
            holder.bytesReceived.add(bytes);
    }

    public long sessionsRejectedTimeout() {
        return holder == null ? 0 : holder.sessionsRejectedTimeout.value();
    }

    public void sessionsRejectedTimeoutIncrement() {
        if (holder != null)
            holder.sessionsRejectedTimeout.increment();
    }

    public long requestsActive() {
        return holder == null ? 0 : holder.requestsActive.value();
    }

    public void requestsActiveIncrement() {
        if (holder != null)
            holder.requestsActive.increment();
    }

    public void requestsActiveDecrement() {
        if (holder != null)
            holder.requestsActive.decrement();
    }

    public long requestsProcessed() {
        return holder == null ? 0 : holder.requestsProcessed.value();
    }

    public void requestsProcessedIncrement() {
        if (holder != null)
            holder.requestsProcessed.increment();
    }

    public long requestsFailed() {
        return holder == null ? 0 : holder.requestsFailed.value();
    }

    public void requestsFailedIncrement() {
        if (holder != null)
            holder.requestsFailed.increment();
    }

    public long transactionsActive() {
        return holder == null ? 0 : holder.transactionsActive.value();
    }

    public void transactionsActiveIncrement() {
        if (holder != null)
            holder.transactionsActive.increment();
    }

    public void transactionsActiveDecrement() {
        if (holder != null)
            holder.transactionsActive.decrement();
    }

    public long cursorsActive() {
        return holder == null ? 0 : holder.cursorsActive.value();
    }

    public void cursorsActiveIncrement() {
        if (holder != null)
            holder.cursorsActive.increment();
    }

    public void cursorsActiveDecrement() {
        if (holder != null)
            holder.cursorsActive.decrement();
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        holder = new Holder();
        List<Metric> metrics = holder.metrics;

        var metricSet = new HashMap<String, Metric>(metrics.size());

        for (var metric : metrics) {
            metricSet.put(metric.name(), metric);
        }

        return new MetricSet(SOURCE_NAME, metricSet);
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
    }
}
