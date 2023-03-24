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

package org.apache.ignite.internal.client;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSetBuilder;

/**
 * Client-side metrics.
 */
public class ClientMetricSource extends AbstractMetricSource<ClientMetricSource.Holder> {
    /**
     * Constructor.
     */
    public ClientMetricSource() {
        super("client");
    }

    /**
     * Gets active connections.
     *
     * @return Active connections.
     */
    public long connectionsActive() {
        Holder h = holder();

        return h == null ? 0 : h.connectionsActive.value();
    }

    void connectionsActiveIncrement() {
        Holder h = holder();

        if (h != null) {
            h.connectionsActive.increment();
        }
    }

    void connectionsActiveDecrement() {
        Holder h = holder();

        if (h != null) {
            h.connectionsActive.decrement();
        }
    }

    /**
     * Gets total established connections.
     *
     * @return Total established connections.
     */
    public long connectionsEstablished() {
        Holder h = holder();

        return h == null ? 0 : h.connectionsEstablished.value();
    }

    public void connectionsEstablishedIncrement() {
        Holder h = holder();

        if (h != null) {
            h.connectionsEstablished.increment();
        }
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    @Override
    protected void init(MetricSetBuilder bldr, Holder holder) {
        holder.register(bldr);
    }

    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final AtomicLongMetric connectionsActive =
                new AtomicLongMetric("ConnectionsActive", "Currently active connections");

        private final AtomicLongMetric connectionsEstablished =
                new AtomicLongMetric("ConnectionsEstablished", "Total established connections");

        private final AtomicLongMetric connectionsLost =
                new AtomicLongMetric("ConnectionsLost", "Total lost connections");

        private final AtomicLongMetric connectionsLostTimeout =
                new AtomicLongMetric("ConnectionsLostTimeout", "Total lost connections due to a timeout");

        private final AtomicLongMetric handshakesFailed =
                new AtomicLongMetric("HandshakesFailed", "Total failed handshakes");

        private final AtomicLongMetric handshakesFailedTls =
                new AtomicLongMetric("HandshakesFailedTls", "Total failed handshakes due to a TLS error");

        private final AtomicLongMetric handshakesFailedTimeout =
                new AtomicLongMetric("HandshakesFailedTimeout", "Total failed handshakes due to a timeout");

        private final AtomicLongMetric requestsActive =
                new AtomicLongMetric("RequestsActive", "Total failed handshakes due to a timeout");

        private final AtomicLongMetric requestsSent =
                new AtomicLongMetric("RequestsSent", "Total requests sent");

        private final AtomicLongMetric requestsCompleted =
                new AtomicLongMetric("RequestsCompleted", "Total requests completed (response received)");

        private final AtomicLongMetric requestsCompletedWithRetry =
                new AtomicLongMetric(
                        "RequestsCompletedWithRetry",
                        "Total requests completed with retry (response received after one or more retries)");

        private final AtomicLongMetric requestsFailed = new AtomicLongMetric("RequestsFailed","Total requests failed");

        private final AtomicLongMetric bytesSent = new AtomicLongMetric("BytesSent","Total bytes sent");
        private final AtomicLongMetric bytesReceived = new AtomicLongMetric("BytesReceived","Total bytes received");

        final List<Metric> metrics = Arrays.asList(
                connectionsActive,
                connectionsEstablished,
                connectionsLost,
                connectionsLostTimeout,
                handshakesFailed,
                handshakesFailedTls,
                handshakesFailedTimeout,
                requestsActive,
                requestsSent,
                requestsCompleted,
                requestsCompletedWithRetry,
                requestsFailed,
                bytesSent,
                bytesReceived
        );

        void register(MetricSetBuilder bldr) {
            for (var metric : metrics) {
                bldr.register(metric);
            }
        }
    }
}
