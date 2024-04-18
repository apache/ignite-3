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
import org.apache.ignite.internal.streamer.StreamerMetricSink;

/**
 * Client-side metrics.
 */
public class ClientMetricSource extends AbstractMetricSource<ClientMetricSource.Holder> implements StreamerMetricSink {
    /**
     * Constructor.
     */
    ClientMetricSource() {
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

    /**
     * Increments active connections.
     */
    public void connectionsActiveIncrement() {
        Holder h = holder();

        if (h != null) {
            h.connectionsActive.increment();
        }
    }

    /**
     * Decrements active connections.
     */
    public void connectionsActiveDecrement() {
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

    /**
     * Increments established connections.
     */
    public void connectionsEstablishedIncrement() {
        Holder h = holder();

        if (h != null) {
            h.connectionsEstablished.increment();
        }
    }

    /**
     * Gets total lost connections.
     *
     * @return Total lost connections.
     */
    public long connectionsLost() {
        Holder h = holder();

        return h == null ? 0 : h.connectionsLost.value();
    }

    /**
     * Increments lost connections.
     */
    public void connectionsLostIncrement() {
        Holder h = holder();

        if (h != null) {
            h.connectionsLost.increment();
        }
    }

    /**
     * Gets total lost connections due to a timeout.
     *
     * @return Total lost connections due to a timeout.
     */
    public long connectionsLostTimeout() {
        Holder h = holder();

        return h == null ? 0 : h.connectionsLostTimeout.value();
    }

    /**
     * Increments lost connections due to a timeout.
     */
    public void connectionsLostTimeoutIncrement() {
        Holder h = holder();

        if (h != null) {
            h.connectionsLostTimeout.increment();
        }
    }

    /**
     * Gets total failed handshakes.
     *
     * @return Total failed handshakes.
     */
    public long handshakesFailed() {
        Holder h = holder();

        return h == null ? 0 : h.handshakesFailed.value();
    }

    /**
     * Increments failed handshakes.
     */
    public void handshakesFailedIncrement() {
        Holder h = holder();

        if (h != null) {
            h.handshakesFailed.increment();
        }
    }

    /**
     * Gets total failed handshakes due to a timeout.
     *
     * @return Total failed handshakes due to a timeout.
     */
    public long handshakesFailedTimeout() {
        Holder h = holder();

        return h == null ? 0 : h.handshakesFailedTimeout.value();
    }

    /**
     * Increments failed handshakes due to a timeout.
     */
    public void handshakesFailedTimeoutIncrement() {
        Holder h = holder();

        if (h != null) {
            h.handshakesFailedTimeout.increment();
        }
    }

    /**
     * Gets currently active (in-flight) requests.
     *
     * @return Currently active (in-flight) requests.
     */
    public long requestsActive() {
        Holder h = holder();

        return h == null ? 0 : h.requestsActive.value();
    }

    /**
     * Increments currently active (in-flight) requests.
     */
    public void requestsActiveIncrement() {
        Holder h = holder();

        if (h != null) {
            h.requestsActive.increment();
        }
    }

    /**
     * Decrements currently active (in-flight) requests.
     */
    public void requestsActiveDecrement() {
        Holder h = holder();

        if (h != null) {
            h.requestsActive.decrement();
        }
    }

    /**
     * Gets sent requests.
     *
     * @return Sent requests.
     */
    public long requestsSent() {
        Holder h = holder();

        return h == null ? 0 : h.requestsSent.value();
    }

    /**
     * Increments sent requests.
     */
    public void requestsSentIncrement() {
        Holder h = holder();

        if (h != null) {
            h.requestsSent.increment();
        }
    }

    /**
     * Gets completed requests.
     *
     * @return Completed requests.
     */
    public long requestsCompleted() {
        Holder h = holder();

        return h == null ? 0 : h.requestsCompleted.value();
    }

    /**
     * Increments completed requests.
     */
    public void requestsCompletedIncrement() {
        Holder h = holder();

        if (h != null) {
            h.requestsCompleted.increment();
        }
    }

    /**
     * Gets retried requests.
     *
     * @return Retried requests.
     */
    public long requestsRetried() {
        Holder h = holder();

        return h == null ? 0 : h.requestsRetried.value();
    }

    /**
     * Increments requests completed with retry.
     */
    public void requestsRetriedIncrement() {
        Holder h = holder();

        if (h != null) {
            h.requestsRetried.increment();
        }
    }

    /**
     * Gets failed requests.
     *
     * @return Failed requests.
     */
    public long requestsFailed() {
        Holder h = holder();

        return h == null ? 0 : h.requestsFailed.value();
    }

    /**
     * Increments failed requests.
     */
    public void requestsFailedIncrement() {
        Holder h = holder();

        if (h != null) {
            h.requestsFailed.increment();
        }
    }

    /**
     * Gets total sent bytes.
     *
     * @return Sent bytes.
     */
    public long bytesSent() {
        Holder h = holder();

        return h == null ? 0 : h.bytesSent.value();
    }

    /**
     * Adds sent bytes.
     *
     * @param bytes Sent bytes.
     */
    public void bytesSentAdd(long bytes) {
        Holder h = holder();

        if (h != null) {
            h.bytesSent.add(bytes);
        }
    }

    /**
     * Gets total received bytes.
     *
     * @return Received bytes.
     */
    public long bytesReceived() {
        Holder h = holder();

        return h == null ? 0 : h.bytesReceived.value();
    }

    /**
     * Adds received bytes.
     *
     * @param bytes Received bytes.
     */
    public void bytesReceivedAdd(long bytes) {
        Holder h = holder();

        if (h != null) {
            h.bytesReceived.add(bytes);
        }
    }

    /**
     * Gets streamer batches sent.
     */
    public long streamerBatchesSent() {
        Holder h = holder();

        return h == null ? 0 : h.streamerBatchesSent.value();
    }

    /**
     * Adds streamer batches sent.
     *
     * @param batches Sent batches.
     */
    @Override
    public void streamerBatchesSentAdd(long batches) {
        Holder h = holder();

        if (h != null) {
            h.streamerBatchesSent.add(batches);
        }
    }

    /**
     * Gets streamer items sent.
     */
    public long streamerItemsSent() {
        Holder h = holder();

        return h == null ? 0 : h.streamerItemsSent.value();
    }

    /**
     * Adds streamer items sent.
     *
     * @param items Sent items.
     */
    @Override
    public void streamerItemsSentAdd(long items) {
        Holder h = holder();

        if (h != null) {
            h.streamerItemsSent.add(items);
        }
    }

    /**
     * Gets streamer batches active.
     */
    public long streamerBatchesActive() {
        Holder h = holder();

        return h == null ? 0 : h.streamerBatchesActive.value();
    }

    /**
     * Adds streamer batches active.
     *
     * @param batches Active batches.
     */
    @Override
    public void streamerBatchesActiveAdd(long batches) {
        Holder h = holder();

        if (h != null) {
            h.streamerBatchesActive.add(batches);
        }
    }

    /**
     * Gets streamer items queued.
     */
    public long streamerItemsQueued() {
        Holder h = holder();

        return h == null ? 0 : h.streamerItemsQueued.value();
    }

    /**
     * Adds streamer items queued.
     *
     * @param items Queued items.
     */
    @Override
    public void streamerItemsQueuedAdd(long items) {
        Holder h = holder();

        if (h != null) {
            h.streamerItemsQueued.add(items);
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

    /**
     * Metrics holder.
     */
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

        private final AtomicLongMetric handshakesFailedTimeout =
                new AtomicLongMetric("HandshakesFailedTimeout", "Total failed handshakes due to a timeout");

        private final AtomicLongMetric requestsActive =
                new AtomicLongMetric("RequestsActive", "Currently active requests");

        private final AtomicLongMetric requestsSent =
                new AtomicLongMetric("RequestsSent", "Total requests sent");

        private final AtomicLongMetric requestsCompleted =
                new AtomicLongMetric("RequestsCompleted", "Total requests completed (response received)");

        private final AtomicLongMetric requestsRetried =
                new AtomicLongMetric(
                        "RequestsRetried",
                        "Total requests retries (request sent again after a failure)");

        private final AtomicLongMetric requestsFailed = new AtomicLongMetric("RequestsFailed", "Total requests failed");

        private final AtomicLongMetric bytesSent = new AtomicLongMetric("BytesSent", "Total bytes sent");

        private final AtomicLongMetric bytesReceived = new AtomicLongMetric("BytesReceived", "Total bytes received");

        private final AtomicLongMetric streamerBatchesSent = new AtomicLongMetric(
                "StreamerBatchesSent", "Total data streamer batches sent");

        private final AtomicLongMetric streamerItemsSent = new AtomicLongMetric(
                "StreamerItemsSent", "Total number of data streamer items sent");

        private final AtomicLongMetric streamerBatchesActive = new AtomicLongMetric(
                "StreamerBatchesActive", "Total number of existing data streamer batches");

        private final AtomicLongMetric streamerItemsQueued = new AtomicLongMetric(
                "StreamerItemsQueued", "Total number of queued data streamer items (rows)");

        final List<Metric> metrics = Arrays.asList(
                connectionsActive,
                connectionsEstablished,
                connectionsLost,
                connectionsLostTimeout,
                handshakesFailed,
                handshakesFailedTimeout,
                requestsActive,
                requestsSent,
                requestsCompleted,
                requestsRetried,
                requestsFailed,
                bytesSent,
                bytesReceived,
                streamerBatchesSent,
                streamerItemsSent,
                streamerBatchesActive,
                streamerItemsQueued
        );

        void register(MetricSetBuilder bldr) {
            for (var metric : metrics) {
                bldr.register(metric);
            }
        }
    }
}
