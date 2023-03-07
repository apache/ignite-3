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

import java.util.HashMap;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

public class ClientHandlerMetricSource implements MetricSource {
    /** Source name. */
    private static final String SOURCE_NAME = "client-handler";

    private final LongAdderMetric connectionsInitiated =
            new LongAdderMetric("connections.initiated", "Total initiated client connections");

    private final LongAdderMetric sessionsAccepted =
            new LongAdderMetric("sessions.accepted", "Total accepted client sessions");

    private final LongAdderMetric sessionsRejected =
            new LongAdderMetric("sessions.rejected", "Total rejected client sessions");

    private boolean enabled;

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    public LongAdderMetric connectionsInitiated() {
        return connectionsInitiated;
    }

    public LongAdderMetric sessionsAccepted() {
        return sessionsAccepted;
    }

    public LongAdderMetric sessionsRejected() {
        return sessionsRejected;
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        var metrics = new HashMap<String, Metric>();

        metrics.put("sessions.total", connectionsInitiated);

        enabled = true;

        return new MetricSet(SOURCE_NAME, metrics);
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
