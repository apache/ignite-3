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

package org.apache.ignite.internal.metrics;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;
import static org.apache.ignite.lang.ErrorGroups.Common.RESOURCE_CLOSING_ERR;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** Base implementation of {@link MetricManager}. */
public abstract class AbstractMetricManager implements MetricManager {
    protected final MetricRegistry registry = new MetricRegistry();

    protected final Map<String, MetricExporter> enabledMetricExporters = new ConcurrentHashMap<>();

    protected final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    @Override
    public void beforeNodeStop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        try {
            closeAllManually(Stream.concat(
                    enabledMetricExporters.values().stream().map(metricExporter -> (ManuallyCloseable) metricExporter::stop),
                    Stream.of(registry)
            ));
        } catch (Exception e) {
            throw new IgniteInternalException(RESOURCE_CLOSING_ERR, e);
        }
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        enabledMetricExporters.clear();

        return nullCompletedFuture();
    }

    @Override
    public void registerSource(MetricSource src) {
        inBusyLock(busyLock, () -> registry.registerSource(src));
    }

    @Override
    public void unregisterSource(MetricSource src) {
        inBusyLockSafe(busyLock, () -> {
            if (metricSources().contains(src)) {
                MetricSet metricSet = getMetricSet(src.name());

                registry.unregisterSource(src);

                removeMetricSet(metricSet);
            }
        });
    }

    @Override
    public void unregisterSource(String srcName) {
        inBusyLockSafe(busyLock, () -> {
            if (metricSources().stream().anyMatch(metricSource -> metricSource.name().equals(srcName))) {
                MetricSet metricSet = getMetricSet(srcName);

                registry.unregisterSource(srcName);

                removeMetricSet(metricSet);
            }
        });
    }

    @Override
    public MetricSet enable(MetricSource src) {
        return inBusyLock(busyLock, () -> {
            MetricSet enabled = registry.enable(src);

            if (enabled != null) {
                enabledMetricExporters.values().forEach(e -> e.addMetricSet(enabled));
            }

            return enabled;
        });
    }

    @Override
    public MetricSet enable(String srcName) {
        return inBusyLock(busyLock, () -> {
            MetricSet enabled = registry.enable(srcName);

            if (enabled != null) {
                enabledMetricExporters.values().forEach(e -> e.addMetricSet(enabled));
            }

            return enabled;
        });
    }

    @Override
    public void disable(MetricSource src) {
        inBusyLockSafe(busyLock, () -> {
            MetricSet metricSet = getMetricSet(src.name());

            registry.disable(src);

            removeMetricSet(metricSet);
        });
    }

    @Override
    public void disable(String srcName) {
        inBusyLockSafe(busyLock, () -> {
            MetricSet metricSet = getMetricSet(srcName);

            registry.disable(srcName);

            removeMetricSet(metricSet);
        });
    }

    private MetricSet getMetricSet(String srcName) {
        return registry.snapshot().metrics().get(srcName);
    }

    private void removeMetricSet(MetricSet metricSet) {
        enabledMetricExporters.values().forEach(e -> e.removeMetricSet(metricSet));
    }

    @Override
    public MetricSnapshot metricSnapshot() {
        return inBusyLock(busyLock, registry::snapshot);
    }

    @Override
    public Collection<MetricSource> metricSources() {
        return inBusyLock(busyLock, registry::metricSources);
    }

    @Override
    public Collection<MetricExporter> enabledExporters() {
        return inBusyLock(busyLock, enabledMetricExporters::values);
    }
}
