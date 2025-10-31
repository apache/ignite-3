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

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Metric registry. Metrics source (see {@link MetricSource} must be registered in this metrics registry after initialization
 * of corresponding component and must be unregistered in case of component is destroyed or stopped. Metrics registry also
 * provides access to all enabled metrics through corresponding metrics sets. Metrics registry lifetime is equal to the node lifetime.
 */
public class MetricRegistry implements MetricProvider, ManuallyCloseable {
    private final ReentrantLock lock = new ReentrantLock();

    /** Registered metric sources. */
    private final Map<String, MetricSource> sources = new HashMap<>();

    /**
     * Metrics snapshot. This is a snapshot of metric sets with corresponding version, the values of the metrics in the
     * metric sets that are included into the snapshot, are changed dynamically.
     */
    private volatile MetricSnapshot metricSnapshot = new MetricSnapshot(emptyMap(), 0L);

    /**
     * Register metric source. It must be registered in this metrics registry after initialization of corresponding component
     * and must be unregistered in case of component is destroyed or stopped, see {@link #unregisterSource(MetricSource)}.
     * By registering, the metric source isn't enabled implicitly.
     *
     * @param src Metric source.
     * @throws IllegalStateException If metric source with the given name already exists.
     */
    public void registerSource(MetricSource src) {
        lock.lock();

        try {
            // TODO https://issues.apache.org/jira/browse/IGNITE-26703
            // Metric source shouldn't be enabled before because the second call of MetricSource#enable will return null.
            assert !src.enabled() : "Metric source shouldn't be enabled before registration in registry.";

            MetricSource old = sources.putIfAbsent(src.name(), src);

            if (old != null) {
                throw new IllegalStateException("Metrics source with given name already exists: " + src.name());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unregister metric source. It must be unregistered in case of corresponding component is destroyed or stopped.
     * Metric source is also disabled while unregistered, see {@link #disable(String)}.
     *
     * @param src Metric source.
     * @throws IllegalStateException If the given metric source isn't registered.
     */
    public void unregisterSource(MetricSource src) {
        unregisterSource(src.name());
    }

    /**
     * Unregister metric source. It must be unregistered in case of corresponding component is destroyed or stopped.
     * Metric source is also disabled while unregistered, see {@link #disable(String)}.
     *
     * @param srcName Metric source name.
     * @throws IllegalStateException If the metric source with the given name isn't registered.
     */
    public void unregisterSource(String srcName) {
        lock.lock();

        try {
            disable(srcName);

            sources.remove(srcName);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enable metric set for the given metric source.
     *
     * @param src Metric source.
     * @return Metric set, or {@code null} if the metric set is already enabled.
     * @throws IllegalStateException If metric source isn't registered.
     * @throws IllegalArgumentException If metric source isn't the same as registered.
     */
    public @Nullable MetricSet enable(MetricSource src) {
        lock.lock();

        try {
            MetricSource registered = checkAndGetRegistered(src);

            MetricSet metricSet = registered.enable();

            if (metricSet != null) {
                addMetricSet(src.name(), metricSet);
            }

            return metricSet;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enable metric set for the given metric source.
     *
     * @param srcName Metric source name.
     * @return Metric set, or {@code null} if the metric set is already enabled.
     * @throws IllegalStateException If metric source with the given name doesn't exist.
     */
    public @Nullable MetricSet enable(String srcName) {
        lock.lock();

        try {
            MetricSource src = sources.get(srcName);

            if (src == null) {
                throw new IllegalStateException("Metrics source with given name doesn't exist: " + srcName);
            }

            MetricSet metricSet = src.enable();

            if (metricSet != null) {
                addMetricSet(src.name(), metricSet);
            }

            return metricSet;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Disable metric set for the given metric source.
     *
     * @param src Metric source.
     * @throws IllegalStateException If metric source isn't registered.
     * @throws IllegalArgumentException If metric source isn't the same as registered.
     */
    public void disable(MetricSource src) {
        lock.lock();

        try {
            MetricSource registered = checkAndGetRegistered(src);

            if (!registered.enabled()) {
                return;
            }

            registered.disable();

            removeMetricSet(registered.name());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Disable metric set for the given metric source.
     *
     * @param srcName Metric source name.
     * @throws IllegalStateException If metric source with given name doesn't exist.
     */
    public void disable(String srcName) {
        lock.lock();

        try {
            MetricSource src = sources.get(srcName);

            if (src == null) {
                throw new IllegalStateException("Metrics source with given name doesn't exist: " + srcName);
            }

            if (!src.enabled()) {
                return;
            }

            src.disable();

            removeMetricSet(srcName);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check that the given metric source is registered. This method should be called under the {@link MetricRegistry#lock}.
     *
     * @param src Metric source.
     * @return Registered metric source.
     * @throws IllegalStateException If metric source isn't registered.
     * @throws IllegalArgumentException If metric source isn't the same as registered.
     */
    private MetricSource checkAndGetRegistered(MetricSource src) {
        assert lock.isHeldByCurrentThread() : "Access to shared state from an incorrect thread " + Thread.currentThread().getName();

        requireNonNull(src);

        MetricSource registered = sources.get(src.name());

        if (registered == null) {
            throw new IllegalStateException("Metrics source isn't registered: " + src.name());
        }

        if (!src.equals(registered)) {
            throw new IllegalArgumentException("Given metric source is not the same as registered by the same name: " + src.name());
        }

        return registered;
    }

    /**
     * Add metric set to {@link MetricRegistry#metricSnapshot}. This creates new version of metric snapshot. This method should be
     * called under the {@link MetricRegistry#lock}.
     *
     * @param srcName Metric source name.
     * @param metricSet Metric set.
     */
    private void addMetricSet(String srcName, MetricSet metricSet) {
        assert lock.isHeldByCurrentThread() : "Access to a shared state from an incorrect thread " + Thread.currentThread().getName();

        SortedMap<String, MetricSet> metricSets = new TreeMap<>(metricSnapshot.metrics());

        metricSets.put(srcName, metricSet);

        updateMetricSnapshot(metricSets);
    }

    /**
     * Removes metric set from {@link MetricRegistry#metricSnapshot}. This creates new version of metric snapshot. This method should be
     * called under the {@link MetricRegistry#lock}.
     *
     * @param srcName Metric source name.
     */
    private void removeMetricSet(String srcName) {
        assert lock.isHeldByCurrentThread() : "Access to a shared state from an incorrect thread " + Thread.currentThread().getName();

        SortedMap<String, MetricSet> metricSets = new TreeMap<>(metricSnapshot.metrics());

        metricSets.remove(srcName);

        updateMetricSnapshot(metricSets);
    }

    /**
     * Create a new version of {@link MetricRegistry#metricSnapshot}. This method should be called under the {@link MetricRegistry#lock}.
     *
     * @param metricSets New map of metric sets that should be saved to new version of metric snapshot.
     */
    private void updateMetricSnapshot(SortedMap<String, MetricSet> metricSets) {
        assert lock.isHeldByCurrentThread() : "Access to shared state from an incorrect thread " + Thread.currentThread().getName();

        MetricSnapshot old = metricSnapshot;

        metricSnapshot = new MetricSnapshot(unmodifiableMap(metricSets), old.version() + 1);
    }

    @Override
    public MetricSnapshot snapshot() {
        return metricSnapshot;
    }

    /**
     * Gets a collection of registered metric sources.
     *
     * @return Metric sources.
     */
    public Collection<MetricSource> metricSources() {
        lock.lock();
        try {
            return List.copyOf(sources.values());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        lock.lock();

        try {
            sources.values().forEach(MetricSource::disable);
        } finally {
            lock.unlock();
        }
    }
}
