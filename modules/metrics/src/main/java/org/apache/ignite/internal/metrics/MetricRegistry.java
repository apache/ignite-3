/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.ignite.internal.util.FilteringIterator;
import org.apache.ignite.internal.util.TransformingIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/**
 * Metric registry. Metrics source (see {@link MetricSource} must be registered in this metrics registry after initialization
 * of corresponding component and must be unregistered in case of component is destroyed or stopped. Metrics registry also
 * provides access to all enabled metrics through corresponding metrics sets. Metrics registry lifetime is equal to the node lifetime.
 * <br>
 * Implements an {@link Iterable} over the metric sets for enabled metric sources.
 */
public class MetricRegistry implements Iterable<MetricSet> {
    private final Lock lock = new ReentrantLock();

    /** Map of metric sources' names to tuples of registered sources with metric sets, if enabled. */
    private volatile Map<String, IgniteBiTuple<MetricSource, MetricSet>> sources = new TreeMap<>();

    /** Version always should be changed on metrics enabled/disabled action. */
    private volatile long version;

    /**
     * Register metric source. It must be registered in this metrics registry after initialization of corresponding component
     * and must be unregistered in case of component is destroyed or stopped, see {@link #unregisterSource(MetricSource)}.
     * By registering, the metric source isn't enabled implicitly.
     *
     * @param src Metric source.
     * @throws IllegalStateException If metric source with the given name already exists.
     */
    public void registerSource(MetricSource src) {
        modifySources(sources -> {
            IgniteBiTuple<MetricSource, MetricSet> s = new IgniteBiTuple<>(src, null);

            IgniteBiTuple<MetricSource, MetricSet> old = sources.putIfAbsent(src.name(), s);

            if (old != null) {
                throw new IllegalStateException("Metrics source with given name already exists: " + src.name());
            }

            // Now we sure that this metric source wasn't registered before.
            assert !src.enabled() : "Metric source shouldn't be enabled before registration in registry.";

            return true;
        });
    }

    /**
     * Unregister metric source. It must be unregistered in case of corresponding component is destroyed or stopped.
     * Metric source is also disabled while unregistered, see {@link #disable(String)}.
     *
     * @param src Metric source.
     */
    public void unregisterSource(MetricSource src) {
        unregisterSource(src.name());
    }

    /**
     * Unregister metric source. It must be unregistered in case of corresponding component is destroyed or stopped.
     * Metric source is also disabled while unregistered, see {@link #disable(String)}.
     *
     * @param srcName Metric source name.
     */
    public void unregisterSource(String srcName) {
        modifySources(sources -> {
            IgniteBiTuple<MetricSource, MetricSet> s = sources.get(srcName);

            if (s == null) {
                return false;
            }

            assert s.get1() != null;

            s.get1().disable();

            sources.remove(srcName);

            return true;
        });
    }

    /**
     * Enable metric set for the given metric source.
     *
     * @param src Metric source.
     * @return Metric set, or {@code null} if the metric set is already enabled.
     * @throws IllegalStateException If metric source isn't registered.
     * @throws IllegalArgumentException If metric source isn't the same as registered.
     */
    public MetricSet enable(@NotNull MetricSource src) {
        AtomicReference<MetricSet> metricSetRef = new AtomicReference<>();

        modifySources(sources -> {
            IgniteBiTuple<MetricSource, MetricSet> registered = checkRegistered(sources, src);

            if (registered.get2() != null) {
                assert src.enabled();
                return false;
            }

            MetricSet metricSet = src.enable();

            assert metricSet != null;

            metricSetRef.set(metricSet);

            IgniteBiTuple<MetricSource, MetricSet> updated = new IgniteBiTuple<>(src, metricSet);

            sources.put(src.name(), updated);

            metricSetRef.set(metricSet);

            return true;
        });

        return metricSetRef.get();
    }

    /**
     * Enable metric set for the given metric source.
     *
     * @param srcName Metric source name.
     * @return Metric set, or {@code null} if the metric set is already enabled.
     * @throws IllegalStateException If metric source with the given name doesn't exist.
     */
    public MetricSet enable(final String srcName) {
        AtomicReference<MetricSet> metricSetRef = new AtomicReference<>();

        modifySources(sources -> {
            IgniteBiTuple<MetricSource, MetricSet> registered = sources.get(srcName);

            if (registered == null) {
                throw new IllegalStateException("Metrics source with given name doesn't exist: " + srcName);
            }

            MetricSource src = registered.get1();

            if (registered.get2() != null) {
                assert src.enabled();
                return false;
            }

            MetricSet metricSet = src.enable();
            assert metricSet != null;

            IgniteBiTuple<MetricSource, MetricSet> updated = new IgniteBiTuple<>(src, metricSet);

            sources.put(src.name(), updated);

            metricSetRef.set(metricSet);

            return true;
        });

        return metricSetRef.get();
    }

    /**
     * Disable metric set for the given metric source.
     *
     * @param src Metric source.
     * @throws IllegalStateException If metric source isn't registered.
     * @throws IllegalArgumentException If metric source isn't the same as registered.
     */
    public void disable(@NotNull MetricSource src) {
        modifySources(sources -> {
            checkRegistered(sources, src);

            src.disable();

            IgniteBiTuple<MetricSource, MetricSet> updated = new IgniteBiTuple<>(src, null);

            sources.put(src.name(), updated);

            return true;
        });
    }

    /**
     * Disable metric set for the given metric source.
     *
     * @param srcName Metric source name.
     * @throws IllegalStateException If metric source with given name doesn't exists.
     */
    public void disable(final String srcName) {
        modifySources(sources -> {
            IgniteBiTuple<MetricSource, MetricSet> registered = sources.get(srcName);

            if (registered == null) {
                throw new IllegalStateException("Metrics source with given name doesn't exists: " + srcName);
            }

            MetricSource src = registered.get1();

            src.disable();

            IgniteBiTuple<MetricSource, MetricSet> updated = new IgniteBiTuple<>(src, null);

            sources.put(src.name(), updated);

            return true;
        });
    }

    /**
     * Check that the given metric source is registered.
     *
     * @param sources Sources map.
     * @param src Metric source.
     * @return Registered pair of metric source and metric set.
     * @throws IllegalStateException If metric source isn't registered.
     * @throws IllegalArgumentException If metric source isn't the same as registered.
     */
    @NotNull
    private IgniteBiTuple<MetricSource, MetricSet> checkRegistered(
        Map<String, IgniteBiTuple<MetricSource, MetricSet>> sources,
        @NotNull MetricSource src
    ) {
        requireNonNull(src);

        IgniteBiTuple<MetricSource, MetricSet> registered = sources.get(src.name());

        if (registered == null) {
            throw new IllegalStateException("Metrics source isn't registered: " + src.name());
        }

        if (!src.equals(registered.get1())) {
            throw new IllegalArgumentException("Given metric source is not the same as registered by the same name: " + src.name());
        }

        return registered;
    }

    /**
     * Updates {@link MetricRegistry#sources} map using copy-on-write principle. Increments version of registry.
     *
     * @param modifier Modifier for the sources map. Accepts the new, modifiable version of the map. Returns boolean value, whether
     *                 the map was modified.
     */
    private void modifySources(Function<Map<String, IgniteBiTuple<MetricSource, MetricSet>>, Boolean> modifier) {
        lock.lock();

        try {
            Map<String, IgniteBiTuple<MetricSource, MetricSet>> sources0 = new TreeMap<>(sources);

            boolean modified = modifier.apply(sources0);

            if (modified) {
                sources = sources0;

                version++;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns registry schema version.
     *
     * @return Version.
     */
    public long version() {
        return version;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<MetricSet> iterator() {
        return new TransformingIterator<>(
            new FilteringIterator<>(sources.values().iterator(), v -> v.get2() != null),
            IgniteBiTuple::get2
        );
    }
}
