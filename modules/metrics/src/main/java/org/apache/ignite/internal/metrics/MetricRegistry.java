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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.TestOnly;

/**
 * Metric registry. Metrics source (see {@link MetricSource} must be registered in this metrics registry after initialization
 * of corresponding component and must be unregistered in case of component is destroyed or stopped. Metrics registry also
 * provides access to all enabled metrics through corresponding metrics sets. Metrics registry lifetime is equal to the node lifetime.
 */
public class MetricRegistry {
    private final Lock lock = new ReentrantLock();

    /** Registered metric sources. */
    private final Map<String, MetricSource> sources = new HashMap<>();

    /** Enabled metric sets. */
    private final Map<String, MetricSet> sets = new TreeMap<>();

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
        lock.lock();

        try {
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
     */
    public void unregisterSource(MetricSource src) {
        lock.lock();

        try {
            disable(src.name());

            sources.remove(src.name());
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
    public MetricSet enable(final String srcName) {
        lock.lock();

        try {
            MetricSource src = sources.get(srcName);

            if (src == null) {
                throw new IllegalStateException("Metrics source with given name doesn't exist: " + srcName);
            }

            MetricSet metricSet = src.enable();

            if (metricSet != null) {
                sets.put(srcName, metricSet);

                version++;
            }

            return metricSet;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Disable metric set for the given metric source.
     *
     * @param srcName Metric source name.
     */
    public void disable(final String srcName) {
        lock.lock();

        try {
            MetricSource src = sources.get(srcName);

            if (src == null) {
                throw new IllegalStateException("Metrics source with given name doesn't exists: " + srcName);
            }

            src.disable();

            sets.remove(srcName);

            version++;
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
}
