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

import org.jetbrains.annotations.Nullable;

/**
 * Interface for all metrics sources. Metric source provides access to related metrics. Metrics source always corresponds
 * to some component or entity of the system (e.g. node is a metrics source, storage engine is a metric source). Metrics source
 * has a unique name and type (e.g. class name). The metrics source exposes an interface for modification of metrics values.
 * Instead of looking up metrics by a name and modifying metrics value directly, developers must use methods defined in the
 * metrics source.
 */
public interface MetricSource {
    /** Returns metric source name. */
    String name();

    /**
     * Returns metrics source description.
     *
     * @return Metrics source description.
     **/
    default @Nullable String description() {
        return null;
    }

    /**
     * Returns a group name for this metric source.
     * In general, a group name is a prefix of the source name.
     * For example, thread pool metric sources have the following group: {@code thread.pools}.
     * This group name allows adding additional grouping in external systems like JConsole.
     *
     * @return Group name.
     */
    default @Nullable String group() {
        return null;
    }

    /**
     * Enables metrics for metric source. Creates and returns {@link MetricSet} built during enabling. Nothing happens if
     * the metrics are already enabled for this source.
     *
     * @return Newly created {@link MetricSet} instance or {@code null} if metrics are already enabled.
     */
    @Nullable MetricSet enable();

    // TODO https://issues.apache.org/jira/browse/IGNITE-26702
    /** Disables metrics for metric source. Nothing happens if the metrics are already disabled for this source. */
    void disable();

    /**
     * Checks whether metrics is enabled (switched on) or not (switched off) for metric source.
     *
     * @return {@code true} if metrics are enabled, otherwise - {@code false}.
     */
    boolean enabled();
}
