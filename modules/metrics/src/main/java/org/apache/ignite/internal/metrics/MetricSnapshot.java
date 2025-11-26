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

import java.util.Collections;
import java.util.Map;

/**
 * Represents a collection of metric sets with a version.
 */
public class MetricSnapshot {
    /** Mapping of source name to the corresponding metric set. */
    private final Map<String, MetricSet> metrics;

    /** Snapshot version. */
    private final long version;

    public MetricSnapshot(Map<String, MetricSet> metrics, long version) {
        this.metrics = Collections.unmodifiableMap(metrics);
        this.version = version;
    }

    /**
     * Returns a collection of metrics sets included into this snapshot.
     *
     * @return Collection of metrics sets included into this snapshot.
     */
    public Map<String, MetricSet> metrics() {
        return metrics;
    }

    /**
     * Returns version of this snapshot.
     *
     * @return Snapshot version.
     */
    public long version() {
        return version;
    }
}
