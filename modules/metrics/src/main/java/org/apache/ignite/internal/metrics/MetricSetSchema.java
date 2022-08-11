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

import java.util.Collections;
import java.util.List;

/**
 * Metric set schema. Contains schema version and corresponding metric sets.
 */
public class MetricSetSchema {
    /** Version. */
    private final long version;

    /** Metric sets. */
    private final List<MetricSet> metricSets;

    /**
     * Constructor.
     *
     * @param version Version.
     * @param metricSets Metric sets list.
     */
    public MetricSetSchema(long version, List<MetricSet> metricSets) {
        this.version = version;
        this.metricSets = Collections.unmodifiableList(metricSets);
    }

    /** Version. */
    public long version() {
        return version;
    }

    /** Metric sets. */
    public List<MetricSet> metricSets() {
        return metricSets;
    }
}
