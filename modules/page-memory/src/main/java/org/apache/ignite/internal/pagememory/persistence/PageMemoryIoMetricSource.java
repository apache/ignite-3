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

package org.apache.ignite.internal.pagememory.persistence;

import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.pagememory.metrics.CollectionMetricSource;

/**
 * Metric source for page memory byte-level I/O operations.
 *
 * <p>This metric source tracks physical I/O performance including bytes transferred,
 * operation sizes, and latencies at the file I/O level.
 */
public class PageMemoryIoMetricSource extends CollectionMetricSource {
    /**
     * Constructor.
     */
    public PageMemoryIoMetricSource(String name) {
        super(name, "storage", "Page memory byte-level I/O metrics");
    }

    @Override
    public synchronized <T extends Metric> T addMetric(T metric) {
        return super.addMetric(metric);
    }
}
