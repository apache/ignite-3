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

package org.apache.ignite.internal.metrics.logstorage;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogStorageMetricSourceTest {
    private final LogStorageMetricSource metricSource = new LogStorageMetricSource();

    private MetricSet metricSet;

    @BeforeEach
    void setUp() {
        MetricSet ms = metricSource.enable();
        assertThat(ms, is(notNullValue()));
        metricSet = ms;
    }

    @Test
    void metricSetIsAsExpected() {
        Set<String> metricNames = StreamSupport.stream(metricSet.spliterator(), false)
                .map(Metric::name)
                .collect(toUnmodifiableSet());

        assertThat(
                metricNames,
                is(Set.of("CmgLogStorageSize", "MetastorageLogStorageSize", "PartitionsLogStorageSize", "TotalLogStorageSize"))
        );
    }

    @Test
    void metricsAreInitializedToZero() {
        assertThatLongGaugeHasValue("CmgLogStorageSize", 0);
        assertThatLongGaugeHasValue("MetastorageLogStorageSize", 0);
        assertThatLongGaugeHasValue("PartitionsLogStorageSize", 0);
        assertThatLongGaugeHasValue("TotalLogStorageSize", 0);
    }

    private void assertThatLongGaugeHasValue(String metricName, long expectedValue) {
        LongGauge gauge = metricSet.get(metricName);

        assertThat(gauge, is(notNullValue()));
        assertThat(gauge.value(), is(expectedValue));
    }

    @Test
    void cmgLogStorageSizeIsUpdated() {
        metricSource.cmgLogStorageSize(100);

        assertThatLongGaugeHasValue("CmgLogStorageSize", 100);
    }

    @Test
    void metastorageLogStorageSizeIsUpdated() {
        metricSource.metastorageLogStorageSize(200);

        assertThatLongGaugeHasValue("MetastorageLogStorageSize", 200);
    }

    @Test
    void partitionsLogStorageSizeIsUpdated() {
        metricSource.partitionsLogStorageSize(300);

        assertThatLongGaugeHasValue("PartitionsLogStorageSize", 300);
    }

    @Test
    void totalLogStorageSizeIsUpdated() {
        metricSource.cmgLogStorageSize(1);
        metricSource.metastorageLogStorageSize(10);
        metricSource.partitionsLogStorageSize(100);

        assertThatLongGaugeHasValue("TotalLogStorageSize", 111);
    }
}
