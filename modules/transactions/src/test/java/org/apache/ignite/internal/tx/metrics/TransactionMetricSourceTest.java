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

package org.apache.ignite.internal.tx.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests metric source name and transaction metric names.
 * If you want to change the name, or add a new metric, please don't forget to update the corresponding documentation.
 */
@ExtendWith(MockitoExtension.class)
public class TransactionMetricSourceTest extends BaseIgniteAbstractTest {
    @Mock
    ClockService clockService;

    @Test
    void testMetricSourceName() {
        assertThat(TransactionMetricsSource.SOURCE_NAME, is("transactions"));
    }

    @Test
    void testMetricNames() {
        var metricSource = new TransactionMetricsSource(clockService);

        MetricSet set = metricSource.enable();

        assertThat(set, is(notNullValue()));

        Set<String> expectedMetrics = Set.of(
                "TotalCommits",
                "TotalRollbacks",
                "RwCommits",
                "RwRollbacks",
                "RoCommits",
                "RoRollbacks",
                "RwDuration",
                "RoDuration",
                "PendingWriteIntents");

        var actualMetrics = new HashSet<String>();
        set.forEach(m -> actualMetrics.add(m.name()));

        assertThat(actualMetrics, is(expectedMetrics));
    }
}
