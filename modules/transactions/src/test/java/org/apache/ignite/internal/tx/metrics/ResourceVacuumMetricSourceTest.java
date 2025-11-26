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

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests metric source name and metric names.
 */
public class ResourceVacuumMetricSourceTest extends BaseIgniteAbstractTest {
    @Test
    void testMetricSourceName() {
        assertThat(ResourceVacuumMetrics.SOURCE_NAME, is("resource.vacuum"));
    }

    @Test
    void testMetricNames() {
        var metricSource = new ResourceVacuumMetrics();

        MetricSet set = metricSource.enable();

        assertThat(set, is(notNullValue()));

        Set<String> expectedMetrics = Set.of(
                "VacuumizedVolatileTxnMetaCount",
                "VacuumizedPersistentTransactionMetaCount",
                "MarkedForVacuumTransactionMetaCount",
                "SkippedForFurtherProcessingUnfinishedTransactionCount"
        );

        var actualMetrics = StreamSupport.stream(set.spliterator(), false).map(Metric::name).collect(toSet());

        assertThat(actualMetrics, is(expectedMetrics));
    }
}
