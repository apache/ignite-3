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

package org.apache.ignite.internal.placementdriver.metrics;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.placementdriver.AssignmentsTracker;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests metric source name and metric names.
 */
@ExtendWith(MockitoExtension.class)
public class PlacementDriverMetricSourceTest extends BaseIgniteAbstractTest {
    @Mock
    private LeaseTracker leaseTracker;
    @Mock
    private AssignmentsTracker assignmentsTracker;

    @Test
    void testMetricSourceName() {
        assertThat(PlacementDriverMetricSource.SOURCE_NAME, is("placement-driver"));
    }

    @Test
    void testMetricNames() {
        var metricSource = new PlacementDriverMetricSource(leaseTracker, assignmentsTracker, mock(ClockService.class));

        MetricSet set = metricSource.enable();

        assertThat(set, is(notNullValue()));

        Set<String> expectedMetrics = Set.of(
                "AcceptedLeases",
                "LeaseNegotiations",
                "ReplicationGroups"
        );

        var actualMetrics = StreamSupport.stream(set.spliterator(), false).map(Metric::name).collect(toSet());

        assertThat(actualMetrics, is(expectedMetrics));
    }
}
