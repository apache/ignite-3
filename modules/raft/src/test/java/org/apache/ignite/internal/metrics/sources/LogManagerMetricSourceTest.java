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

package org.apache.ignite.internal.metrics.sources;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.metrics.MetricSet;
import org.junit.jupiter.api.Test;

/**
 * Tests for log manager metrics.
 */
public class LogManagerMetricSourceTest {
    private static final String GROUP_ID = "test_group";

    @Test
    void testMetricSourceName() {
        var metricSource = new LogManagerMetricSource(GROUP_ID);

        assertThat(metricSource.name(), is("raft.logmanager." + GROUP_ID));
    }

    @Test
    void testMetricNames() {
        var metricSource = new LogManagerMetricSource(GROUP_ID);

        MetricSet set = metricSource.enable();

        assertThat(set, is(notNullValue()));

        Set<String> expectedMetrics = Set.of(
                "TruncateLogSuffixDuration",
                "TruncateLogPrefixDuration",
                "AppendLogsCount",
                "AppendLogsSize",
                "AppendLogsDuration"
        );

        var actualMetrics = new HashSet<String>();
        set.forEach(m -> actualMetrics.add(m.name()));

        assertThat(actualMetrics, is(expectedMetrics));
    }
}

