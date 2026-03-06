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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.option.ReplicatorOptions;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.junit.jupiter.api.Test;

/**
 * Tests for replicator metrics.
 */
public class ReplicatorMetricSourceTest {
    private static final String GROUP_ID = "test_group";

    @Test
    void testMetricSourceName() {
        Replicator replicator = mock(Replicator.class);
        ReplicatorOptions opts = mock(ReplicatorOptions.class);
        LogManager logManager = mock(LogManager.class);
        when(opts.getLogManager()).thenReturn(logManager);

        var metricSource = new ReplicatorMetricSource(GROUP_ID, replicator, opts);

        assertThat(metricSource.name(), is("raft.replicator." + GROUP_ID));
    }

    @Test
    void testMetricNames() {
        Replicator replicator = mock(Replicator.class);
        ReplicatorOptions opts = mock(ReplicatorOptions.class);
        LogManager logManager = mock(LogManager.class);
        when(opts.getLogManager()).thenReturn(logManager);

        var metricSource = new ReplicatorMetricSource(GROUP_ID, replicator, opts);

        MetricSet set = metricSource.enable();

        assertThat(set, is(notNullValue()));

        Set<String> expectedMetrics = Set.of(
                "InflightCount",
                "LogLags",
                "NextIndex",
                "HeartbeatTimes",
                "InstallSnapshotTimes",
                "ProbeTimes",
                "AppendEntriesDuration",
                "AppendEntriesSize",
                "AppendEntriesBytes",
                "ConsecutiveErrorTimes",
                "State",
                "RunningState",
                "Locked"
        );

        var actualMetrics = new HashSet<String>();
        set.forEach(m -> actualMetrics.add(m.name()));

        assertThat(actualMetrics, is(expectedMetrics));
    }
}

