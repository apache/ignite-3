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

package org.apache.ignite.internal.metrics.exporters;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.instanceOf;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.exporters.log.LogPushExporter;
import org.junit.jupiter.api.Test;

/**
 * Checks that log exporter is enabled by default.
 */
public class ItLogPushExporterEnabledByDefault extends ClusterPerClassIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    public void checkThatLogExporterIsEnabledByDefault() {
        IgniteImpl ignite = unwrapIgniteImpl(CLUSTER.runningNodes().findAny().orElseThrow());

        MetricManager metricManager = ignite.metricManager();

        assertThat(metricManager.enabledExporters(), containsInRelativeOrder(instanceOf(LogPushExporter.class)));
    }
}
