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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * Integration metrics tests.
 */
public class ItMetricsTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration("ignite.metrics.exporters.doubleStart.exporterName: doubleStart");
    }

    /**
     * Test that will ensure that metric exporter will be started once only despite the fact that start is triggered
     * within both {@code MetricManager#start} and {@code IgniteImpl#recoverComponentsStateOnStart()}.
     */
    @Test
    void testMetricExporterStartsOnceOnly() {
        assertEquals(1, TestDoubleStartExporter.startCounter());
    }
}
