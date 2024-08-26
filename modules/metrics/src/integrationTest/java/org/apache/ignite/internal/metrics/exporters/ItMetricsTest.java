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

import java.nio.file.Path;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration metrics tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItMetricsTest extends BaseIgniteAbstractTest {
    @WorkDirectory
    private Path workDir;

    private Cluster cluster;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        this.cluster = new Cluster(testInfo, workDir);
    }

    @AfterEach
    void tearDown() {
        cluster.shutdown();
    }

    /**
     * Test that will ensure that metric exporter will be started once only despite the fact that start is triggered
     * within both {@code MetricManager#start} and {@code IgniteImpl#recoverComponentsStateOnStart()}.
     */
    @Test
    void testMetricExporterStartsOnceOnly() {
        cluster.startAndInit(1, builder -> builder.clusterConfiguration(
                "ignite.metrics.exporters.doubleStart.exporterName: doubleStart"));

        assertEquals(1, TestDoubleStartExporter.startCounter());
    }
}
