/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.internal.metrics.MetricManager;
import org.junit.jupiter.api.Test;

public class MetricExportersLoadingTest {

    @Test
    public void test() throws Exception {
        MetricManager metricManager = new MetricManager();

        TestMetricsSource src = new TestMetricsSource("TestMetricsSource");

        metricManager.registerSource(src);

        metricManager.enable(src.name());

        OutputStream pullOutputStream = new ByteArrayOutputStream();
        TestPullMetricExporter.setOutputStream(pullOutputStream);

        OutputStream pushOutputStream = new ByteArrayOutputStream();
        TestPushMetricExporter.setOutputStream(pushOutputStream);

        assertEquals(0, pullOutputStream.toString().length());

        assertEquals(0, pushOutputStream.toString().length());

        metricManager.start();

        src.inc();

        waitForOutput(pushOutputStream,"TestMetricsSource:\nmetric:1");
        assertTrue(pushOutputStream.toString().contains("TestMetricsSource:\nmetric:1"));

        TestPullMetricExporter.requestMetrics();

        waitForOutput(pullOutputStream,"TestMetricsSource:\nmetric:1");
        assertTrue(pullOutputStream.toString().contains("TestMetricsSource:\nmetric:1"));

        metricManager.stop();

        pushOutputStream.close();
        pullOutputStream.close();
    }

    private void waitForOutput(OutputStream outputStream, String content) {
        while (!outputStream.toString().contains(content)) {
            LockSupport.parkNanos(100_000_000);
        }
    }

}
