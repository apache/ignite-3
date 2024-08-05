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

package org.apache.ignite.internal.runner.app;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.internal.testframework.IgniteTestUtils;

/**
 * Helper class for non-Java platform benchmarks (.NET, C++, Python, ...).
 */
public class PlatformBenchmarkNodeRunner {
    /** Test node name. */
    private static final String NODE_NAME = PlatformBenchmarkNodeRunner.class.getCanonicalName();

    /** Nodes bootstrap configuration. */
    private static final Map<String, String> nodesBootstrapCfg = Map.of(
            NODE_NAME, "{\n"
                    + "  \"clientConnector\":{\"port\": 10420,\"idleTimeout\":999000},"
                    + "  \"network\": {\n"
                    + "    \"port\":3344,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\" ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}"
    );

    /** Base path for all temporary folders. */
    private static final Path BASE_PATH = Path.of("target", "work", "PlatformBenchmarkNodeRunner");

    /**
     * Entry point.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Starting benchmark node runner...");

        ResourceLeakDetector.setLevel(Level.DISABLED);

        List<IgniteServer> startedNodes = PlatformTestNodeRunner.startNodes(BASE_PATH, nodesBootstrapCfg);

        Ignite ignite = startedNodes.get(0).api();
        Object clientHandlerModule = IgniteTestUtils.getFieldValue(ignite, "clientHandlerModule");
        ClientHandlerMetricSource metrics = IgniteTestUtils.getFieldValue(clientHandlerModule, "metrics");
        metrics.enable();

        while (true) {
            Thread.sleep(3000);
            System.out.println();
            System.out.println("sessionsActive: " + metrics.sessionsActive());
            System.out.println("sessionsAccepted: " + metrics.sessionsAccepted());
            System.out.println("requestsActive: " + metrics.requestsActive());
            System.out.println("requestsProcessed: " + metrics.requestsProcessed());
        }
    }
}
