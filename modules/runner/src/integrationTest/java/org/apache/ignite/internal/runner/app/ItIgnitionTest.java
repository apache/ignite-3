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

package org.apache.ignite.internal.runner.app;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.ItUtils;
import org.apache.ignite.internal.app.IgnitionImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Ignition interface tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
class ItIgnitionTest {
    /** Network ports of the test nodes. */
    private static final int[] PORTS = {3344, 3345, 3346};

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    /** Collection of started nodes. */
    private final List<Ignite> startedNodes = new ArrayList<>();

    /** Path to the working directory. */
    @WorkDirectory
    private Path workDir;

    /**
     * Before each.
     */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        String node0Name = testNodeName(testInfo, PORTS[0]);
        String node1Name = testNodeName(testInfo, PORTS[1]);
        String node2Name = testNodeName(testInfo, PORTS[2]);

        nodesBootstrapCfg.put(
                node0Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[0] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );

        nodesBootstrapCfg.put(
                node1Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[1] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );

        nodesBootstrapCfg.put(
                node2Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[2] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(ItUtils.reverse(startedNodes));
    }

    /**
     * Check that Ignition.start() with bootstrap configuration returns Ignite instance.
     */
    @Test
    void testNodesStartWithBootstrapConfiguration() {
        nodesBootstrapCfg.forEach((nodeName, configStr) ->
                startedNodes.add(IgnitionManager.start(nodeName, configStr, workDir.resolve(nodeName)))
        );

        Assertions.assertEquals(3, startedNodes.size());

        startedNodes.forEach(Assertions::assertNotNull);
    }

    /**
     * Check that Ignition.start() with bootstrap configuration returns Ignite instance.
     */
    @Test
    void testNodeStartWithoutBootstrapConfiguration(TestInfo testInfo) {
        startedNodes.add(IgnitionManager.start(testNodeName(testInfo, 47500), null, workDir));

        Assertions.assertNotNull(startedNodes.get(0));
    }

    /**
     * Tests scenario when we try to start node with invalid configuration.
     */
    @Test
    void testErrorWhenStartNodeWithInvalidConfiguration() {
        try {
            startedNodes.add(IgnitionManager.start("invalid-config-name",
                    "{Invalid-Configuration}",
                    workDir.resolve("invalid-config-name"))
            );

            fail();
        } catch (Throwable t) {
            assertTrue(IgniteTestUtils.hasCause(t,
                    IgniteException.class,
                    "Unable to parse user-specific configuration."
            ));
        }
    }

    /**
     * Tests scenario when we try to start node with URL configuration.
     */
    @Test
    void testStartNodeWithUrlConfig() throws Exception {
        Ignition ign = new IgnitionImpl();

        String nodeName = "node-url-config";

        String cfg = "{\n"
                + "  network: {\n"
                + "    port: " + PORTS[0] + "\n"
                + "  }\n"
                + "}";

        URL url = buildUrl("testURL.txt", cfg);

        startedNodes.add(ign.start(nodeName, url, workDir.resolve(nodeName)));
    }

    /**
     * Test URL with content in memory.
     *
     * @param path URL path.
     * @param data Data is available by URL.
     * @return URL.
     * @throws Exception If failed.
     */
    private URL buildUrl(String path, String data) throws Exception {
        URLStreamHandler handler = new URLStreamHandler() {
            private final byte[] content = data.getBytes(StandardCharsets.UTF_8);

            @Override
            protected URLConnection openConnection(URL url) {
                return new URLConnection(url) {
                    @Override
                    public void connect() {
                        connected = true;
                    }

                    @Override
                    public long getContentLengthLong() {
                        return content.length;
                    }

                    @Override
                    public InputStream getInputStream() {
                        return new ByteArrayInputStream(content);
                    }
                };
            }
        };

        return new URL("memory", "", -1, path, handler);
    }
}
