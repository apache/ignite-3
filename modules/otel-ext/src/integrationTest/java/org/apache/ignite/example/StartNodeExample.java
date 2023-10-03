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

package org.apache.ignite.example;

import static java.util.stream.Collectors.toList;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.lang.IgniteStringFormatter;

/**
 * Tests for transactional examples.
 */
public class StartNodeExample {
    /** Base port number. */
    private static final int BASE_PORT = 3344;

    /** Base thin client port number. */
    private static final int BASE_CLIENT_PORT = 10800;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    portRange: 10,\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes:  [ {} ] \n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} }\n"
            + "}";

    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Invalid argument count");
        }

        int nodeIndex = Integer.parseInt(args[0]);
        boolean activate = Boolean.parseBoolean(args[1]);

        var workDir = Path.of("work");

        var nodeName = "ignite-node-" + nodeIndex;

        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        var cfgString = IgniteStringFormatter.format(
                NODE_BOOTSTRAP_CFG_TEMPLATE,
                BASE_PORT + nodeIndex,
                connectNodeAddr,
                BASE_CLIENT_PORT + nodeIndex
        );

        var igniteFuture = TestIgnitionManager.start(nodeName, cfgString, workDir.resolve(nodeName));

        if (activate) {
            InitParameters initParameters = InitParameters.builder()
                    .destinationNodeName(nodeName)
                    .metaStorageNodeNames(IntStream.range(0, nodeIndex + 1).mapToObj(i -> "ignite-node-" + i).collect(toList()))
                    .clusterName("cluster")
                    .build();

            TimeUnit.SECONDS.sleep(10L);

            IgnitionManager.init(initParameters);

            igniteFuture.join();
        }

        TimeUnit.HOURS.sleep(3);
    }
}
