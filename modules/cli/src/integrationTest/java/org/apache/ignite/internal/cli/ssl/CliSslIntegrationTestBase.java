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

package org.apache.ignite.internal.cli.ssl;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;

/**
 * Integration test base for SSL tests.
 */
public class CliSslIntegrationTestBase extends CliCommandTestNotInitializedIntegrationBase {

    private static final String keyStorePath = "ssl/keystore.p12";
    private static final String keyStorePassword = "changeit";
    private static final String trustStorePath = "ssl/truststore.jks";
    private static final String trustStorePassword = "changeit";

    /**
     * Template for node bootstrap config with Scalecube and Logical Topology settings for fast failure detection.
     */
    protected static final String FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    },\n"
            + "    membership: {\n"
            + "      membershipSyncInterval: 1000,\n"
            + "      failurePingInterval: 500,\n"
            + "      scaleCube: {\n"
            + "        membershipSuspicionMultiplier: 1,\n"
            + "        failurePingRequestMembers: 1,\n"
            + "        gossipInterval: 10\n"
            + "      },\n"
            + "    }\n"
            + "  },\n"
            + "  rest: {"
            + "    dualProtocol: " + true + ",\n"
            + "    ssl: {\n"
            + "      enabled: " + true + ",\n"
            + "      clientAuth: \"require\",\n"
            + "      keyStore: {\n"
            + "        path: \"" + getResourcePath(keyStorePath) + "\",\n"
            + "        password: " + keyStorePassword + "\n"
            + "      }, \n"
            + "      trustStore: {\n"
            + "        type: JKS,\n"
            + "        path: \"" + getResourcePath(trustStorePath) + "\",\n"
            + "        password: " + trustStorePassword + "\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  cluster.failoverTimeout: 100\n"
            + "}";

    @Override
    protected String nodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    protected static String getResourcePath(String resource) {
        try {
            URL url = CliSslIntegrationTestBase.class.getClassLoader().getResource(resource);
            Objects.requireNonNull(url, "Resource " + resource + " not found.");
            Path path = Path.of(url.toURI()); // Properly extract file system path from the "file:" URL
            return path.toString().replace("\\", "\\\\"); // Escape backslashes for the config parser
        } catch (URISyntaxException e) {
            throw new RuntimeException(e); // Shouldn't happen since URL is obtained from the class loader
        }
    }
}
