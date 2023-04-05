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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getResourcePath;

import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;

/**
 * Integration test base for SSL tests.
 */
public class CliSslNotInitializedIntegrationTestBase extends CliCommandTestNotInitializedIntegrationBase {
    protected static final String keyStorePath = "ssl/keystore.p12";
    protected static final String keyStorePassword = "changeit";
    protected static final String trustStorePath = "ssl/truststore.jks";
    protected static final String trustStorePassword = "changeit";

    static final String resolvedKeystorePath = getResourcePath(CliSslClientConnectorIntegrationTestBase.class, keyStorePath);
    static final String resolvedTruststorePath = getResourcePath(CliSslClientConnectorIntegrationTestBase.class, trustStorePath);

    /**
     * Template for node bootstrap config with Scalecube and Logical Topology settings for fast failure detection.
     */
    protected static final String REST_SSL_BOOTSTRAP_CONFIG = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    },\n"
            + "  },\n"
            + "  rest: {"
            + "    dualProtocol: " + true + ",\n"
            + "    ssl: {\n"
            + "      enabled: " + true + ",\n"
            + "      clientAuth: \"require\",\n"
            + "      keyStore: {\n"
            + "        path: \"" +  escapeWindowsPath(resolvedKeystorePath) + "\",\n"
            + "        password: " + keyStorePassword + "\n"
            + "      }, \n"
            + "      trustStore: {\n"
            + "        type: JKS,\n"
            + "        path: \"" + escapeWindowsPath(resolvedTruststorePath) + "\",\n"
            + "        password: " + trustStorePassword + "\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

    @Override
    protected String nodeBootstrapConfigTemplate() {
        return REST_SSL_BOOTSTRAP_CONFIG;
    }
}
