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

import org.apache.ignite.internal.cli.commands.CliCommandTestInitializedIntegrationBase;

/**
 * Test base for SSL tests with client connector. The cluster is initialized with SSL enabled for clients.
 */
public class CliSslClientConnectorIntegrationTestBase extends CliCommandTestInitializedIntegrationBase {
    static final String keyStorePath = "ssl/keystore.p12";
    static final String keyStorePassword = "changeit";
    static final String trustStorePath = "ssl/truststore.jks";
    static final String trustStorePassword = "changeit";

    @Override
    protected String nodeBootstrapConfigTemplate() {
        return "{\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ {} ]\n"
                + "    },\n"
                + "  },\n"
                + "  clientConnector: {"
                + "    ssl: {\n"
                + "      enabled: " + true + ",\n"
                + "      clientAuth: \"require\",\n"
                + "      keyStore: {\n"
                + "        path: \"" + escapeWindowsPath(getResourcePath(CliSslClientConnectorIntegrationTestBase.class, keyStorePath))
                + "\",\n"
                + "        password: " + keyStorePassword + "\n"
                + "      }, \n"
                + "      trustStore: {\n"
                + "        type: JKS,\n"
                + "        path: \"" + escapeWindowsPath(getResourcePath(CliSslClientConnectorIntegrationTestBase.class, trustStorePath))
                + "\",\n"
                + "        password: " + trustStorePassword + "\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";
    }
}
