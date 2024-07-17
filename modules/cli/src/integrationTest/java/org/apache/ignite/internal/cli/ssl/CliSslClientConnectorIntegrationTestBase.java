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

import org.apache.ignite.internal.NodeConfig;
import org.apache.ignite.internal.cli.CliIntegrationTest;

/**
 * Test base for SSL tests with client connector. The cluster is initialized with SSL enabled for clients.
 */
public class CliSslClientConnectorIntegrationTestBase extends CliIntegrationTest {

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return NodeConfig.CLIENT_CONNECTOR_SSL_BOOTSTRAP_CONFIG;
    }
}
