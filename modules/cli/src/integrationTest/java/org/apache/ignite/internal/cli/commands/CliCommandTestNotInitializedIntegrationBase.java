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

package org.apache.ignite.internal.cli.commands;

import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration test base for cli commands. Setup commands, ignite cluster, and provides useful fixtures and assertions. Note: ignite cluster
 * won't be initialized. If you want to use initialized cluster use {@link CliIntegrationTest} directly.
 */
public class CliCommandTestNotInitializedIntegrationBase extends CliIntegrationTest {

    protected static String CLUSTER_NOT_INITIALIZED_ERROR_MESSAGE = "Probably, you have not initialized the cluster, "
            + "try to run ignite cluster init command";

    protected static String CLUSTER_NOT_INITIALIZED_REPL_ERROR_MESSAGE = "Probably, you have not initialized the cluster, "
            + "try to run cluster init command";

    @BeforeAll
    @Override
    protected void beforeAll(TestInfo testInfo) {
        CLUSTER = new Cluster(testInfo, WORK_DIR, getNodeBootstrapConfigTemplate());

        for (int i = 0; i < initialNodes(); i++) {
            CLUSTER.startEmbeddedNode(i);
        }
    }
}
