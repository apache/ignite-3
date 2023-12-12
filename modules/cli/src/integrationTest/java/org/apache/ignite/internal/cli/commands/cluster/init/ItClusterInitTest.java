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

package org.apache.ignite.internal.cli.commands.cluster.init;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClusterInitCommand}.
 */
public class ItClusterInitTest extends CliCommandTestNotInitializedIntegrationBase {

    @Test
    @DisplayName("Init cluster with basic authentication")
    void initClusterWithBasicAuthentication() {

        // when
        execute("connect", "http://localhost:10301");

        resetOutput();

        String clusterConfigurationFile = ItClusterInitTest.class.getClassLoader()
                .getResource("cluster-configuration-with-enabled-auth.conf")
                .getPath();

        execute(
                "cluster", "init",
                "--meta-storage-node", CLUSTER_NODE_NAMES.get(0),
                "--cluster-name", "cluster",
                "--cluster-config-file", clusterConfigurationFile
        );

        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Cluster was initialized successfully")
        );

        // then
        awaitClusterInitialized();

        // basic authentication has been enabled
        assertRestIsUnavailable();

        // set basic authentication settings
        execute("cli", "config", "set", "ignite.auth.basic.username=admin");
        execute("cli", "config", "set", "ignite.auth.basic.password=password");

        // REST is available
        assertRestIsAvailable();
    }

    private void assertRestIsUnavailable() {
        resetOutput();
        execute("cluster", "config", "show");

        assertAll(
                () -> assertErrOutputContains("Authentication error"),
                this::assertOutputIsEmpty
        );
    }

    private void assertRestIsAvailable() {
        resetOutput();
        execute("cluster", "config", "show");

        assertAll(
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

}
