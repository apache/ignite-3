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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;
import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClusterInitCommand}.
 *
 * <p>Because the {@link org.apache.ignite.internal.cli.CliIntegrationTest} extends
 * {@link org.apache.ignite.internal.ClusterPerClassIntegrationTest}, each CLI test case for init has to be placed in a separate
 * test class. It'd ideal to refactor the base classes to have a CLI init test class with multiple test cases.
 * This may be needed if more tests are added.
 */
public class ItClusterInitTest extends CliCommandTestNotInitializedIntegrationBase {
    @Test
    @DisplayName("Init cluster with basic authentication")
    void initClusterWithBasicAuthentication() throws InterruptedException {

        // when
        connect(NODE_URL);

        File clusterConfigurationFile = TestConfigManagerHelper.readClusterConfigurationWithEnabledAuthFile();

        execute(
                "cluster", "init",
                "--metastorage-group", CLUSTER.nodeName(1),
                "--cluster-management-group", CLUSTER.nodeName(2),
                "--name", "cluster",
                "--config-files", clusterConfigurationFile.getAbsolutePath()
        );

        assertAll(
                this::assertExitCodeIsZero,
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

        execute("cluster", "topology", "logical");
        assertExitCodeIsZero();
        for (int i = 0; i < initialNodes(); i++) {
            assertOutputContains(CLUSTER.nodeName(i));
        }
    }

    private void awaitClusterInitialized() throws InterruptedException {
        assertTrue(waitForCondition(() -> CLUSTER.runningNodes().count() == initialNodes(), 30_000));
    }

    private void assertRestIsUnavailable() {
        execute("cluster", "config", "show");

        assertAll(
                () -> assertErrOutputContains("Authentication error"),
                this::assertOutputIsEmpty
        );
    }

    private void assertRestIsAvailable() {
        execute("cluster", "config", "show");

        assertAll(
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }
}
