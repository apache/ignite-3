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

package org.apache.ignite.internal.component;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.app.IgniteRunner;
import org.apache.ignite.internal.runner.app.IgniteRunnerTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test that after Ignite is started there is a file with REST server address in working directory.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItRestAddressReportTest extends IgniteIntegrationTest {
    private static final String NODE_NAME = "node";

    @WorkDirectory
    private Path workDir;

    private static int cutPort(String retAddress) {
        return Integer.parseInt(retAddress.split(":")[1]);
    }

    @Test
    @DisplayName("Should report rest port to the file after RestComponent started")
    void restPortReportedToFile() throws Exception {
        // Given configuration with rest port configured rest.port=10333, rest.portRange=10
        Path configPath = Path.of(IgniteRunnerTest.class.getResource("/ignite-config-rest-port-not-default.json").toURI());

        // When start node
        CompletableFuture<Ignite> ign = IgniteRunner.start(
                "--config-path", configPath.toAbsolutePath().toString(),
                "--work-dir", workDir.resolve(NODE_NAME).toAbsolutePath().toString(),
                "--node-name", NODE_NAME
        );

        // And init cluster
        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(NODE_NAME)
                .metaStorageNodeNames(List.of(NODE_NAME))
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(initParameters);

        // Then node is started
        assertThat(ign, willCompleteSuccessfully());

        // And there is a file in work dir with the rest address
        File reportFile = workDir.resolve(NODE_NAME).resolve("rest-address").toFile();
        assertThat(reportFile.exists(), is(true));

        // And the file contains valid rest server network address
        URI restUri = URI.create(Files.readString(workDir.resolve(NODE_NAME).resolve("rest-address")));
        assertThat(restUri.getHost(), is(equalTo(getHostAddress())));
        // And port is in configured range
        int port = restUri.getPort();
        assertThat(port >= 10333 && port < 10333 + 10, is(true));

        // When stop node
        IgnitionManager.stop(NODE_NAME);

        // Then the file is removed
        assertThat(reportFile.exists(), is(false));
    }

    private static String getHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }
}
