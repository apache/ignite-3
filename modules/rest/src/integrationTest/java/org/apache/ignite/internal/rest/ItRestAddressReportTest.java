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

package org.apache.ignite.internal.rest;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.net.InetAddress;
import java.net.URL;
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

    @Test
    @DisplayName("Should report rest port to the file after RestComponent started")
    void restPortReportedToFile() throws Exception {
        // Given configuration with rest port configured rest.port=10333
        Path configPath = Path.of(ItRestAddressReportTest.class.getResource("/ignite-config-rest-port-not-default.json").toURI());

        // When start node
        CompletableFuture<Ignite> ign = IgniteRunner.start(
                "--config-path", configPath.toAbsolutePath().toString(),
                "--work-dir", workDir.resolve(NODE_NAME).toAbsolutePath().toString(),
                "--name", NODE_NAME
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
        Path reportFile = workDir.resolve(NODE_NAME).resolve("rest-address");
        assertThat(Files.exists(reportFile), is(true));

        // And the file contains valid rest server network address
        URL restUri = new URL(Files.readString(reportFile));
        assertThat(restUri.getHost(), is(equalTo(getHostName())));
        assertThat(restUri.getPort(), is(equalTo(10333)));

        // When stop node
        IgnitionManager.stop(NODE_NAME);

        // Then the file is removed
        assertThat(Files.exists(reportFile), is(false));
    }

    private static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }
}
