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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for creating tests for examples.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class AbstractExamplesTest extends IgniteAbstractTest {
    private static final String TEST_NODE_NAME = "ignite-node";

    /** Empty argument to invoke an example. */
    protected static final String[] EMPTY_ARGS = new String[0];

    /** Started embedded node. */
    private IgniteServer node;

    /**
     * Starts a node.
     */
    @BeforeEach
    public void startNode() throws Exception {
        node = IgniteServer.start(
                TEST_NODE_NAME,
                configFile(),
                workDir
        );

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodeNames(TEST_NODE_NAME)
                .clusterName("cluster")
                .build();

        CompletableFuture<Void> initFuture = node.initClusterAsync(initParameters);
        assertThat(initFuture, willCompleteSuccessfully());
    }

    /**
     * Stops the node.
     */
    @AfterEach
    public void stopNode() {
        node.shutdown();
    }

    /**
     * Copy the original node configuration file to the temporary directory.
     * It needs for the safety reasons: some tests can mutate local configurations (for example storage tests)
     * and mutate this file as a result. So, further tests will run with inappropriate configuration.
     *
     * @return The path of the copied configuration file.
     * @throws IOException If an I/O error occurs during the file copying process.
     */
    private Path configFile() throws IOException {
        var configFileName = "ignite-config.conf";

        return Files.copy(
                Path.of("config", configFileName),
                workDir.resolve(configFileName));
    }
}
