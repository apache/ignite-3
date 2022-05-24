/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for creating tests for examples.
 */
public abstract class AbstractExamplesTest extends IgniteAbstractTest {
    private static final String TEST_NODE_NAME = "ignite-node";

    /** Empty argument to invoke an example. */
    protected static final String[] EMPTY_ARGS = new String[0];

    /**
     * Starts a node.
     */
    @BeforeEach
    public void startNode() {
        CompletableFuture<Ignite> ignite = IgnitionManager.start(
                TEST_NODE_NAME,
                Path.of("config", "ignite-config.json"),
                workDir,
                null
        );

        IgnitionManager.init(TEST_NODE_NAME, List.of(TEST_NODE_NAME), "cluster");

        assertThat(ignite, willCompleteSuccessfully());
    }

    /**
     * Stops the node.
     */
    @AfterEach
    public void stopNode() {
        IgnitionManager.stop(TEST_NODE_NAME);
    }
}
