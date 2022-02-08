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

import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for creating tests for examples.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class AbstractExamplesTest {
    private static final String TEST_NODE_NAME = "ignite-node";

    /**
     * Starts a node.
     *
     * @param workDir Work directory for the started node. Must not be {@code null}.
     */
    @BeforeEach
    public void startNode(@WorkDirectory Path workDir) throws Exception {
        IgniteImpl ignite = (IgniteImpl) IgnitionManager.start(
                TEST_NODE_NAME,
                Path.of("config", "ignite-config.json"),
                workDir,
                null
        );

        ignite.init(List.of(ignite.name()));
    }

    /**
     * Stop node.
     */
    @AfterEach
    public void stopNode() {
        IgnitionManager.stop(TEST_NODE_NAME);
    }
}
