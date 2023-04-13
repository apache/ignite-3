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

package org.apache.ignite.internal.runner.app.client;

import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Thin client authorization integration test.
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ItThinClientAuthorizationTest {
    private Path workDir;

    /**
     * Before all.
     */
    @BeforeAll
    void beforeAll(TestInfo testInfo, @WorkDirectory Path workDir) {
        this.workDir = workDir;
    }

    @Test
    public void test() {
        var cfg = "{\n"
                        + "  network.port: 3344,\n"
                        + "  network.nodeFinder.netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "}";

        var nodeName = "node1";

        Ignite server = TestIgnitionManager.start(nodeName, cfg, workDir.resolve(nodeName)).join();

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(nodeName)
                .metaStorageNodeNames(List.of(nodeName))
                .clusterName("cluster")
                .build();

        IgnitionManager.init(initParameters);
    }
}
