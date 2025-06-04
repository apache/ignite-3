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

package org.apache.ignite.internal;

import java.nio.file.Path;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
public class ItCompatibilityTest extends BaseIgniteAbstractTest {

    @WorkDirectory
    private static Path WORK_DIR;

    private MixedCluster cluster;

    @AfterEach
    void cleanup() {
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    void upgrade(TestInfo testInfo) throws Exception {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, WORK_DIR).build();

        int nodesCount = 3;
        cluster = new MixedCluster(clusterConfiguration);
        cluster.start( "3.0.0", nodesCount);

        cluster.init();

        try (IgniteClient client = cluster.createClient()) {
            client.sql().execute(null, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL VARCHAR)");
        }

        cluster.stop();

        cluster.startEmbedded(nodesCount);

        cluster.stop();
    }
}
