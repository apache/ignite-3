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
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for testing cluster upgrades. Starts a cluster on an old version, initializes it, stops it, then starts it in the
 * embedded mode using current version.
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
public abstract class PersistenceTestBase extends BaseIgniteAbstractTest {

    @WorkDirectory
    private static Path WORK_DIR;

    protected IgniteCluster cluster;

    @BeforeAll
    void startCluster(TestInfo testInfo) {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, WORK_DIR).build();

        int nodesCount = 3;
        cluster = new IgniteCluster(clusterConfiguration);
        cluster.start(baseVersion(), nodesCount);

        cluster.init();

        try (IgniteClient client = cluster.createClient()) {
            setupBaseVersion(client);
        }

        cluster.stop();

        cluster.startEmbedded(nodesCount);
    }

    @AfterAll
    void stopCluster() {
        if (cluster != null) {
            cluster.stop();
        }
    }

    protected abstract String baseVersion();

    protected abstract void setupBaseVersion(Ignite baseIgnite);

    protected List<List<Object>> sql(String query) {
        return ClusterPerClassIntegrationTest.sql(cluster.node(0), null, null, null, query);
    }
}
