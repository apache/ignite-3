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

package org.apache.ignite.jdbc;

import java.nio.file.Path;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ConfigTemplates;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
public class ItJdbcPooledClusterRestartTest {
    /** Work directory. */
    @WorkDirectory
    private static Path WORK_DIR;

    @Test
    public void test(TestInfo testInfo) throws Exception {
        var cluster = startCluster(testInfo);

        cluster.shutdown();
    }

    private Cluster startCluster(TestInfo testInfo) {
        ClusterConfiguration.Builder clusterConfiguration = ClusterConfiguration.builder(testInfo, WORK_DIR)
                .defaultNodeBootstrapConfigTemplate(ConfigTemplates.NODE_BOOTSTRAP_CFG_TEMPLATE);

        var cluster = new Cluster(clusterConfiguration.build());

        cluster.startAndInit(testInfo, 1, new int[]{0}, initParametersBuilder -> {});

        return cluster;
    }
}
