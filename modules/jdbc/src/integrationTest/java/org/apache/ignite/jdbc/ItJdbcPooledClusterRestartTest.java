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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
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
        var cluster = getCluster(testInfo);
        startCluster(testInfo, cluster);

        var url = "jdbc:ignite:thin://127.0.0.1:10800";

        try (ComboPooledDataSource c3p0Pool = new ComboPooledDataSource()) {
            c3p0Pool.setJdbcUrl(url);

            for (int i = 0; i < 5; i++) {
                try (Connection conn = c3p0Pool.getConnection()) {
                    try (var stmt = conn.createStatement()) {
                        stmt.setFetchSize(100);
                        try (ResultSet rs = stmt.executeQuery("SELECT * FROM SYSTEM_RANGE(0, 2000)")) {
                            int cnt = 0;

                            while (rs.next()) {
                                assertEquals(cnt, rs.getInt(1));
                                cnt++;
                            }

                            assertEquals(2001, cnt);
                        }
                    }
                }

                cluster.shutdown();
                cluster = getCluster(testInfo);
                startCluster(testInfo, cluster);
            }
        }
    }

    private static void startCluster(TestInfo testInfo, Cluster cluster) {
        cluster.startAndInit(testInfo, 1, new int[]{0}, initParametersBuilder -> {});
    }

    private static Cluster getCluster(TestInfo testInfo) {
        ClusterConfiguration.Builder clusterConfiguration = ClusterConfiguration.builder(testInfo, WORK_DIR)
                .defaultNodeBootstrapConfigTemplate(ConfigTemplates.NODE_BOOTSTRAP_CFG_TEMPLATE);

        return new Cluster(clusterConfiguration.build());
    }
}
