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

package org.apache.ignite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for Example application.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ExampleApplicationTest {

    @WorkDirectory
    private Path workDir;

    private Cluster cluster;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, workDir).build();

        this.cluster = new Cluster(clusterConfiguration);
        this.cluster.startAndInit(1);
    }

    @Test
    void test() {
        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withPropertyValues("ignite.client.addresses=127.0.0.1:10800")
                .withConfiguration(AutoConfigurations.of(ExampleApplication.class));

        contextRunner.run(ctx -> {
            ApplicationRunner runner = ctx.getBean(ApplicationRunner.class);
            runner.run(null);
        });

        boolean tableExists = cluster.aliveNode().tables().tables().stream().anyMatch(t -> t.qualifiedName().objectName().equals("PERSON"));
        ResultSet<SqlRow> resultSet = cluster.aliveNode().sql().execute(null, "SELECT COUNT(*) FROM PERSON");
        long rowsCount = resultSet.next().longValue(0);

        assertTrue(tableExists);
        assertEquals(1, rowsCount);
    }

    @AfterEach
    void cleanup() {
        cluster.shutdown();
    }
}
