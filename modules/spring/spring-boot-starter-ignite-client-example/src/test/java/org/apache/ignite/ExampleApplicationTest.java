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

import java.io.IOException;
import java.nio.file.Path;
import org.apache.ignite.customizer.example.ExampleApplicationWithCustomAuthenticator;
import org.apache.ignite.example.ExampleApplication;
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
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.support.ResourcePropertySource;

/**
 * Tests for Example application.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ExampleApplicationTest {

    private static String CONFIG_AUTH_ENABLED = "ignite {"
            + " security {"
            + "     enabled:true,"
            + "     authentication.providers:[{"
            + "             name:default,"
            + "             type:basic,"
            + "             users:[{"
            + "                     username:ignite,"
            + "                     password:ignite"
            + "                 }]"
            + "         }]"
            + "}}";

    @WorkDirectory
    private Path workDir;

    private Cluster cluster;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, workDir)
                .build();

        this.cluster = new Cluster(clusterConfiguration);
    }

    @Test
    void testBasic() {
        this.cluster.startAndInit(1);

        runApplication(ExampleApplication.class, "application.properties");

        verifyApplicationRun();
    }

    @Test
    void testAuthEnabled() {
        this.cluster.startAndInit(1, params -> params.clusterConfiguration(CONFIG_AUTH_ENABLED));

        runApplication(ExampleApplication.class, "application-auth.properties");

        verifyApplicationRun();
    }

    @Test
    void testAuthSetWithCustomizer() {
        this.cluster.startAndInit(1, params -> params.clusterConfiguration(CONFIG_AUTH_ENABLED));

        runApplication(ExampleApplicationWithCustomAuthenticator.class, "application.properties");

        verifyApplicationRun();
    }

    private void verifyApplicationRun() {
        boolean tableExists = cluster.aliveNode().tables().tables().stream().anyMatch(t -> t.qualifiedName().objectName().equals("PERSON"));
        ResultSet<SqlRow> resultSet = cluster.aliveNode().sql().execute(null, "SELECT COUNT(*) FROM PERSON");
        long rowsCount = resultSet.next().longValue(0);

        assertTrue(tableExists);
        assertEquals(1, rowsCount);
    }

    private void runApplication(Class<?> applicationClass, String propertiesFile) {
        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withInitializer(context -> {
                    try {
                        ResourcePropertySource propertySource = new ResourcePropertySource("classpath:" + propertiesFile);
                        context.getEnvironment().getPropertySources().addLast(propertySource);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .withUserConfiguration(applicationClass);

        contextRunner.run(ctx -> {
            ApplicationRunner runner = ctx.getBean(ApplicationRunner.class);
            runner.run(null);
        });
    }

    @AfterEach
    void cleanup() {
        cluster.shutdown();
    }
}
