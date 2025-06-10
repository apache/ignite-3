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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Base class for testing cluster upgrades. Starts a cluster on an old version, initializes it, stops it, then starts it in the
 * embedded mode using current version.
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
@ParameterizedClass
@MethodSource("baseVersions")
public abstract class PersistenceTestBase extends BaseIgniteAbstractTest {

    // If there are no fields annotated with @Parameter, constructor injection will be used, which is incompatible with the
    // Lifecycle.PER_CLASS.
    @SuppressWarnings("unused")
    @Parameter
    String baseVersion;

    @WorkDirectory
    private static Path WORK_DIR;

    protected IgniteCluster cluster;

    @SuppressWarnings("unused")
    @BeforeParameterizedClassInvocation
    void startCluster(String baseVersion, TestInfo testInfo) {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, WORK_DIR).build();

        int nodesCount = 3;
        cluster = new IgniteCluster(clusterConfiguration);
        cluster.start(baseVersion, nodesCount);

        cluster.init();

        try (IgniteClient client = cluster.createClient()) {
            setupBaseVersion(client);
        }

        cluster.stop();

        cluster.startEmbedded(nodesCount);
    }

    @SuppressWarnings("unused")
    @AfterParameterizedClassInvocation
    void stopCluster() {
        if (cluster != null) {
            cluster.stop();
        }
    }

    protected abstract void setupBaseVersion(Ignite baseIgnite);

    protected List<List<Object>> sql(String query) {
        return ClusterPerClassIntegrationTest.sql(cluster.node(0), null, null, null, query);
    }

    private static List<String> baseVersions() {
        List<String> versions = readVersions();
        if (System.getProperty("testAllVersions") != null) {
            return versions;
        } else {
            // Take at most two latest versions by default.
            int fromIndex = Math.max(versions.size() - 2, 0);
            return versions.subList(fromIndex, versions.size());
        }
    }

    private static List<String> readVersions() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(RunnerNode.class.getResource("/versions.json"));

            List<String> result = new ArrayList<>();

            node.forEach(entry ->
                    entry.get("versions").forEach(version ->
                            result.add(version.get("version").textValue())
                    )
            );

            return result;
        } catch (IOException e) {
            Loggers.forClass(RunnerNode.class).error("Failed to read versions.json", e);
            return List.of();
        }
    }
}
