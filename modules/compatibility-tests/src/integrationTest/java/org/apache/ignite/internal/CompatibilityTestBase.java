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

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.IgniteVersions.Version;
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
public abstract class CompatibilityTestBase extends BaseIgniteAbstractTest {
    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  storage.profiles: {"
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  },\n"
            + "  clientConnector.port: {},\n"
            + "  clientConnector.sendServerExceptionStackTraceToClient: true,\n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    // If there are no fields annotated with @Parameter, constructor injection will be used, which is incompatible with the
    // Lifecycle.PER_CLASS.
    @SuppressWarnings("unused")
    @Parameter
    private String baseVersion;

    // Force per class template work directory so that non-static field doesn't get overwritten by the BeforeEach callback.
    @WorkDirectory(forcePerClassTemplate = true)
    private Path workDir;

    protected IgniteCluster cluster;

    @SuppressWarnings("unused")
    @BeforeParameterizedClassInvocation
    void startCluster(String baseVersion, TestInfo testInfo) {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, workDir)
                .defaultNodeBootstrapConfigTemplate(NODE_BOOTSTRAP_CFG_TEMPLATE)
                .build();

        int nodesCount = nodesCount();

        cluster = new IgniteCluster(clusterConfiguration);
        cluster.start(baseVersion, nodesCount);

        cluster.init(this::configureInitParameters);

        try (IgniteClient client = cluster.createClient()) {
            setupBaseVersion(client);
        }

        if (restartWithCurrentEmbeddedVersion()) {
            cluster.stop();

            cluster.startEmbedded(nodesCount);
        }
    }

    @SuppressWarnings("unused")
    @AfterParameterizedClassInvocation
    void stopCluster() {
        if (cluster != null) {
            cluster.stop();
        }
    }

    protected String getNodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    protected int nodesCount() {
        return 3;
    }

    protected boolean restartWithCurrentEmbeddedVersion() {
        return true;
    }

    /**
     * This method can be overridden to add custom init parameters during cluster initialization.
     */
    protected void configureInitParameters(InitParametersBuilder builder) {
    }

    protected abstract void setupBaseVersion(Ignite baseIgnite);

    protected List<List<Object>> sql(String query) {
        return sql(node(0), query);
    }

    protected List<List<Object>> sql(Ignite ignite, String query) {
        return ClusterPerClassIntegrationTest.sql(ignite, null, null, null, query);
    }

    protected Ignite node(int index) {
        return cluster.node(index);
    }

    private static List<String> baseVersions() {
        return baseVersions(2);
    }

    /**
     * Returns a list of base versions. If {@code testAllVersions} system property is set, then all versions are returned, otherwise, at
     * most {@code numLatest} are taken.
     *
     * @param numLatest Number of latest versions to take by default.
     * @return A list of base versions for a test.
     */
    protected static List<String> baseVersions(int numLatest) {
        List<String> versions = IgniteVersions.INSTANCE.versions().stream().map(Version::version).collect(Collectors.toList());
        if (System.getProperty("testAllVersions") != null) {
            return versions;
        } else {
            // Take at most two latest versions by default.
            int fromIndex = Math.max(versions.size() - numLatest, 0);
            return versions.subList(fromIndex, versions.size());
        }
    }
}
