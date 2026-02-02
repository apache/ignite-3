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
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.properties.IgniteProductVersion.fromString;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.awaitility.Awaitility.await;

import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;

/**
 * Base class for testing cluster upgrades. Starts a cluster on an old version, initializes it, stops it, then starts it in the embedded
 * mode using current version.
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
public abstract class CompatibilityTestBase extends BaseIgniteAbstractTest {
    /** Nodes bootstrap configuration pattern. */
    public static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
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
            + "  rest.ssl.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false,\n"
            + "  nodeAttributes: {\n"
            + "    nodeAttributes: {nodeName: \"{}\", nodeIndex: \"{}\"}\n"
            + "  }\n"
            + "}";

    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    // If there are no fields annotated with @Parameter, constructor injection will be used, which is incompatible with the
    // Lifecycle.PER_CLASS.
    @SuppressWarnings("unused")
    @Parameter
    private String baseVersion;

    // Force per class template work directory so that non-static field doesn't get overwritten by the BeforeEach callback.
    @WorkDirectory(forcePerClassTemplate = true)
    protected Path workDir;

    protected IgniteCluster cluster;

    protected List<String> extraIgniteModuleIds() {
        return Collections.emptyList();
    }

    @Inject
    @Client(NODE_URL + "/management/v1/deployment")
    protected HttpClient deploymentClient;

    @SuppressWarnings("unused")
    @BeforeParameterizedClassInvocation
    void startCluster(String baseVersion, TestInfo testInfo) throws Exception {
        log.info("Starting nodes for base version: {}", baseVersion);

        cluster = createCluster(testInfo, workDir);

        int nodesCount = nodesCount();
        cluster.start(baseVersion, nodesCount, extraIgniteModuleIds());

        cluster.init(this::configureInitParameters);

        try (IgniteClient client = cluster.createClient()) {
            setupBaseVersion(client);
        }

        boolean shouldEnableColocation = fromString(baseVersion).compareTo(fromString("3.1.0")) >= 0;

        System.setProperty(COLOCATION_FEATURE_FLAG, String.valueOf(shouldEnableColocation));

        if (restartWithCurrentEmbeddedVersion()) {
            cluster.stop();

            cluster.startEmbedded(nodesCount);
            await().until(this::noActiveRebalance, willBe(true));
        }
    }

    /**
     * Creates a cluster with the given test info and work directory.
     *
     * @param testInfo Test information.
     * @param workDir Work directory.
     * @return A new instance of {@link IgniteCluster}.
     */
    public IgniteCluster createCluster(TestInfo testInfo, Path workDir) {
        return createCluster(testInfo, workDir, getNodeBootstrapConfigTemplate());
    }

    /**
     * Creates a cluster with the given test info and work directory.
     *
     * @param testInfo Test information.
     * @param workDir Work directory.
     * @return A new instance of {@link IgniteCluster}.
     */
    public static IgniteCluster createCluster(TestInfo testInfo, Path workDir, String nodeBootstrapConfigTemplate) {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, workDir)
                .defaultNodeBootstrapConfigTemplate(nodeBootstrapConfigTemplate)
                .build();

        return new IgniteCluster(clusterConfiguration);
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

    protected abstract void setupBaseVersion(Ignite baseIgnite) throws Exception;

    protected List<List<Object>> sql(String query, Object... args) {
        return sql(node(0), query, args);
    }

    protected static List<List<Object>> sql(Ignite ignite, String query, Object... args) {
        return sql(ignite, null, query, args);
    }

    protected static List<List<Object>> sql(Ignite ignite, @Nullable Transaction tx, String query, Object... args) {
        return ClusterPerClassIntegrationTest.sql(ignite, tx, null, null, query, args);
    }

    protected Ignite node(int index) {
        return cluster.node(index);
    }

    /**
     * Returns a list of base versions. If {@code testAllVersions} system property is set, then all versions are returned, otherwise, last 2
     * are taken.
     *
     * @return A list of base versions for a test.
     */
    public static List<String> baseVersions() {
        return baseVersions(2);
    }

    /**
     * Returns a list of base versions. If {@code testAllVersions} system property is empty or set to {@code true}, then all versions are
     * returned, otherwise, at most {@code numLatest} latest versions are taken.
     *
     * @param numLatest Number of latest versions to take by default.
     * @param skipVersions Array of strings to skip.
     * @return A list of base versions for a test.
     */
    public static List<String> baseVersions(int numLatest, String... skipVersions) {
        List<String> versions = baseVersions(skipVersions);
        if (shouldTestAllVersions()) {
            return versions;
        } else {
            // Take at most numLatest latest versions.
            int fromIndex = Math.max(versions.size() - numLatest, 0);
            return versions.subList(fromIndex, versions.size());
        }
    }

    /**
     * Returns a list of base versions.
     *
     * @param skipVersions Array of strings to skip.
     * @return A list of base versions for a test.
     */
    public static List<String> baseVersions(String... skipVersions) {
        Set<String> skipSet = Arrays.stream(skipVersions).collect(Collectors.toSet());
        return IgniteVersions.INSTANCE.versions().keySet().stream()
                .filter(Predicate.not(skipSet::contains))
                .collect(Collectors.toList());
    }

    private static boolean shouldTestAllVersions() {
        String value = System.getProperty("testAllVersions");
        if (value != null) {
            value = value.trim().toLowerCase();
            return value.isEmpty() || "true".equals(value);
        }
        return false;
    }

    /**
     * Checks if there is an active rebalance happening. Does this by checking for pending assignments.
     *
     * @return {@code true} if there are no pending assignments in the metastorage.
     */
    protected CompletableFuture<Boolean> noActiveRebalance() {
        IgniteImpl node = unwrapIgniteImpl(node(0));

        ByteArray prefix = pendingAssignmentsQueuePrefix();

        return subscribeToList(node.metaStorageManager().prefix(prefix))
                .thenApply(List::isEmpty);
    }

    private static ByteArray pendingAssignmentsQueuePrefix() {
        return new ByteArray(PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES);
    }
}
