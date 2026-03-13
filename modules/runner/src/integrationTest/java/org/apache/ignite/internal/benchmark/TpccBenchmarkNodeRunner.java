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

package org.apache.ignite.internal.benchmark;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeOrHaltFailureHandlerConfigurationSchema;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Base benchmark class for {@link SelectBenchmark} and {@link InsertBenchmark}.
 *
 * <p>Starts an Ignite cluster with a single table {@link #TABLE_NAME}, that has
 * single PK column and 10 value columns.
 */
@State(Scope.Benchmark)
public class TpccBenchmarkNodeRunner {
    private static final IgniteLogger LOG = Loggers.forClass(TpccBenchmarkNodeRunner.class);

    private static final int BASE_PORT = 3344;
    protected static final int BASE_CLIENT_PORT = 10800;
    private static final int BASE_REST_PORT = 10300;

    private static final List<IgniteServer> igniteServers = new ArrayList<>();

    protected static Ignite publicIgnite;
    protected static IgniteImpl igniteImpl;

    @Nullable
    protected String clusterConfiguration() {
        return "ignite {}";
    }

    public static void main(String[] args) throws Exception {
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            LOG.info("Shut down initiated");
//
//            try {
//                IgniteUtils.closeAll(igniteServers.stream().map(node -> node::shutdown));
//            } catch (Exception e) {
//                LOG.error("Shut down failed", e);
//            }
//        }));
        TpccBenchmarkNodeRunner runner = new TpccBenchmarkNodeRunner();
        runner.startCluster();
    }

    public IgniteImpl node(int idx) {
        return unwrapIgniteImpl(igniteServers.get(idx).api());
    }

    private void startCluster() throws Exception {
        Path workDir = workDir();

        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        @Language("HOCON")
        String configTemplate = "ignite {\n"
                + "  \"network\": {\n"
                + "    \"port\":{},\n"
                + "    \"nodeFinder\":{\n"
                + "      \"netClusterNodes\": [ {} ]\n"
                + "    }\n"
                + "  },\n"
                + "  storage.profiles: {"
                + "        " + DEFAULT_STORAGE_PROFILE + ".engine: aipersist, "
                + "        " + DEFAULT_STORAGE_PROFILE + ".sizeBytes: 2073741824 " // Avoid page replacement.
                + "  },\n"
                + "  clientConnector: { port:{} },\n"
                + "  clientConnector.sendServerExceptionStackTraceToClient: true\n"
                + "  rest.port: {},\n"
                + "  raft.fsync = " + fsync() + ",\n"
                + "  system.partitionsLogPath = \"" + logPath() + "\",\n"
                + "  failureHandler.handler: {\n"
                + "      type: \"" + StopNodeOrHaltFailureHandlerConfigurationSchema.TYPE + "\",\n"
                + "      tryStop: true,\n"
                + "      timeoutMillis: 60000,\n" // 1 minute for graceful shutdown
                + "  },\n"
                + "}";

        for (int i = 0; i < nodes(); i++) {
            int port = BASE_PORT + i;
            String nodeName = nodeName(port);

            String config = IgniteStringFormatter.format(configTemplate, port, connectNodeAddr,
                    BASE_CLIENT_PORT + i, BASE_REST_PORT + i);

            igniteServers.add(TestIgnitionManager.startWithProductionDefaults(nodeName, config, workDir.resolve(nodeName)));
        }

        String metaStorageNodeName = nodeName(BASE_PORT);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodeNames(metaStorageNodeName)
                .clusterName("cluster")
                .clusterConfiguration(clusterConfiguration())
                .build();

        TestIgnitionManager.init(igniteServers.get(0), initParameters);

        for (IgniteServer node : igniteServers) {
            assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

            if (publicIgnite == null) {
                publicIgnite = node.api();
                igniteImpl = unwrapIgniteImpl(publicIgnite);
            }
        }
    }

    private static String nodeName(int port) {
        return "node_" + port;
    }

    protected Path workDir() throws Exception {
        return new File("c:/work/tpcc").toPath();
    }

    protected String logPath() {
        return "";
    }

    protected boolean fsync() {
        return false;
    }

    protected int nodes() {
        return 1;
    }
}
