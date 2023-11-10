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

import static org.apache.ignite.internal.sql.engine.property.PropertiesHelper.newBuilder;
import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.network.configuration.NetworkConfigurationSchema;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.intellij.lang.annotations.Language;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Base benchmark class for {@link SelectBenchmark} and {@link InsertBenchmark}. Starts an Ignite node with a single table
 * {@link #TABLE_NAME}, that has single PK column and 10 value columns.
 */
@State(Scope.Benchmark)
public class AbstractOneNodeBenchmark {
    private static final int PORT = NetworkConfigurationSchema.DEFAULT_PORT;

    private static final String NODE_NAME = "node" + PORT;

    protected static final String FIELD_VAL = "a".repeat(100);

    protected static final String TABLE_NAME = "usertable";

    protected static IgniteImpl clusterNode;

    @Param({"true", "false"})
    private boolean fsync;

    /**
     * Starts ignite node and creates table {@link #TABLE_NAME}.
     */
    @Setup
    public final void nodeSetUp() throws IOException {
        Path workDir = Files.createTempDirectory("tmpDirPrefix").toFile().toPath();

        @Language("HOCON")
        String config = "network: {\n"
                        + "  nodeFinder:{\n"
                        + "    netClusterNodes: [ \"localhost:" + PORT + "\"] \n"
                        + "  }\n"
                        + "},"
                        + "raft.fsync = " + fsync;

        var fut =  TestIgnitionManager.start(NODE_NAME, config, workDir.resolve(NODE_NAME));

        TestIgnitionManager.init(new InitParametersBuilder()
                .clusterName("cluster")
                .destinationNodeName(NODE_NAME)
                .cmgNodeNames(List.of(NODE_NAME))
                .metaStorageNodeNames(List.of(NODE_NAME))
                .build()
        );

        clusterNode = (IgniteImpl) fut.join();

        var queryEngine = clusterNode.queryEngine();

        SessionId sessionId = queryEngine.createSession(newBuilder()
                .set(QueryProperty.DEFAULT_SCHEMA, "PUBLIC")
                .set(QueryProperty.QUERY_TIMEOUT, TimeUnit.SECONDS.toMillis(60))
                .build()
        );

        var sql = "CREATE TABLE " + TABLE_NAME + "(\n"
                + "    ycsb_key int PRIMARY KEY,\n"
                + "    field1   varchar(100),\n"
                + "    field2   varchar(100),\n"
                + "    field3   varchar(100),\n"
                + "    field4   varchar(100),\n"
                + "    field5   varchar(100),\n"
                + "    field6   varchar(100),\n"
                + "    field7   varchar(100),\n"
                + "    field8   varchar(100),\n"
                + "    field9   varchar(100),\n"
                + "    field10  varchar(100)\n"
                + ");";

        try {
            var context = QueryContext.create(SqlQueryType.SINGLE_STMT_TYPES);

            getAllFromCursor(
                    await(queryEngine.querySingleAsync(sessionId, context, clusterNode.transactions(), sql))
            );
        } finally {
            queryEngine.closeSession(sessionId);
        }
    }

    @TearDown
    public final void nodeTearDown() {
        IgnitionManager.stop(NODE_NAME);
    }
}
