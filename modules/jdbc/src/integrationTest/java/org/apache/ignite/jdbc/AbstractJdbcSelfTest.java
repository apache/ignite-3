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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.jdbc.util.JdbcTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

/**
 * Abstract jdbc self test.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class AbstractJdbcSelfTest extends BaseIgniteAbstractTest {
    private static final int TEST_PORT = 47500;

    /** URL. */
    protected static final String URL = "jdbc:ignite:thin://127.0.0.1:10800";

    /** Work directory. */
    @WorkDirectory
    private static Path WORK_DIR;

    /** Cluster nodes. */
    protected static final List<Ignite> clusterNodes = new ArrayList<>();

    /** Connection. */
    protected static Connection conn;

    /** Statement. */
    protected Statement stmt;

    /**
     * Creates a cluster of three nodes.

     * @param testInfo Test info.
     */
    @BeforeAll
    public static void beforeAllBase(TestInfo testInfo) throws Exception {
        String nodeName = testNodeName(testInfo, TEST_PORT);

        CompletableFuture<Ignite> future = TestIgnitionManager.start(nodeName, null, WORK_DIR.resolve(nodeName));

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(nodeName)
                .metaStorageNodeNames(List.of(nodeName))
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(initParameters);

        assertThat(future, willCompleteSuccessfully());

        clusterNodes.add(future.join());

        conn = DriverManager.getConnection(URL);

        conn.setSchema("PUBLIC");
    }

    /**
     * Close all cluster nodes.
     *
     * @throws Exception if failed.
     */
    @AfterAll
    public static void afterAllBase(TestInfo testInfo) throws Exception {
        IgniteUtils.closeAll(
                conn != null && !conn.isClosed() ? conn : null,
                () -> IgnitionManager.stop(testNodeName(testInfo, TEST_PORT))
        );

        conn = null;
        clusterNodes.clear();
    }

    @BeforeEach
    protected void setUpBase() throws Exception {
        conn.setAutoCommit(true);

        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    @AfterEach
    protected void tearDownBase() throws Exception {
        if (stmt != null) {
            stmt.close();

            assert stmt.isClosed();
        }
    }

    /**
     * Checks that the function throws SQLException about a closed result set.
     *
     * @param ex Executable function that throws an error.
     */
    protected void checkResultSetClosed(Executable ex) {
        JdbcTestUtils.assertThrowsSqlException("Result set is closed", ex);
    }

    /**
     * Checks that the function throws SQLException about a closed statement.
     *
     * @param ex Executable function that throws an error.
     */
    protected void checkStatementClosed(Executable ex) {
        JdbcTestUtils.assertThrowsSqlException("Statement is closed", ex);
    }

    /**
     * Checks that the function throws SQLException about a closed connection.
     *
     * @param ex Executable function that throws an error.
     */
    protected void checkConnectionClosed(Executable ex) {
        JdbcTestUtils.assertThrowsSqlException("Connection is closed", ex);
    }

    /**
     * Checks that the function throws SQLFeatureNotSupportedException.
     *
     * @param ex Executable function that throws an error.
     */
    protected void checkNotSupported(Executable ex) {
        assertThrows(SQLFeatureNotSupportedException.class, ex);
    }

    /** Return a size of stored resources. Reflection based implementation, need to be refactored. */
    int openResources() {
        IgniteImpl ignite = (IgniteImpl) clusterNodes.get(0);
        IgniteComponent cliHnd = IgniteTestUtils.getFieldValue(ignite, "clientHandlerModule");
        Object clientInboundHandler = IgniteTestUtils.getFieldValue(cliHnd, "handler");
        Object rsrc = IgniteTestUtils.getFieldValue(clientInboundHandler, "resources");
        Map resources = IgniteTestUtils.getFieldValue(rsrc, "res");
        return resources.size();
    }

    /** Returns a size of opened cursors. */
    int openCursors() {
        IgniteImpl ignite = (IgniteImpl) clusterNodes.get(0);
        SqlQueryProcessor queryProcessor = (SqlQueryProcessor) ignite.queryEngine();
        return queryProcessor.openedCursors();
    }
}
