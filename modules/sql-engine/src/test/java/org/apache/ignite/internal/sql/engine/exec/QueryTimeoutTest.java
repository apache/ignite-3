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

package org.apache.ignite.internal.sql.engine.exec;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.hamcrest.MatcherAssert.assertThat;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests cases for cancellation due to timeout. */
@SuppressWarnings("ThrowableNotThrown")
public class QueryTimeoutTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "gateway";

    private final AtomicBoolean ignoreCatalogUpdates = new AtomicBoolean(false);

    // Relatively high timeout is set to make tests stable on TC. The reason is that
    // query have to make it to the final point which is varied from test to test in
    // order to 1) make sure timeout is handled properly at particular stages of query
    // execution, and 2) to fail with proper exception class.
    private static final SqlProperties PROPS_WITH_TIMEOUT = new SqlProperties().queryTimeout(1_000L);

    private TestCluster cluster;
    private TestNode gatewayNode;

    @BeforeAll
    static void warmUpCluster() throws Exception {
        TestBuilders.warmupTestCluster();
    }

    @BeforeEach
    void startCluster() {
        ignoreCatalogUpdates.set(false);

        cluster = TestBuilders.cluster()
                .nodes(NODE_NAME)
                .catalogManagerDecorator(this::catalogManagerDecorator)
                .operationKillHandlers(
                        new OperationKillHandler() {
                            @Override
                            public CompletableFuture<Boolean> cancelAsync(String operationId) {
                                return new CompletableFuture<>();
                            }

                            @Override
                            public boolean local() {
                                return true;
                            }

                            @Override
                            public CancellableOperationType type() {
                                return CancellableOperationType.QUERY;
                            }
                        }
                )
                .build();

        cluster.start();

        gatewayNode = cluster.node(NODE_NAME);

        gatewayNode.initSchema("CREATE TABLE my_table (id INT PRIMARY KEY, val VARCHAR(128))");
        cluster.setDataProvider("MY_TABLE", neverReplyingScannableTable());
        cluster.setUpdatableTable("MY_TABLE", neverReplyingUpdatableTable());
        cluster.setAssignmentsProvider(
                "MY_TABLE",
                (partitionsCount, includeBackups) -> IntStream.range(0, partitionsCount)
                        .mapToObj(p -> List.of(NODE_NAME))
                        .collect(Collectors.toList())
        );
    }

    @AfterEach
    void stopCluster() throws Exception {
        cluster.stop();
    }

    @Test
    void testTimeoutDdl() {
        ignoreCatalogUpdates.set(true);

        assertThrows(
                SqlException.class,
                () -> gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "CREATE TABLE x (id INTEGER PRIMARY KEY, val INTEGER)"),
                QueryCancelledException.TIMEOUT_MSG
        );
    }

    @Test
    void testTimeoutKill() {
        assertThrows(
                SqlException.class,
                () -> gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "KILL QUERY '" + randomUUID() + '\''),
                QueryCancelledException.TIMEOUT_MSG
        );
    }

    @Test
    void testTimeoutSelectCount() {
        assertThrows(
                SqlException.class,
                () -> gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "SELECT COUNT(*) FROM my_table"),
                QueryCancelledException.TIMEOUT_MSG
        );
    }

    @Test
    void testTimeoutKvGet() {
        assertThrows(
                SqlException.class,
                () -> gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "SELECT * FROM my_table WHERE id = ?", 2),
                QueryCancelledException.TIMEOUT_MSG
        );
    }

    @Test
    void testTimeoutKvPut() {
        assertThrows(
                SqlException.class,
                () -> gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "INSERT INTO my_table VALUES (?, ?)", 1, "1"),
                QueryCancelledException.TIMEOUT_MSG
        );
    }

    @Test
    void testTimeoutDistributedRead() {
        AsyncSqlCursor<?> cursor = gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "SELECT * FROM my_table");

        assertThat(
                cursor.requestNextAsync(1),
                willThrowWithCauseOrSuppressed(
                        SqlException.class,
                        QueryCancelledException.TIMEOUT_MSG
                )
        );
    }

    @Test
    void testTimeoutDistributedModify() {
        assertThrows(
                SqlException.class,
                () -> gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "UPDATE my_table SET val = val || val"),
                QueryCancelledException.TIMEOUT_MSG
        );
    }

    private CatalogManager catalogManagerDecorator(CatalogManager original) {
        return (CatalogManager) Proxy.newProxyInstance(
                QueryTimeoutTest.class.getClassLoader(),
                new Class<?>[] {CatalogManager.class},
                (proxy, method, args) -> {
                    if (ignoreCatalogUpdates.get() && "execute".equals(method.getName())) {
                        return new CompletableFuture<>();
                    }

                    return method.invoke(original, args);
                }
        );
    }

    private static ScannableTable neverReplyingScannableTable() {
        return new DummyScannableTable();
    }

    private static UpdatableTable neverReplyingUpdatableTable() {
        return new DummyUpdatableTable();
    }
}
