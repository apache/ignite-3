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
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests cases for cancellation due to timeout. */
@SuppressWarnings("ThrowableNotThrown")
public class QueryTimeoutTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "gateway";

    private final AtomicBoolean ignoreCatalogUpdates = new AtomicBoolean(false);

    private static final SqlProperties PROPS_WITH_TIMEOUT = SqlPropertiesHelper.newBuilder()
            // Relatively high timeout is set to make tests stable on TC. The reason is that
            // query have to make it to the final point which is varied from test to test in
            // order to 1) make sure timeout is handled properly at particular stages of query
            // execution, and 2) to fail with proper exception class.
            .set(QueryProperty.QUERY_TIMEOUT, 2_000L)
            .build();

    private TestCluster cluster;
    private TestNode gatewayNode;

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
                QueryCancelledException.class,
                () -> gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "CREATE TABLE x (id INTEGER PRIMARY KEY, val INTEGER)"),
                QueryCancelledException.TIMEOUT_MSG
        );
    }

    @Test
    void testTimeoutKill() {
        assertThrows(
                QueryCancelledException.class,
                () -> gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "KILL QUERY '" + randomUUID() + '\''),
                QueryCancelledException.TIMEOUT_MSG
        );
    }

    @Test
    void testTimeoutSelectCount() {
        AsyncSqlCursor<?> cursor = gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "SELECT COUNT(*) FROM my_table");

        assertThat(
                cursor.requestNextAsync(1),
                willThrowWithCauseOrSuppressed(
                        QueryCancelledException.class,
                        QueryCancelledException.TIMEOUT_MSG
                )
        );
    }

    @Test
    void testTimeoutKvGet() {
        AsyncSqlCursor<?> cursor = gatewayNode.executeQuery(PROPS_WITH_TIMEOUT, "SELECT * FROM my_table WHERE id = ?", 2);

        assertThat(
                cursor.requestNextAsync(1),
                willThrowWithCauseOrSuppressed(
                        QueryCancelledException.class,
                        QueryCancelledException.TIMEOUT_MSG
                )
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
        return new ScannableTable() {
            @Override
            public <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory, @Nullable BitSet requiredColumns) {
                return SubscriptionUtils.fromIterable(new CompletableFuture<>());
            }

            @Override
            public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, @Nullable RangeCondition<RowT> cond,
                    @Nullable BitSet requiredColumns) {
                return SubscriptionUtils.fromIterable(new CompletableFuture<>());
            }

            @Override
            public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, RowT key, @Nullable BitSet requiredColumns) {
                return SubscriptionUtils.fromIterable(new CompletableFuture<>());
            }

            @Override
            public <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(ExecutionContext<RowT> ctx,
                    @Nullable InternalTransaction explicitTx, RowFactory<RowT> rowFactory, RowT key, @Nullable BitSet requiredColumns) {
                return new CompletableFuture<>();
            }

            @Override
            public CompletableFuture<Long> estimatedSize() {
                return new CompletableFuture<>();
            }
        };
    }

    private static UpdatableTable neverReplyingUpdatableTable() {
        return new UpdatableTable() {
            @Override
            public TableDescriptor descriptor() {
                throw new UnsupportedOperationException("UpdatableTable#descriptor");
            }

            @Override
            public <RowT> CompletableFuture<?> insertAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
                return new CompletableFuture<>();
            }

            @Override
            public <RowT> CompletableFuture<Void> insert(@Nullable InternalTransaction explicitTx, ExecutionContext<RowT> ectx, RowT row) {
                return new CompletableFuture<>();
            }

            @Override
            public <RowT> CompletableFuture<?> upsertAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
                return new CompletableFuture<>();
            }

            @Override
            public <RowT> CompletableFuture<?> deleteAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
                return new CompletableFuture<>();
            }
        };
    }
}
