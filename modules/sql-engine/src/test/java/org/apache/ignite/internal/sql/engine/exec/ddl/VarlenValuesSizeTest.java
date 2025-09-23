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

package org.apache.ignite.internal.sql.engine.exec.ddl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.util.InjectQueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for varlen values of different sizes.
 */
@ExtendWith(QueryCheckerExtension.class)
public class VarlenValuesSizeTest extends BaseIgniteAbstractTest {

    @InjectQueryCheckerFactory
    private static QueryCheckerFactory queryCheckerFactory;

    private TestCluster testCluster;

    private TestNode gatewayNode;

    @BeforeEach
    public void initCluster() {
        testCluster = TestBuilders.cluster()
                .nodes("N1")
                .defaultDataProvider(tableName -> TestBuilders.tableScan(DataProvider.fromCollection(List.of())))
                .defaultAssignmentsProvider(tableName -> (partitionsCount, includeBackups) -> IntStream.range(0, partitionsCount)
                        .mapToObj(i -> List.of("N1"))
                        .collect(Collectors.toList()))
                .build();

        testCluster.start();

        gatewayNode = testCluster.node("N1");
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (testCluster != null) {
            testCluster.stop();
        }
    }

    @ParameterizedTest
    @MethodSource("sizes")
    public void testVarBinary(int size)  {
        {
            String stmt = format("CREATE TABLE t (id INT PRIMARY KEY, val VARBINARY({}))", size);
            AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(stmt);

            BatchedResult<InternalSqlRow> rs = cursor.requestNextAsync(1).join();
            assertNotNull(rs);
        }

        // Metadata
        assertQuery("SELECT val FROM t")
                .columnMetadata(new MetadataMatcher().type(ColumnType.BYTE_ARRAY).precision(size))
                .check();

        // CAST is not removed
        if (size > 1 && size < Math.pow(2, 16)) {
            byte[] val = (byte[]) SqlTestUtils.generateValueByType(ColumnType.BYTE_ARRAY, size, 0);
            assert val != null;
            byte[] shorten = Arrays.copyOf(val, val.length - 1);

            assertQuery(format("SELECT CAST(? AS VARBINARY({}))", shorten.length))
                    .withParams(val)
                    .returns(shorten)
                    .columnMetadata(new MetadataMatcher().type(ColumnType.BYTE_ARRAY).precision(shorten.length))
                    .check();
        }
    }

    @ParameterizedTest
    @MethodSource("sizes")
    public void testVarchar(int size)  {
        {
            String stmt = format("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR({}))", size);
            AsyncSqlCursor<InternalSqlRow> cursor = gatewayNode.executeQuery(stmt);

            BatchedResult<InternalSqlRow> rs = cursor.requestNextAsync(1).join();
            assertNotNull(rs);
        }

        // Metadata
        assertQuery("SELECT val FROM t")
                .columnMetadata(new MetadataMatcher().type(ColumnType.STRING).precision(size))
                .check();

        // CAST is not removed
        if (size > 1 && size < Math.pow(2, 16)) {
            String val = (String) SqlTestUtils.generateValueByType(ColumnType.STRING, size, 0);
            assert val != null;
            String shorten = val.substring(0, size - 1);

            assertQuery(format("SELECT CAST(? AS VARCHAR({}))", shorten.length()))
                    .withParams(val)
                    .returns(shorten)
                    .columnMetadata(new MetadataMatcher().type(ColumnType.STRING).precision(shorten.length()))
                    .check();
        }
    }

    private static Stream<Arguments> sizes() {
        return IntStream.range(0, 32)
                .map(i -> (int) Math.pow(2, i))
                .mapToObj(Arguments::of);
    }

    private QueryChecker assertQuery(String query) {
        //noinspection DataFlowIssue
        return queryCheckerFactory.create(
                gatewayNode.name(),
                new QueryProcessor() {
                    @Override
                    public CompletableFuture<QueryMetadata> prepareSingleAsync(
                            SqlProperties properties,
                            @Nullable InternalTransaction transaction,
                            String qry,
                            Object... params
                    ) {
                        QueryPlan queryPlan = gatewayNode.prepare(query, params);
                        return completedFuture(new QueryMetadata(queryPlan.metadata(), queryPlan.parameterMetadata()));
                    }

                    @Override
                    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryAsync(
                            SqlProperties properties,
                            HybridTimestampTracker observableTime,
                            @Nullable InternalTransaction transaction,
                            @Nullable CancellationToken token,
                            String qry,
                            Object... params
                    ) {
                        return completedFuture(gatewayNode.executeQuery(qry, params));
                    }

                    @Override
                    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                        return nullCompletedFuture();
                    }

                    @Override
                    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                        return nullCompletedFuture();
                    }
                },
                null,
                null,
                query
        );
    }
}
