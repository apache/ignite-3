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

package org.apache.ignite.internal.sql.engine.planner;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.util.InjectQueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.CancellationToken;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for selectivity estimations.
 */
@ExtendWith(QueryCheckerExtension.class)
public class BaseRowsProcessedEstimationTest extends BaseIgniteAbstractTest {
    @InjectQueryCheckerFactory
    private static QueryCheckerFactory queryCheckerFactory;

    static QueryChecker assertQuery(TestNode node, String query) {
        return queryCheckerFactory.create(
                node.name(),
                new QueryProcessor() {
                    @Override
                    public CompletableFuture<QueryMetadata> prepareSingleAsync(SqlProperties properties,
                            @Nullable InternalTransaction transaction, String qry, Object... params) {
                        throw new AssertionError("Should not be called");
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
                        return completedFuture(node.executeQuery(qry));
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

    static Matcher<Integer> approximatelyEqual(double expected) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof Integer)) {
                    return false;
                }

                double value = ((Integer) o).doubleValue();
                return Math.abs(1 - (value / expected)) < 0.05;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("equals to ").appendValue(expected).appendText("±5%");
            }
        };
    }
}
