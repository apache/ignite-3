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

package org.apache.ignite.internal.sql.engine.util;

import java.util.function.Consumer;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.util.QueryChecker.QueryTemplate;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Factory class for {@link QueryCheckerImpl}.
 *
 * @see QueryCheckerExtension
 */
class QueryCheckerFactoryImpl implements QueryCheckerFactory {
    private final Consumer<QueryChecker> onCreatedCallback;
    private final Consumer<QueryChecker> onUsedCallback;

    /**
     * Creates factory instance.
     *
     * @param onCreatedCallback Callback function that is called when a new QueryChecker is created.
     * @param onUsedCallback Callback function that is called when previously created QueryChecker is used.
     */
    QueryCheckerFactoryImpl(Consumer<QueryChecker> onCreatedCallback, Consumer<QueryChecker> onUsedCallback) {
        this.onCreatedCallback = onCreatedCallback;
        this.onUsedCallback = onUsedCallback;
    }

    @Override
    public QueryChecker create(
            String nodeName,
            QueryProcessor queryProcessor,
            HybridTimestampTracker observableTimeTracker,
            InternalTransaction tx,
            String query
    ) {
        return create(nodeName, queryProcessor, observableTimeTracker, (ignore) -> {}, tx, returnOriginalQuery(query));
    }

    @Override
    public QueryChecker create(
            String nodeName,
            QueryProcessor queryProcessor,
            HybridTimestampTracker observableTimeTracker,
            Consumer<ResultSetMetadata> metadataValidator,
            QueryTemplate queryTemplate
    ) {
        return create(nodeName, queryProcessor, observableTimeTracker, (ignore) -> {}, null, queryTemplate);
    }

    private QueryChecker create(
            String nodeName,
            QueryProcessor queryProcessor,
            HybridTimestampTracker observableTimeTracker,
            Consumer<ResultSetMetadata> metadataValidator,
            @Nullable InternalTransaction tx,
            QueryTemplate queryTemplate
    ) {
        QueryCheckerImpl queryChecker = new QueryCheckerImpl(tx, queryTemplate) {
            @Override
            protected QueryProcessor getEngine() {
                return queryProcessor;
            }

            @Override
            protected HybridTimestampTracker observableTimeTracker() {
                return observableTimeTracker;
            }

            @Override
            protected void checkMetadata(ResultSetMetadata metadata) {
                metadataValidator.accept(metadata);
            }

            @Override
            public void check() {
                onUsedCallback.accept(this);

                super.check();
            }

            @Override
            protected String nodeName() {
                return nodeName;
            }
        };

        onCreatedCallback.accept(queryChecker);

        return queryChecker;
    }

    /** Template that always returns original query. **/
    private static QueryTemplate returnOriginalQuery(String query) {
        return new QueryTemplate() {
            @Override
            public String originalQueryString() {
                return query;
            }

            @Override
            public String createQuery() {
                return query;
            }
        };
    }
}
