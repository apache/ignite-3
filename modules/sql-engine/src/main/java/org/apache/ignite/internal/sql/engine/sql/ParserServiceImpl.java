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

package org.apache.ignite.internal.sql.engine.sql;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.function.Supplier;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;

/**
 * An implementation of {@link ParserService} that, apart of parsing, introduces cache of parsed results.
 */
public class ParserServiceImpl implements ParserService {

    private final Cache<String, ParsedResult> queryToParsedResultCache;

    /**
     * Constructs the object.
     *
     * @param cacheFactory A factory to create cache for parsed results.
     */
    public ParserServiceImpl(int cacheSize, CacheFactory cacheFactory) {
        this.queryToParsedResultCache = cacheFactory.create(cacheSize);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public ParsedResult parse(String query) {
        ParsedResult cachedResult = queryToParsedResultCache.get(query);

        if (cachedResult != null) {
            return cachedResult;
        }

        StatementParseResult parsedStatement = IgniteSqlParser.parse(query, StatementParseResult.MODE);

        SqlNode parsedTree = parsedStatement.statement();

        SqlQueryType queryType = Commons.getQueryType(parsedTree);

        assert queryType != null : parsedTree.toString();

        ParsedResult result = new ParsedResultImpl(
                queryType,
                query,
                parsedTree.toString(),
                parsedStatement.dynamicParamsCount(),
                () -> IgniteSqlParser.parse(query, StatementParseResult.MODE).statement()
        );

        if (shouldBeCached(queryType)) {
            queryToParsedResultCache.put(query, result);
        }

        return result;
    }

    static class ParsedResultImpl implements ParsedResult {
        private final SqlQueryType queryType;
        private final String originalQuery;
        private final String normalizedQuery;
        private final int dynamicParamCount;
        private final Supplier<SqlNode> parsedTreeSupplier;

        private ParsedResultImpl(
                SqlQueryType queryType,
                String originalQuery,
                String normalizedQuery,
                int dynamicParamCount,
                Supplier<SqlNode> parsedTreeSupplier
        ) {
            this.queryType = queryType;
            this.originalQuery = originalQuery;
            this.normalizedQuery = normalizedQuery;
            this.dynamicParamCount = dynamicParamCount;
            this.parsedTreeSupplier = parsedTreeSupplier;
        }

        /** {@inheritDoc} */
        @Override
        public SqlQueryType queryType() {
            return queryType;
        }

        /** {@inheritDoc} */
        @Override
        public String originalQuery() {
            return originalQuery;
        }

        /** {@inheritDoc} */
        @Override
        public String normalizedQuery() {
            return normalizedQuery;
        }

        /** {@inheritDoc} */
        @Override
        public int dynamicParamsCount() {
            return dynamicParamCount;
        }

        /** {@inheritDoc} */
        @Override
        public SqlNode parsedTree() {
            return parsedTreeSupplier.get();
        }
    }

    private static boolean shouldBeCached(SqlQueryType queryType) {
        return queryType == SqlQueryType.QUERY || queryType == SqlQueryType.DML;
    }
}
