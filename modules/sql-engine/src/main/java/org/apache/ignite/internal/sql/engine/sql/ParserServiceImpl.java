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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * An implementation of {@link ParserService} that, apart of parsing, introduces cache of parsed results.
 */
public class ParserServiceImpl implements ParserService {

    private static final SqlWriterConfig NORMALIZED_SQL_WRITER_CONFIG = SqlPrettyWriter.config()
            // Uses the same config as SqlNode::toString
            .withDialect(AnsiSqlDialect.DEFAULT)
            .withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(0);

    /** {@inheritDoc} */
    @Override
    public ParsedResult parse(String query) {
        StatementParseResult parsedStatement = IgniteSqlParser.parse(query, StatementParseResult.MODE);

        SqlNode parsedTree = parsedStatement.statement();

        SqlQueryType queryType = Commons.getQueryType(parsedTree);

        SqlPrettyWriter w = new SqlPrettyWriter(NORMALIZED_SQL_WRITER_CONFIG);
        parsedTree.unparse(w, 0, 0);
        String normalizedQuery = w.toString();

        assert queryType != null : normalizedQuery;

        AtomicReference<SqlNode> holder = new AtomicReference<>(parsedTree);

        @SuppressWarnings("UnnecessaryLocalVariable")
        ParsedResult result = new ParsedResultImpl(
                queryType,
                query,
                normalizedQuery,
                parsedStatement.dynamicParamsCount(),
                () -> {
                    SqlNode ast = holder.getAndSet(null);

                    if (ast != null) {
                        return ast;
                    }

                    return IgniteSqlParser.parse(query, StatementParseResult.MODE).statement();
                }
        );

        return result;
    }

    /** {@inheritDoc} */
    @Override
    public List<ParsedResult> parseScript(String query) {
        ScriptParseResult parsedStatement = IgniteSqlParser.parse(query, ScriptParseResult.MODE);
        List<ParsedResult> results = new ArrayList<>(parsedStatement.results().size());

        for (StatementParseResult result : parsedStatement.results()) {
            SqlNode parsedTree = result.statement();
            SqlQueryType queryType = Commons.getQueryType(parsedTree);
            String normalizedQuery = parsedTree.toString();

            assert queryType != null : normalizedQuery;

            results.add(new ParsedResultImpl(
                    queryType,
                    normalizedQuery,
                    normalizedQuery,
                    result.dynamicParamsCount(),
                    () -> parsedTree
            ));
        }

        return results;
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
}
