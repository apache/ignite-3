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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.fsm.DdlBatchGroup;
import org.apache.ignite.internal.sql.engine.exec.fsm.DdlBatchingHelper;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.Nullable;

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

        return prepareSingleResult(query, parsedTree, parsedStatement.dynamicParamsCount());
    }

    /** {@inheritDoc} */
    @Override
    public List<ParsedResult> parseScript(String query) {
        ScriptParseResult parsedStatement = IgniteSqlParser.parse(query, ScriptParseResult.MODE);

        if (parsedStatement.results().size() == 1) {
            StatementParseResult parseResult = parsedStatement.results().get(0);

            return List.of(
                    prepareSingleResult(query, parseResult.statement(), parseResult.dynamicParamsCount())
            );
        }

        List<ParsedResult> results = new ArrayList<>(parsedStatement.results().size());

        List<String> scriptLines = query.lines().collect(Collectors.toList());
        for (StatementParseResult result : parsedStatement.results()) {
            SqlNode parsedTree = result.statement();
            SqlQueryType queryType = Commons.getQueryType(parsedTree);
            String originalQuery = resembleOriginalQuery(scriptLines, parsedTree.getParserPosition());
            String normalizedQuery = parsedTree.toString();

            assert queryType != null : normalizedQuery;

            AtomicBoolean used = new AtomicBoolean();

            results.add(new ParsedResultImpl(
                    queryType,
                    originalQuery,
                    normalizedQuery,
                    result.dynamicParamsCount(),
                    DdlBatchingHelper.extractDdlBatchGroup(parsedTree),
                    queryType == SqlQueryType.TX_CONTROL ? () -> parsedTree
                            : () -> {
                                if (!used.compareAndSet(false, true)) {
                                    throw new IllegalStateException("Parsed result of script is not reusable.");
                                }

                                return parsedTree;
                            }
            ));
        }

        return results;
    }

    private static String resembleOriginalQuery(
            List<String> scriptLines, SqlParserPos parserPos
    ) {
        StringBuilder sb = new StringBuilder();

        // Positions in ParserPos are 1-based.
        int startLine = parserPos.getLineNum() - 1;
        int startColumn = parserPos.getColumnNum() - 1;
        int endLine = parserPos.getEndLineNum() - 1;
        int endColumn = parserPos.getEndColumnNum(); // do not substruct 1 to preserve semicolon
        for (int line = startLine; line <= endLine; line++) {
            String lineString = scriptLines.get(line);

            sb.append(
                    lineString,
                    line == startLine ? startColumn : 0,
                    line == endLine ? Math.min(endColumn + 1, lineString.length()) : lineString.length()
            );

            if (line < endLine) {
                sb.append(System.lineSeparator());
            }
        }

        return sb.toString().trim();
    }

    private static ParsedResult prepareSingleResult(String originalQuery, SqlNode parsedTree, int dynamicParamsCount) {
        SqlQueryType queryType = Commons.getQueryType(parsedTree);

        SqlPrettyWriter w = new SqlPrettyWriter(NORMALIZED_SQL_WRITER_CONFIG);
        parsedTree.unparse(w, 0, 0);
        String normalizedQuery = w.toString();

        assert queryType != null : normalizedQuery;

        AtomicReference<SqlNode> holder = new AtomicReference<>(parsedTree);

        return new ParsedResultImpl(
                queryType,
                originalQuery,
                normalizedQuery,
                dynamicParamsCount,
                DdlBatchingHelper.extractDdlBatchGroup(parsedTree),
                () -> {
                    // Descendants of SqlNode class are mutable, thus we must use every
                    // syntax node only once to avoid problem. But we already parsed the
                    // query once to get normalized result. An `unparse` operation is known
                    // to be safe, so let's reuse result of parsing for the first invocation
                    // of `parsedTree` method to avoid double-parsing for one time queries.
                    SqlNode ast = holder.getAndSet(null);

                    if (ast != null) {
                        return ast;
                    }

                    return IgniteSqlParser.parse(originalQuery, StatementParseResult.MODE).statement();
                }
        );
    }

    static class ParsedResultImpl implements ParsedResult {
        private final SqlQueryType queryType;
        private final String originalQuery;
        private final String normalizedQuery;
        private final int dynamicParamCount;
        private final Supplier<SqlNode> parsedTreeSupplier;
        private final DdlBatchGroup ddlBatchGroup;

        private ParsedResultImpl(
                SqlQueryType queryType,
                String originalQuery,
                String normalizedQuery,
                int dynamicParamCount,
                @Nullable DdlBatchGroup ddlBatchGroup,
                Supplier<SqlNode> parsedTreeSupplier
        ) {
            this.queryType = queryType;
            this.originalQuery = originalQuery;
            this.normalizedQuery = normalizedQuery;
            this.dynamicParamCount = dynamicParamCount;
            this.parsedTreeSupplier = parsedTreeSupplier;
            this.ddlBatchGroup = ddlBatchGroup;

            // Here we ensure that DDL batch group is set for DDL queries.
            // For the case the one missed adding DDL operation to the Multi-statement test.
            assert queryType != SqlQueryType.DDL || ddlBatchGroup != null : "DDL query without batch group";
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
        public @Nullable DdlBatchGroup ddlBatchGroup() {
            return ddlBatchGroup;
        }

        /** {@inheritDoc} */
        @Override
        public SqlNode parsedTree() {
            return parsedTreeSupplier.get();
        }
    }
}
