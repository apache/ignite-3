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

import static org.apache.ignite.internal.util.ExceptionUtils.withCauseAndCode;
import static org.apache.ignite.lang.ErrorGroup.extractCauseMessage;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;

import java.io.Reader;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.util.SourceStringReader;
import org.apache.ignite.internal.generated.query.calcite.sql.IgniteSqlParserImpl;
import org.apache.ignite.sql.SqlException;

/**
 * Provides method for parsing SQL statements in SQL dialect of Apache Ignite 3.
 *
 * <p>One should use parsing methods defined in this class,
 * instead of creating {@link SqlParser} that use {@link IgniteSqlParserImpl} directly.
 */
public final class IgniteSqlParser  {

    /**
     * Parser configuration.
     */
    public static final SqlParser.Config PARSER_CONFIG = SqlParser.config()
            .withParserFactory(InternalIgniteSqlParser.FACTORY)
            .withLex(Lex.ORACLE)
            .withConformance(IgniteSqlConformance.INSTANCE);

    private IgniteSqlParser() {

    }

    /**
     * Parses the given SQL string in the specified {@link ParseMode mode},
     * which determines the result of the parse operation.
     *
     * @param sql  An SQL string.
     * @param mode  A parse mode.
     * @return  A parse result.
     *
     * @see StatementParseResult#MODE
     * @see ScriptParseResult#MODE
     */
    public static <T extends ParseResult> T parse(String sql, ParseMode<T> mode) {
        try (SourceStringReader reader = new SourceStringReader(sql)) {
            return parse(reader, mode);
        }
    }

    /**
     * Parses an SQL string from the given reader in the specified {@link ParseMode mode},
     * which determines the result of the parse operation.
     *
     * @param reader  A read that contains an SQL string.
     * @param mode  A parse mode.
     * @return  A parse result.
     *
     * @see StatementParseResult#MODE
     * @see ScriptParseResult#MODE
     */
    public static <T extends ParseResult> T parse(Reader reader, ParseMode<T> mode) {
        try  {
            InternalIgniteSqlParser.dynamicParamCount.set(null);

            SqlParser parser = SqlParser.create(reader, PARSER_CONFIG);
            SqlNodeList nodeList = parser.parseStmtList();

            Integer dynamicParamsCount = InternalIgniteSqlParser.dynamicParamCount.get();
            assert dynamicParamsCount != null : "dynamicParamCount has not been updated";

            return mode.createResult(nodeList.getList(), dynamicParamsCount);
        } catch (SqlParseException e) {
            throw withCauseAndCode(
                    SqlException::new,
                    STMT_PARSE_ERR,
                    "Failed to parse query: " + extractCauseMessage(e.getMessage()),
                    e);
        } finally {
            InternalIgniteSqlParser.dynamicParamCount.set(null);
        }
    }

    private static final class InternalIgniteSqlParser extends IgniteSqlParserImpl {

        /**
         * A factory that create instances of {@link IgniteSqlParser}.
         */
        private static final SqlParserImplFactory FACTORY = new SqlParserImplFactory() {
            @Override
            public SqlAbstractParserImpl getParser(Reader reader) {
                InternalIgniteSqlParser parser = new InternalIgniteSqlParser(reader);
                if (reader instanceof SourceStringReader) {
                    String sql = ((SourceStringReader) reader).getSourceString();
                    parser.setOriginalSql(sql);
                }
                return parser;
            }
        };


        // We store the number of dynamic parameters in a thread local since
        // it is not possible to access an instance of IgniteSqlParser created by a parser factory.
        static final ThreadLocal<Integer> dynamicParamCount = new ThreadLocal<>();

        InternalIgniteSqlParser(Reader reader) {
            super(reader);
        }

        /** {@inheritDoc} **/
        @Override
        public SqlNode parseSqlExpressionEof() throws Exception {
            try {
                return super.parseSqlExpressionEof();
            } finally {
                dynamicParamCount.set(nDynamicParams);
            }
        }

        /** {@inheritDoc} **/
        @Override
        public SqlNode parseSqlStmtEof() throws Exception {
            try {
                return super.parseSqlStmtEof();
            } finally {
                dynamicParamCount.set(nDynamicParams);
            }
        }

        /** {@inheritDoc} **/
        @Override
        public SqlNodeList parseSqlStmtList() throws Exception {
            try {
                return super.parseSqlStmtList();
            } finally {
                dynamicParamCount.set(nDynamicParams);
            }
        }
    }
}
