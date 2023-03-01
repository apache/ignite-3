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
import static org.apache.ignite.lang.ErrorGroups.Sql.QUERY_INVALID_ERR;

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
 * Implementation of SQL parser.
 */
public final class IgniteSqlParser extends IgniteSqlParserImpl {

    /**
     * A factory that create instances of {@link IgniteSqlParser}.
     */
    public static final SqlParserImplFactory FACTORY = new SqlParserImplFactory() {
        @Override
        public SqlAbstractParserImpl getParser(Reader reader) {
            IgniteSqlParser parser = new IgniteSqlParser(reader);
            if (reader instanceof SourceStringReader) {
                String sql = ((SourceStringReader) reader).getSourceString();
                parser.setOriginalSql(sql);
            }
            return parser;
        }
    };

    /**
     * Parser configuration.
     */
    public static final SqlParser.Config PARSER_CONFIG = SqlParser.config()
            .withParserFactory(FACTORY)
            .withLex(Lex.ORACLE)
            .withConformance(IgniteSqlConformance.INSTANCE);

    // We store the number of dynamic parameters in a thread local since
    // it is not possible to access an instance of IgniteSqlParser created by a parser factory.
    private static final ThreadLocal<Integer> dynamicParamCount = new ThreadLocal<>();

    /** Constructor. **/
    IgniteSqlParser(Reader reader) {
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

    /**
     * Parses an SQL statement or a sequence of statements using {@link #PARSER_CONFIG default parser config}.
     *
     * @param qry Query string.
     * @return Parsed query.
     */
    public static IgniteSqlScriptNode parse(String qry) {
        try {
            return parse(new SourceStringReader(qry), PARSER_CONFIG);
        } catch (SqlParseException e) {
            throw withCauseAndCode(
                    SqlException::new,
                    QUERY_INVALID_ERR,
                    "Failed to parse query: " + extractCauseMessage(e.getMessage()),
                    e);
        }
    }

    /**
     * Parses an SQL statement or a sequence of statements using the given parser configuration.
     *
     * @param qry Query string.
     * @param parserCfg Parser config.
     * @return Parsed query.
     */
    public static IgniteSqlScriptNode parse(String qry, SqlParser.Config parserCfg) {
        try {
            return parse(new SourceStringReader(qry), parserCfg);
        } catch (SqlParseException e) {
            throw withCauseAndCode(
                    SqlException::new,
                    QUERY_INVALID_ERR,
                    "Failed to parse query: " + extractCauseMessage(e.getMessage()),
                    e);
        }
    }

    /**
     * Parses an SQL statement or a sequence of statements using {@link #PARSER_CONFIG default parser config}.
     *
     * @param reader Source string reader.
     * @return Parsed query.
     * @throws SqlParseException on parse error.
     */
    public static IgniteSqlScriptNode parse(Reader reader) throws SqlParseException {
        return parse(reader, PARSER_CONFIG);
    }

    /**
     * Parses an SQL statement or a sequence of statements using the given parser configuration.
     *
     * @param reader Source string reader.
     * @param parserCfg Parser config.
     * @return Parsed query.
     * @throws SqlParseException on parse error.
     */
    public static IgniteSqlScriptNode parse(Reader reader, SqlParser.Config parserCfg) throws SqlParseException {
        try {
            dynamicParamCount.set(null);

            SqlParser parser = SqlParser.create(reader, parserCfg);
            SqlNodeList nodeList = parser.parseStmtList();

            Integer dynamicParamsCount = dynamicParamCount.get();
            assert dynamicParamsCount != null : "dynamicParamCount has not been updated";

            return new IgniteSqlScriptNode(nodeList, dynamicParamsCount);
        } finally {
            dynamicParamCount.set(null);
        }

    }
}
