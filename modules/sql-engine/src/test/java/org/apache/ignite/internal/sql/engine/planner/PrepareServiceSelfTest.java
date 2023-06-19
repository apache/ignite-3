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

import static org.apache.ignite.internal.sql.engine.SqlQueryProcessor.validateParsedStatement;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlannerHelper;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlParser;
import org.apache.ignite.internal.sql.engine.sql.StatementParseResult;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Prepare service self test.
 */
public class PrepareServiceSelfTest extends AbstractPlannerTest {
    private final QueryContext queryCtx = QueryContext.create(SqlQueryType.ALL);
    private PrepareServiceImpl service;

    private BiFunction<SqlNode, IgnitePlanner, IgniteRel> queryPlannerSpy;
    private DdlSqlToCommandConverter ddlPlannerSpy;

    @BeforeEach
    void setUp() {
        queryPlannerSpy = Mockito.spy(new QueryOptimizer());
        ddlPlannerSpy = Mockito.spy(new DdlSqlToCommandConverter(Map.of(), () -> "default"));

        service = new PrepareServiceImpl(
                "node",
                10,
                ddlPlannerSpy,
                queryPlannerSpy
        );

        service.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        service.stop();
    }

    static Stream<Arguments> queries() {
        return Stream.of(
                // Query
                Arguments.of("SELECT * FROM tbl WHERE id > 0", "SELECT * /* a comment. */ FROM tbl WHERE id > 0", new Object[]{}),
                // Query with args
                Arguments.of("SELECT * FROM tbl WHERE id > ?", "SELECT * /* a comment. */ FROM tbl WHERE id > ?", new Object[]{1}),
                // DML
                Arguments.of("INSERT INTO tbl VALUES (1, '42')", "INSERT INTO /* a comment. */ tbl VALUES (1, '42')", new Object[]{}),
                // DML with args
                Arguments.of("INSERT INTO tbl VALUES (?, ?)", "INSERT INTO /* a comment. */ tbl VALUES (?, ?)", new Object[]{1, "42"})
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    public void normalizedQueryCache(String query1, String query2, Object[] params) {
        // Parse query and check nothing cached.
        SqlNode query1Ast = parse(query1, params);
        assertThat(service.cache(), Matchers.anEmptyMap());
        Mockito.verifyNoInteractions(queryPlannerSpy);

        // Preparing AST caches a normalized query plan.
        assertThat(service.prepareAsync(query1Ast, createContext(params)), willBe(notNullValue()));
        assertThat(service.cache(), Matchers.aMapWithSize(1));
        Mockito.verify(queryPlannerSpy, Mockito.times(1)).apply(Mockito.any(), Mockito.any());

        // Preparing same AST returns plan from cache.
        query1Ast = parse(query1, params);
        assertThat(service.prepareAsync(query1Ast, createContext(params)), willBe(notNullValue()));
        assertThat(service.cache(), Matchers.aMapWithSize(1));
        Mockito.verify(queryPlannerSpy, Mockito.times(1)).apply(Mockito.any(), Mockito.any());

        // Prepare similar query returns plan from cache.
        SqlNode query2Ast = parse(query2, params);

        assertThat(service.prepareAsync(query2Ast, createContext(params)), willBe(notNullValue()));
        assertThat(service.cache(), Matchers.aMapWithSize(1));
        Mockito.verify(queryPlannerSpy, Mockito.times(1)).apply(Mockito.any(), Mockito.any());
    }

    private SqlNode parse(String sql, Object... params) {
        // Parse query
        StatementParseResult parseResult = IgniteSqlParser.parse(sql, StatementParseResult.MODE);
        SqlNode sqlNode = parseResult.statement();

        // Validate statement
        validateParsedStatement(queryCtx, null, parseResult, sqlNode, params);

        return sqlNode;
    }

    @Test
    public void ddlBypassCache() {
        String query = "CREATE TABLE tbl0(id INTEGER PRIMARY KEY, val VARCHAR);";

        assertThat(service.prepareAsync(parse(query), createContext()), willBe(notNullValue()));
        assertThat(service.cache(), Matchers.aMapWithSize(0));
        Mockito.verify(ddlPlannerSpy, Mockito.times(1)).convert(Mockito.any(), Mockito.any());
        // DDL goes a separate flow via ddl converter.
        Mockito.verifyNoInteractions(queryPlannerSpy);
    }

    @Test
    public void errors() {
        String query = "SELECT * FROM tbl2 WHERE id > 0"; // Invalid table name.

        assertThat(service.prepareAsync(parse(query), createContext()), willThrow(CalciteContextException.class));
        assertThat(service.cache(), Matchers.aMapWithSize(1));
        assertTrue(service.cache().values().iterator().next().isCompletedExceptionally());
        Mockito.verifyNoInteractions(queryPlannerSpy);
    }

    @Test
    public void disabledCache() {
        PrepareServiceImpl service = new PrepareServiceImpl("node", 0, ddlPlannerSpy, queryPlannerSpy);

        service.start();

        String query = "SELECT * FROM tbl WHERE id > 0";

        assertThat(service.prepareAsync(parse(query), createContext()), willBe(notNullValue()));
        assertThat(service.cache(), Matchers.aMapWithSize(0));
        Mockito.verify(queryPlannerSpy, Mockito.times(1)).apply(Mockito.any(), Mockito.any());

        assertThat(service.prepareAsync(parse(query), createContext()), willBe(notNullValue()));
        assertThat(service.cache(), Matchers.aMapWithSize(0));
        Mockito.verify(queryPlannerSpy, Mockito.times(2)).apply(Mockito.any(), Mockito.any());
    }

    @Test
    public void normalizedQueryCaching() {
        SqlNode queryAst = parse("SELECT NULL");

        String normalizedQuery = queryAst.toString();

        assertThat(service.prepareAsync(parse(normalizedQuery), createContext()), willBe(notNullValue()));
        assertThat(service.cache(), Matchers.aMapWithSize(1));
    }

    @Test
    public void resetCache() {
        assertThat(service.cache(), Matchers.anEmptyMap());

        assertThat(service.prepareAsync(parse("SELECT * FROM tbl WHERE id > 0"), createContext()), willBe(notNullValue()));
        assertThat(service.prepareAsync(parse("SELECT * FROM tbl WHERE id > 1"), createContext()), willBe(notNullValue()));
        assertThat(service.prepareAsync(parse("SELECT * FROM tbl WHERE id > 2"), createContext()), willBe(notNullValue()));
        assertThat(service.prepareAsync(parse("SELECT * FROM tbl WHERE id > 3"), createContext()), willBe(notNullValue()));
        assertThat(service.cache(), Matchers.aMapWithSize(4));

        service.resetCache();

        assertThat(service.cache(), Matchers.anEmptyMap());

        assertThat(service.prepareAsync(parse("SELECT * FROM tbl WHERE id > 0"), createContext()), willBe(notNullValue()));
        assertThat(service.cache(), Matchers.aMapWithSize(1));
    }

    private BaseQueryContext createContext(Object... params) {
        return baseQueryContext(
                List.of(createSchema(
                        TestBuilders.table()
                                .name("TBL")
                                .distribution(IgniteDistributions.broadcast())
                                .addColumn("ID", NativeTypes.INT32)
                                .addColumn("VAL", NativeTypes.STRING)
                                .build()
                )),
                null,
                params
        );
    }

    private static class QueryOptimizer implements BiFunction<SqlNode, IgnitePlanner, IgniteRel> {
        @Override
        public IgniteRel apply(SqlNode sqlNode, IgnitePlanner ignitePlanner) {
            return PlannerHelper.optimize(sqlNode, ignitePlanner);
        }
    }
}
