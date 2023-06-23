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

import static org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl.validateParsedStatement;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.apache.ignite.sql.SqlException;
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
    public void queryCache(String query1, String query2, Object[] params) {
        // Preparing query caches plan for both query and normalized query.
        assertThat(service.prepareAsync(query1, queryCtx, createContext(params)), willBe(notNullValue()));
        assertEquals(1, service.parseCacheSize());
        assertEquals(1, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(1)).apply(Mockito.any(), Mockito.any());

        // Preparing same query returns plan from cache.
        assertThat(service.prepareAsync(query1, queryCtx, createContext(params)), willBe(notNullValue()));
        assertEquals(1, service.parseCacheSize());
        assertEquals(1, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(1)).apply(Mockito.any(), Mockito.any());

        // Preparing similar query returns cached plan and also cache the plan for the query.
        assertThat(service.prepareAsync(query2, queryCtx, createContext(params)), willBe(notNullValue()));
        assertEquals(2, service.parseCacheSize());
        assertEquals(1, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(1)).apply(Mockito.any(), Mockito.any());
    }


    private SqlNode parse(String sql, Object... params) {
        // Parse query
        StatementParseResult parseResult = IgniteSqlParser.parse(sql, StatementParseResult.MODE);
        SqlNode sqlNode = parseResult.statement();

        // Validate statement
        validateParsedStatement(queryCtx, parseResult, sqlNode, params);

        return sqlNode;
    }

    @Test
    public void ddlBypassCache() {
        String query = "CREATE TABLE tbl0(id INTEGER PRIMARY KEY, val VARCHAR);";

        assertThat(service.prepareAsync(query, queryCtx, createContext()), willBe(notNullValue()));
        assertEquals(0, service.planCacheSize());
        Mockito.verify(ddlPlannerSpy, Mockito.times(1)).convert(Mockito.any(), Mockito.any());
        // DDL goes a separate flow via ddl converter.
        Mockito.verifyNoInteractions(queryPlannerSpy);

        // Prepare DDL query once again.
        assertThat(service.prepareAsync(query, queryCtx, createContext()), willBe(notNullValue()));
        assertEquals(0, service.planCacheSize());
        Mockito.verify(ddlPlannerSpy, Mockito.times(2)).convert(Mockito.any(), Mockito.any());
    }

    @Test
    public void errors() {
        String query = "invalid query"; // Invalid table name.

        assertThat(service.prepareAsync(query, queryCtx, createContext()), willThrow(SqlException.class));
        assertEquals(0, service.planCacheSize());
        Mockito.verifyNoInteractions(queryPlannerSpy);

        query = "SELECT * FROM tbl2 WHERE id > 0"; // Invalid table name.

        assertThat(service.prepareAsync(query, queryCtx, createContext()), willThrow(CalciteContextException.class));
        assertEquals(1, service.planCacheSize());
        Mockito.verifyNoInteractions(queryPlannerSpy);
    }

    @Test
    public void disabledCache() {
        PrepareServiceImpl service = new PrepareServiceImpl("node", 0, ddlPlannerSpy, queryPlannerSpy);

        service.start();

        String query = "SELECT * FROM tbl WHERE id > 0";

        assertThat(service.prepareAsync(query, queryCtx, createContext()), willBe(notNullValue()));
        assertEquals(0, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(1)).apply(Mockito.any(), Mockito.any());

        assertThat(service.prepareAsync(query, queryCtx, createContext()), willBe(notNullValue()));
        assertEquals(0, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(2)).apply(Mockito.any(), Mockito.any());
    }

    @Test
    public void normalizedQuery() {
        SqlNode queryAst = parse("SELECT NULL");

        String normalizedQuery = queryAst.toString();

        assertThat(service.prepareAsync(normalizedQuery, queryCtx, createContext()), willBe(notNullValue()));
        assertEquals(1, service.planCacheSize());
    }

    @Test
    public void explainUsesCachedPlans() {
        String query = "SELECT * FROM tbl WHERE id > 0";
        String explainQuery = "EXPLAIN PLAN FOR SELECT * FROM tbl WHERE id > 0";
        String explainQuery2 = "EXPLAIN PLAN FOR SELECT * /* comment */ FROM tbl WHERE id > 0";

        // Ensure explain don't cache anything.
        assertThat(service.prepareAsync(explainQuery, queryCtx, createContext()), willBe(notNullValue()));
        assertEquals(0, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(1)).apply(Mockito.any(), Mockito.any());

        // Cache query plan.
        assertThat(service.prepareAsync(query, queryCtx, createContext()), willBe(notNullValue()));
        assertEquals(1, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(2)).apply(Mockito.any(), Mockito.any());

        // Check explain gets plan from cache.
        assertThat(service.prepareAsync(explainQuery, queryCtx, createContext()), willBe(notNullValue()));
        assertEquals(1, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(2)).apply(Mockito.any(), Mockito.any());

        // Check explain gets plan from cache for similar query.
        assertThat(service.prepareAsync(explainQuery2, queryCtx, createContext()), willBe(notNullValue()));
        assertEquals(1, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(2)).apply(Mockito.any(), Mockito.any());
    }

    @Test
    public void resetCache() {
        assertEquals(0, service.planCacheSize());

        // Fill caches.
        assertThat(service.prepareAsync("SELECT * FROM tbl WHERE id > 0", queryCtx, createContext()), willBe(notNullValue()));
        assertThat(service.prepareAsync("SELECT * FROM tbl WHERE id > 1", queryCtx, createContext()), willBe(notNullValue()));

        assertEquals(2, service.parseCacheSize());
        assertEquals(2, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(2)).apply(Mockito.any(), Mockito.any());

        // Drop cached plans.
        service.invalidateCachedPlans();

        assertEquals(2, service.parseCacheSize());
        assertEquals(0, service.planCacheSize());

        assertThat(service.prepareAsync("SELECT * FROM tbl WHERE id > 0", queryCtx, createContext()), willBe(notNullValue()));
        assertThat(service.prepareAsync("SELECT * FROM tbl WHERE id > 1", queryCtx, createContext()), willBe(notNullValue()));

        assertEquals(2, service.parseCacheSize());
        assertEquals(2, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(4)).apply(Mockito.any(), Mockito.any());

        // Invalidate parser cache and check reusing cached plans.
        service.invalidateParserCache();

        assertThat(service.prepareAsync("SELECT * FROM tbl WHERE id > 0", queryCtx, createContext()), willBe(notNullValue()));

        assertEquals(1, service.parseCacheSize());
        assertEquals(2, service.planCacheSize());
        Mockito.verify(queryPlannerSpy, Mockito.times(4)).apply(Mockito.any(), Mockito.any());
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
