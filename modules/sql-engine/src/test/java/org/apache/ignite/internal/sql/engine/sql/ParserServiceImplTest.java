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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.StatsCounter;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests to verify {@link ParserServiceImpl}.
 */
public class ParserServiceImplTest {
    enum Statement {
        QUERY("SELECT * FROM my_table", SqlQueryType.QUERY, true),
        DML("INSERT INTO my_table VALUES (1, 1)", SqlQueryType.DML, true),
        DDL("CREATE TABLE my_table (id INT PRIMARY KEY, avl INT)", SqlQueryType.DDL, false),
        EXPLAIN_QUERY("EXPLAIN PLAN FOR SELECT * FROM my_table", SqlQueryType.EXPLAIN, false),
        EXPLAIN_DML("EXPLAIN PLAN FOR INSERT INTO my_table VALUES (1, 1)", SqlQueryType.EXPLAIN, false);

        private final String text;
        private final SqlQueryType type;
        private final boolean cacheable;

        Statement(String text, SqlQueryType type, boolean cacheable) {
            this.text = text;
            this.type = type;
            this.cacheable = cacheable;
        }
    }

    @ParameterizedTest
    @EnumSource(Statement.class)
    void serviceAlwaysReturnsResultFromCacheIfPresent(Statement statement) {
        ParsedResult expected = new DummyParsedResults();

        ParsedResult actual = new ParserServiceImpl(0, new SameObjectCacheFactory(expected)).parse(statement.text);

        assertSame(actual, expected);
    }

    @ParameterizedTest
    @EnumSource(Statement.class)
    void serviceCachesOnlyCertainStatements(Statement statement) {
        ParserServiceImpl service = new ParserServiceImpl(
                Statement.values().length, CaffeineCacheFactory.INSTANCE
        );

        ParsedResult firstResult = service.parse(statement.text);
        ParsedResult secondResult = service.parse(statement.text);

        if (statement.cacheable) {
            assertSame(firstResult, secondResult);
        } else {
            assertNotSame(firstResult, secondResult);
        }
    }

    @ParameterizedTest
    @EnumSource(Statement.class)
    void serviceReturnsResultOfExpectedType(Statement statement) {
        ParserServiceImpl service = new ParserServiceImpl(0, EmptyCacheFactory.INSTANCE);

        ParsedResult result = service.parse(statement.text);

        assertThat(result.queryType(), is(statement.type));
    }

    @ParameterizedTest
    @EnumSource(Statement.class)
    void resultReturnedByServiceCreateNewInstanceOfTree(Statement statement) {
        ParserServiceImpl service = new ParserServiceImpl(0, EmptyCacheFactory.INSTANCE);

        ParsedResult result = service.parse(statement.text);

        SqlNode firstCall = result.parsedTree();
        SqlNode secondCall = result.parsedTree();

        assertNotSame(firstCall, secondCall);
        assertThat(firstCall.toString(), is(secondCall.toString()));
    }

    /**
     * Checks the parsing of a query containing multiple statements.
     *
     * <p>This parsing mode is only supported using the {@link ParserService#parseScript(String)} method,
     * so {@link ParserService#parse(String)}} must fail with a validation error.
     *
     * <p>Parsing produces a list of parsing results, each of which must match the parsing
     * result of the corresponding single statement.
     */
    @Test
    void parseMultiStatementQuery() {
        ParserService service = new ParserServiceImpl(0, EmptyCacheFactory.INSTANCE);

        List<Statement> statements = List.of(Statement.values());
        IgniteStringBuilder buf = new IgniteStringBuilder();

        for (Statement statement : statements) {
            buf.app(statement.text).app(';');
        }

        String multiStatementQuery = buf.toString();

        //noinspection ThrowableNotThrown
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Multiple statements are not allowed",
                () -> service.parse(multiStatementQuery)
        );

        List<ParsedResult> results = service.parseScript(multiStatementQuery);
        assertThat(results, hasSize(statements.size()));

        for (int i = 0; i < results.size(); i++) {
            ParsedResult result = results.get(i);
            ParsedResult singleStatementResult = service.parse(statements.get(i).text);

            assertThat(result.queryType(), equalTo(statements.get(i).type));
            assertThat(result.parsedTree(), notNullValue());
            assertThat(result.parsedTree().toString(), equalTo(singleStatementResult.parsedTree().toString()));
            assertThat(result.normalizedQuery(), equalTo(singleStatementResult.normalizedQuery()));
            assertThat(result.originalQuery(), equalTo(singleStatementResult.normalizedQuery()));
        }
    }

    /**
     * Parsed result that throws {@link AssertionError} on every method call.
     *
     * <p>Used in cases where you need to verify referential equality.
     */
    private static class DummyParsedResults implements ParsedResult {

        @Override
        public SqlQueryType queryType() {
            throw new AssertionError();
        }

        @Override
        public String originalQuery() {
            throw new AssertionError();
        }

        @Override
        public String normalizedQuery() {
            throw new AssertionError();
        }

        @Override
        public int dynamicParamsCount() {
            throw new AssertionError();
        }

        @Override
        public SqlNode parsedTree() {
            throw new AssertionError();
        }
    }

    /**
     * A factory that creates a cache that always return value passed to the constructor of factory.
     */
    private static class SameObjectCacheFactory implements CacheFactory {
        private final Object object;

        private SameObjectCacheFactory(Object object) {
            this.object = object;
        }

        @Override
        public <K, V> Cache<K, V> create(int size) {
            return new Cache<>() {
                @Override
                public @Nullable V get(K key) {
                    return (V) object;
                }

                @Override
                public V get(K key, Function<? super K, ? extends V> mappingFunction) {
                    return (V) object;
                }

                @Override
                public void put(K key, V value) {
                    // NO-OP
                }

                @Override
                public void clear() {
                    // NO-OP
                }

                @Override
                public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
                    return (V) object;
                }

                @Override
                public void removeIfValue(Predicate<? super V> valueFilter) {
                    // NO-OP.
                }
            };
        }

        @Override
        public <K, V> Cache<K, V> create(int size, StatsCounter statCounter) {
            return create(size);
        }
    }
}
