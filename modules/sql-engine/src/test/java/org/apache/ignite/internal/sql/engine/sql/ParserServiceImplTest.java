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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.util.Cache;
import org.apache.ignite.internal.sql.engine.util.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.jetbrains.annotations.Nullable;
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
                public void put(K key, V value) {
                    // NO-OP
                }

                @Override
                public void clear() {
                    // NO-OP
                }
            };
        }
    }
}
