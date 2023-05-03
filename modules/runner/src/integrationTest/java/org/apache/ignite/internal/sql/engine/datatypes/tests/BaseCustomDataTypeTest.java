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

package org.apache.ignite.internal.sql.engine.datatypes.tests;

import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryChecker.QueryTemplate;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Base class for test cases for {@link IgniteCustomType custom data type}.
 *
 * <p>Usage:
 * <ul>
 *     <li>{@link #runSql(String, Object...)} should be used instead of {@link #sql(String, Object...)} methods.</li>
 *     <li>{@link #checkQuery(String)} should be used instead of {@link #assertQuery(String)}.</li>
 *     <li>if result set includes a column with name {@code test_key} it expects it to be of a storage type.</li>
 * </ul>
 *
 * <p>Query string support the following template variables:
 * <ul>
 *     <li>{@code <type>} - an SQL name of custom data type.</li>
 *     <li>{@code $N} - the {@code N-th} value from sample values (0-based), converted to an SQL expression by
 *     {@link CustomDataTypeTestSpec#toValueExpr(Comparable)} call (0-based)</li>
 *     <li>{@code $N_lit} - the {@code N-th} value from sample values (0-based) in form of an SQL literal.</li>
 * </ul>
 *
 * @param <T> A storage type for a custom data type.
 */
public abstract class BaseCustomDataTypeTest<T extends Comparable<T>> extends ClusterPerClassIntegrationTest {

    protected CustomDataTypeTestSpec<T> testTypeSpec;

    protected List<T> values;

    protected NavigableSet<T> orderedValues;

    protected Class<?> storageType;

    protected TestDataSamples<T> dataSamples;

    @BeforeAll
    public void createTables() {
        testTypeSpec = getTypeSpec();
        storageType = testTypeSpec.storageType();

        dataSamples = testTypeSpec.createSamples(Commons.typeFactory());
        orderedValues = dataSamples.ordered();
        values = dataSamples.values();

        runSql("CREATE TABLE t(id INTEGER PRIMARY KEY, test_key <type>)");
    }

    @BeforeEach
    public void cleanTables() {
        sql("DELETE FROM t");
    }

    /** Creates a test type spec. **/
    protected abstract CustomDataTypeTestSpec<T> getTypeSpec();

    /**
     * Use this method instead of {@link #assertQuery(String)} because it replaces template variables.
     *
     * @param query A query .
     * @return A {@code QueryChecker}.
     * @see #assertQuery(String)
     */
    protected final QueryChecker checkQuery(String query) {
        QueryTemplate queryTemplate = createQueryTemplate(query);

        return new QueryChecker(null, queryTemplate) {
            @Override
            protected QueryProcessor getEngine() {
                return ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();
            }

            @Override
            protected void checkMetadata(AsyncSqlCursor<?> cursor) {
                Optional<ColumnMetadata> testKey = cursor.metadata().columns()
                        .stream()
                        .filter(c -> "test_key".equalsIgnoreCase(c.name()))
                        .findAny();

                testKey.ifPresent((c) -> {
                    ColumnType columnType = testTypeSpec.columnType();
                    String error = format(
                            "test_key should have type {}. This can happen if a query returned a column ", columnType
                    );
                    assertEquals(c.type(), columnType, error);
                });
            }
        };
    }

    /**
     * Use this method instead of {@link #sql(String, Object...)} because it replaces every {@code <type>} with the name of a custom data
     * type under test, and {@code $N} with corresponding values, where {@code N} is 1-indexed.
     *
     * @param query A query.
     * @return A {@code QueryChecker}.
     * @see #sql(String, Object...)
     */
    protected final List<List<Object>> runSql(String query, Object... params) {
        QueryTemplate queryTemplate = createQueryTemplate(query);
        String queryString = queryTemplate.createQuery();

        return sql(queryString, params);
    }

    /**
     * Creates a query template.
     * <ul>
     *     <li>{@code <type>} are replaced with a value of type name of {@link CustomDataTypeTestSpec}.</li>
     *     <li>{@code $N_lit} are replaced with corresponding literals, where {@code N} is 1-indexed</li>
     *     <li>{@code $N} are replaced with corresponding values, where {@code N} is 1-indexed</li>
     * </ul>
     */
    protected QueryTemplate createQueryTemplate(String query) {
        QueryTemplate parameterProvidingTemplate = new ParameterReplacingTemplate<>(testTypeSpec, query, values);
        return createQueryTemplate(parameterProvidingTemplate, testTypeSpec.typeName());
    }

    private static QueryTemplate createQueryTemplate(QueryTemplate template, String typeName) {
        return new QueryTemplate() {
            @Override
            public String originalQueryString() {
                return template.originalQueryString();
            }

            @Override
            public String createQuery() {
                String templateQuery = template.createQuery();
                return templateQuery.replace("<type>", typeName);
            }
        };
    }

    private static class ParameterReplacingTemplate<T extends Comparable<T>> implements QueryTemplate {

        private final CustomDataTypeTestSpec<T> testTypeSpec;

        private final String query;

        private final List<T> values;

        ParameterReplacingTemplate(CustomDataTypeTestSpec<T> spec, String query, List<T> values) {
            this.testTypeSpec = spec;
            this.query = query;
            this.values = values;
        }

        @Override
        public String originalQueryString() {
            return query;
        }

        @Override
        public String createQuery() {
            String q = query;

            for (var i = 0; i < values.size(); i++) {
                T value = values.get(i);

                if (testTypeSpec.hasLiterals()) {
                    String literalValue = testTypeSpec.toLiteral(value);
                    q = q.replace("$" + i + "_lit", literalValue);
                }

                String placeHolderValue = testTypeSpec.toValueExpr(value);
                q = q.replace("$" + i, placeHolderValue);
            }

            return q;
        }
    }

    protected final Stream<TestTypeArguments<T>> convertedFrom() {
        return TestTypeArguments.unary(testTypeSpec, dataSamples, dataSamples.min())
                .filter(args -> !Objects.equals(args.typeName(0), testTypeSpec.typeName()));
    }

    /** Returns binary operators as sql, enum name pairs. */
    protected static Stream<Arguments> binaryComparisonOperators() {
        return SqlKind.BINARY_COMPARISON.stream()
                // to support IS DISTINCT FROM/IS NOT DISTINCT FROM
                .map(o -> Arguments.of(o.sql.replace("_", " "), o.sql));
    }
}
