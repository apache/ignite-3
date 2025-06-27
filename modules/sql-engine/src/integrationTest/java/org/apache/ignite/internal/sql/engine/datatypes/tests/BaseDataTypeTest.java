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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomTypeSpec;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.NativeTypeWrapper;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryChecker.QueryTemplate;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Base class for test cases for data types including {@link IgniteCustomTypeSpec custom data types}.
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
 *     <li>{@code <type>} - an SQL name of a data type.</li>
 *     <li>{@code $N} - the {@code N-th} value from sample values (0-based), converted to an SQL expression by
 *     {@link DataTypeTestSpec#toLiteral(Comparable)} or by {@link DataTypeTestSpec#toValueExpr(Comparable)} ,
 *     if the type has designated literals or if type does have designated literals.</li>
 * </ul>
 *
 * <p>{@link QueryChecker} is automatically checks columns named {@code test_key}
 * that their {@link ColumnType column type} is equal to {@code columnType} defined by {@link DataTypeTestSpec}.
 *
 * <p><b>Testing types that not implement {@link Comparable}.</b>
 *
 * <p>In order to test non-comparable types (e.g. java arrays) values of those types must be passed as {@link NativeTypeWrapper}.
 * In that case {@code T} must be an implementation of a {@link NativeTypeWrapper} for that type.
 *
 * @param <T> A storage type of a data type.
 */
public abstract class BaseDataTypeTest<T extends Comparable<T>> extends BaseSqlIntegrationTest {

    protected DataTypeTestSpec<T> testTypeSpec;

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

        if (orderedValues.size() != 3) {
            throw new IllegalArgumentException("Test data should have 3 distinct values but got " + values);
        }

        // Use hash index, so primary key is not selected for range scan, when we want to check a specific sorted index.
        runSql("CREATE TABLE t(id INTEGER, test_key <type>, PRIMARY KEY USING HASH (id))");
    }

    @BeforeEach
    public void cleanTables() {
        sql("DELETE FROM t");
    }

    /** Creates a test type spec. **/
    protected abstract DataTypeTestSpec<T> getTypeSpec();

    /**
     * Use this method instead of {@link #assertQuery(String)} because it replaces template variables.
     *
     * @param query A query .
     * @return A {@code QueryChecker}.
     * @see #assertQuery(String)
     */
    protected final QueryChecker checkQuery(String query) {
        QueryTemplate queryTemplate = createQueryTemplate(query);

        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        return queryCheckerFactory.create(
                node.name(), node.queryEngine(), node.observableTimeTracker(), this::validateMetadata, queryTemplate
        );
    }

    private void validateMetadata(ResultSetMetadata metadata) {
        metadata.columns()
                .stream()
                .filter(c -> "test_key".equalsIgnoreCase(c.name()))
                .findAny()
                .ifPresent((c) -> {
                    ColumnType columnType = testTypeSpec.columnType();
                    String error = format(
                            "test_key should have type {}. This can happen if a query returned a column ", columnType
                    );
                    assertEquals(c.type(), columnType, error);
                });
    }

    /**
     * Use this method instead of {@link #sql(String, Object...)} because it replaces every {@code <type>} with the name of a data
     * type under test, and {@code $N} with corresponding values, where {@code N} is 0-indexed.
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
     *     <li>{@code <type>} are replaced with a value of type name of {@link DataTypeTestSpec}.</li>
     *     <li>{@code $N} are replaced with corresponding values provided by
     *     {@link DataTypeTestSpec#toValueExpr(Comparable)} or {@link DataTypeTestSpec#toLiteral(Comparable)}
     *     depending on whether type supports literals or not}. {@code N} is 0-indexed.</li>
     * </ul>
     */
    private QueryTemplate createQueryTemplate(String query) {
        boolean useLiterals = testTypeSpec.hasLiterals();
        QueryTemplate parameterProvidingTemplate = new ParameterReplacingTemplate<>(testTypeSpec, query, values, useLiterals);
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

        private final DataTypeTestSpec<T> testTypeSpec;

        private final String query;

        private final List<T> values;

        private final boolean useLiterals;

        ParameterReplacingTemplate(DataTypeTestSpec<T> spec, String query, List<T> values, boolean useLiterals) {
            this.testTypeSpec = spec;
            this.query = query;
            this.values = values;
            this.useLiterals = useLiterals;
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
                String placeHolderValue;
                if (useLiterals) {
                    placeHolderValue = testTypeSpec.toLiteral(value);
                } else {
                    placeHolderValue = testTypeSpec.toValueExpr(value);
                }
                q = q.replace("$" + i, placeHolderValue);
            }

            return q;
        }
    }

    /** Returns {@link TestTypeArguments} that include types this data type can be converted from. **/
    protected final Stream<TestTypeArguments<T>> convertedFrom() {
        return TestTypeArguments.unary(testTypeSpec, dataSamples, dataSamples.min())
                .filter(args -> !Objects.equals(args.typeName(0), testTypeSpec.typeName()));
    }

    /** Returns binary operators as sql, enum name pairs. */
    protected static Stream<Arguments> binaryComparisonOperators() {
        return SqlKind.BINARY_COMPARISON.stream()
                // to support IS DISTINCT FROM/IS NOT DISTINCT FROM
                .map(o -> Arguments.of(o.sql.replace("_", " ")));
    }

    protected final void insertValues() {
        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");
    }
}
