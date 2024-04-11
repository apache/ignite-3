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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.convertSqlRows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertionsAsync;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Type;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.hint.IgniteHint;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.tx.IgniteTransactions;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;

/**
 * Query checker base class.
 */
abstract class QueryCheckerImpl implements QueryChecker {
    private static final IgniteLogger LOG = Loggers.forClass(QueryCheckerImpl.class);

    private final QueryTemplate queryTemplate;

    private final ArrayList<Matcher<String>> planMatchers = new ArrayList<>();

    private final ArrayList<String> disabledRules = new ArrayList<>();

    private ResultChecker resultChecker;

    private List<ColumnMatcher> metadataMatchers;

    private boolean ordered;

    private Object[] params = OBJECT_EMPTY_ARRAY;

    private ZoneId timeZoneId = SqlQueryProcessor.DEFAULT_TIME_ZONE_ID;

    private final @Nullable InternalTransaction tx;

    /**
     * Constructor.
     *
     * @param tx Transaction.
     * @param queryTemplate A query template.
     */
    QueryCheckerImpl(@Nullable InternalTransaction tx, QueryTemplate queryTemplate) {
        this.tx = tx;
        this.queryTemplate = new AddDisabledRulesTemplate(Objects.requireNonNull(queryTemplate), disabledRules);
    }

    /**
     * Sets ordered.
     *
     * @return This.
     */
    @Override
    public QueryChecker ordered() {
        ordered = true;

        return this;
    }

    /**
     * Sets params.
     *
     * @return This.
     */
    @Override
    public QueryChecker withParams(Object... params) {
        // let's interpret null array as simple single null.
        if (params == null) {
            params = QueryChecker.NULL_AS_VARARG;
        }

        this.params = Arrays.stream(params).map(NativeTypeWrapper::unwrap).toArray();

        return this;
    }

    /**
     * Set a single param. Useful for specifying array parameters w/o triggering IDE-inspection warnings about confusing varargs/array
     * params.
     *
     * @return This.
     */
    @Override
    public QueryChecker withParam(Object param) {
        return this.withParams(param);
    }

    /**
     * Set client time zone for query.
     *
     * @param zoneId Zone ID.
     * @return This.
     */
    @Override
    public QueryChecker withTimeZoneId(ZoneId zoneId) {
        this.timeZoneId = zoneId;

        return this;
    }

    /**
     * Disables rules.
     *
     * @param rules Rules to disable.
     * @return This.
     */
    @Override
    public QueryChecker disableRules(String... rules) {
        if (rules != null) {
            Arrays.stream(rules).filter(Objects::nonNull).forEach(disabledRules::add);
        }

        return this;
    }

    /**
     * This method add the given row to the list of expected, the order of enumeration does not matter unless {@link #ordered()} is set.
     *
     * @param res Array with values one returning tuple. {@code null} array will be interpreted as single-column-null row.
     * @return This.
     */
    @Override
    public QueryChecker returns(Object... res) {
        assert resultChecker == null || resultChecker instanceof RowByRowResultChecker
                : "Result checker already set to " + resultChecker.getClass().getSimpleName();

        RowByRowResultChecker rowByRowResultChecker = (RowByRowResultChecker) resultChecker;

        if (rowByRowResultChecker == null) {
            rowByRowResultChecker = new RowByRowResultChecker();

            resultChecker = rowByRowResultChecker;
        }

        // let's interpret null array as simple single null.
        if (res == null) {
            res = QueryChecker.NULL_AS_VARARG;
        }

        rowByRowResultChecker.expectedResult.add(Arrays.asList(res));

        return this;
    }

    /**
     * Check that return empty result.
     *
     * @return This.
     */
    @Override
    public QueryChecker returnNothing() {
        assert resultChecker == null : "Result checker already set to " + resultChecker.getClass().getSimpleName();

        resultChecker = new EmptyResultChecker();

        return this;
    }

    @Override
    public QueryChecker returnSomething() {
        assert resultChecker == null : "Result checker already set to " + resultChecker.getClass().getSimpleName();

        resultChecker = new NotEmptyResultChecker();

        return this;
    }

    @Override
    public QueryChecker returnRowCount(int rowCount) {
        assert resultChecker == null : "Result checker already set to " + resultChecker.getClass().getSimpleName();

        resultChecker = new RowCountResultChecker(rowCount);

        return this;
    }

    /**
     * Sets columns names.
     *
     * @return This.
     */
    @Override
    public QueryChecker columnNames(String... columns) {
        assert metadataMatchers == null;

        metadataMatchers = Arrays.stream(columns)
                .map(name -> new MetadataMatcher().name(name))
                .collect(Collectors.toList());

        return this;
    }

    /**
     * Sets columns types.
     *
     * @return This.
     */
    @Override
    public QueryChecker columnTypes(Type... columns) {
        assert metadataMatchers == null;

        metadataMatchers = Arrays.stream(columns)
                .map(t -> (ColumnMatcher) columnMetadata -> {
                    Class<?> type = columnMetadata.type().javaClass();

                    assertThat("Column type don't match", type, equalTo(t));
                })
                .collect(Collectors.toList());

        return this;
    }

    /**
     * Sets columns metadata.
     *
     * @return This.
     */
    @Override
    public QueryChecker columnMetadata(MetadataMatcher... matchers) {
        assert metadataMatchers == null;

        metadataMatchers = Arrays.asList(matchers);

        return this;
    }

    /**
     * Adds plan matchers.
     */
    @Override
    @SafeVarargs
    public final QueryChecker matches(Matcher<String>... planMatcher) {
        Collections.addAll(planMatchers, planMatcher);

        return this;
    }

    /**
     * Run checks.
     */
    @Override
    public void check() {
        // Check plan.
        QueryProcessor qryProc = getEngine();

        SqlProperties properties = SqlPropertiesHelper.newBuilder()
                .set(QueryProperty.ALLOWED_QUERY_TYPES, SqlQueryType.SINGLE_STMT_TYPES)
                .set(QueryProperty.TIME_ZONE_ID, timeZoneId)
                .build();

        String qry = queryTemplate.createQuery();

        LOG.info("Executing query: [nodeName={}, query={}]", nodeName(), qry);

        if (!CollectionUtils.nullOrEmpty(planMatchers)) {
            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> explainCursors = qryProc.queryAsync(
                    properties, transactions(), tx, "EXPLAIN PLAN FOR " + qry, params);
            AsyncSqlCursor<InternalSqlRow> explainCursor = await(explainCursors);
            List<InternalSqlRow> explainRes = getAllFromCursor(explainCursor);

            String actualPlan = (String) explainRes.get(0).get(0);

            if (!CollectionUtils.nullOrEmpty(planMatchers)) {
                for (Matcher<String> matcher : planMatchers) {
                    assertThat("Invalid plan:\n" + actualPlan, actualPlan, matcher);
                }
            }
        }

        // Check column metadata only.
        if (resultChecker == null && metadataMatchers != null) {
            QueryMetadata queryMetadata = await(qryProc.prepareSingleAsync(properties, tx, qry, params));

            assertNotNull(queryMetadata);

            checkColumnsMetadata(queryMetadata.columns());

            return;
        }

        // Check result.
        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursors =
                bypassingThreadAssertionsAsync(() -> qryProc.queryAsync(properties, transactions(), tx, qry, params));

        AsyncSqlCursor<InternalSqlRow> cur = await(cursors);

        checkMetadata(cur.metadata());

        if (metadataMatchers != null) {
            checkColumnsMetadata(cur.metadata().columns());
        }

        List<InternalSqlRow> rows = Commons.cast(getAllFromCursor(cur));
        List<List<Object>> res = convertSqlRows(rows);

        if (resultChecker != null) {
            resultChecker.check(res, ordered);
        }
    }

    private void checkColumnsMetadata(List<ColumnMetadata> columnsMetadata) {
        Iterator<ColumnMetadata> valueIterator = columnsMetadata.iterator();
        Iterator<ColumnMatcher> matcherIterator = metadataMatchers.iterator();

        while (matcherIterator.hasNext() && valueIterator.hasNext()) {
            ColumnMatcher matcher = matcherIterator.next();
            ColumnMetadata actualElement = valueIterator.next();

            matcher.check(actualElement);
        }

        assertEquals(metadataMatchers.size(), columnsMetadata.size(), "Column metadata doesn't match");
    }

    @Override
    public String toString() {
        return QueryCheckerImpl.class.getSimpleName() + "[sql=" + queryTemplate.originalQueryString() + "]";
    }

    protected abstract String nodeName();

    protected abstract QueryProcessor getEngine();

    protected abstract IgniteTransactions transactions();

    protected void checkMetadata(ResultSetMetadata metadata) {
        // No-op.
    }

    /**
     * List comparator.
     */
    private static class ListComparator implements Comparator<List<?>> {
        /** {@inheritDoc} */
        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public int compare(List<?> o1, List<?> o2) {
            if (o1.size() != o2.size()) {
                fail("Collections are not equal:\nExpected:\t" + o1 + "\nActual:\t" + o2);
            }

            Iterator<?> it1 = o1.iterator();
            Iterator<?> it2 = o2.iterator();

            while (it1.hasNext()) {
                Object item1 = it1.next();
                Object item2 = it2.next();

                if (Objects.deepEquals(item1, item2)) {
                    continue;
                }

                if (item1 == null) {
                    return 1;
                }

                if (item2 == null) {
                    return -1;
                }

                if (!(item1 instanceof Comparable) && !(item2 instanceof Comparable)) {
                    continue;
                }

                Comparable c1 = (Comparable) item1;
                Comparable c2 = (Comparable) item2;

                int c = c1.compareTo(c2);

                if (c != 0) {
                    return c;
                }
            }

            return 0;
        }
    }

    /**
     * Updates an SQL query string to include hints for the optimizer to disable certain rules.
     */
    private static final class AddDisabledRulesTemplate implements QueryTemplate {
        private static final Pattern SELECT_REGEXP = Pattern.compile("(?i)^select");
        private static final Pattern SELECT_QRY_CHECK = Pattern.compile("(?i)^select .*");

        private final QueryTemplate input;

        private final List<String> disabledRules;

        private AddDisabledRulesTemplate(QueryTemplate input, List<String> disabledRules) {
            this.input = input;
            this.disabledRules = disabledRules;
        }

        @Override
        public String originalQueryString() {
            return input.originalQueryString();
        }

        @Override
        public String createQuery() {
            String qry = input.createQuery();

            if (!disabledRules.isEmpty()) {
                String originalQuery = input.originalQueryString();

                assert SELECT_QRY_CHECK.matcher(qry).matches() : "SELECT query was expected: " + originalQuery + ". Updated: " + qry;

                return SELECT_REGEXP.matcher(qry).replaceAll("select "
                        + HintUtils.toHint(IgniteHint.DISABLE_RULE, disabledRules.toArray(ArrayUtils.STRING_EMPTY_ARRAY)));
            } else {
                return qry;
            }
        }
    }

    @FunctionalInterface
    interface ResultChecker {
        void check(List<List<Object>> rows, boolean ordered);
    }

    private static class EmptyResultChecker implements ResultChecker {
        @Override
        public void check(List<List<Object>> rows, boolean ordered) {
            assertThat(rows, empty());
        }
    }

    private static class NotEmptyResultChecker implements ResultChecker {
        @Override
        public void check(List<List<Object>> rows, boolean ordered) {
            assertThat(rows, not(empty()));
        }
    }

    private static class RowByRowResultChecker implements ResultChecker {
        private final List<List<Object>> expectedResult = new ArrayList<>();

        @Override
        public void check(List<List<Object>> rows, boolean ordered) {
            if (!ordered) {
                // Avoid arbitrary order.
                rows.sort(new ListComparator());
                expectedResult.sort(new ListComparator());
            }

            QueryChecker.assertEqualsCollections(expectedResult, rows);
        }
    }

    private static class RowCountResultChecker implements ResultChecker {
        private final int expRowCount;

        private RowCountResultChecker(int expRowCount) {
            this.expRowCount = expRowCount;
        }

        @Override
        public void check(List<List<Object>> rows, boolean ordered) {
            assertThat(rows, hasSize(expRowCount));
        }
    }
}
