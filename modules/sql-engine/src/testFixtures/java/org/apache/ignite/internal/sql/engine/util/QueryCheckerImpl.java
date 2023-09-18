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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Type;
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
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.hint.IgniteHint;
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.hamcrest.Matcher;

/**
 * Query checker base class.
 */
abstract class QueryCheckerImpl implements QueryChecker {

    final QueryTemplate queryTemplate;

    private final ArrayList<Matcher<String>> planMatchers = new ArrayList<>();

    private final ArrayList<String> disabledRules = new ArrayList<>();

    private List<List<?>> expectedResult;

    private List<ColumnMatcher> metadataMatchers;

    private boolean ordered;

    private Object[] params = OBJECT_EMPTY_ARRAY;

    private final Transaction tx;

    /**
     * Constructor.
     *
     * @param tx Transaction.
     * @param queryTemplate A query template.
     */
    QueryCheckerImpl(Transaction tx, QueryTemplate queryTemplate) {
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
        assert expectedResult != QueryChecker.EMPTY_RES
                : "Erroneous awaiting results mixing, impossible to simultaneously wait something and nothing";

        if (expectedResult == null) {
            expectedResult = new ArrayList<>();
        }

        // let's interpret null array as simple single null.
        if (res == null) {
            res = QueryChecker.NULL_AS_VARARG;
        }

        expectedResult.add(Arrays.asList(res));

        return this;
    }

    /**
     * Check that return empty result.
     *
     * @return This.
     */
    @Override
    public QueryChecker returnNothing() {
        assert expectedResult == null : "Erroneous awaiting results mixing, impossible to simultaneously wait nothing and something";

        expectedResult = QueryChecker.EMPTY_RES;

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
                    Class<?> type = ColumnType.columnTypeToClass(columnMetadata.type());

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

        SessionId sessionId = qryProc.createSession(PropertiesHelper.emptyHolder());

        QueryContext context = QueryContext.create(SqlQueryType.ALL, tx);

        String qry = queryTemplate.createQuery();

        try {

            if (!CollectionUtils.nullOrEmpty(planMatchers)) {

                CompletableFuture<AsyncSqlCursor<List<Object>>> explainCursors = qryProc.querySingleAsync(sessionId,
                        context, transactions(), "EXPLAIN PLAN FOR " + qry, params);
                AsyncSqlCursor<List<Object>> explainCursor = await(explainCursors);
                List<List<Object>> explainRes = getAllFromCursor(explainCursor);

                String actualPlan = (String) explainRes.get(0).get(0);

                if (!CollectionUtils.nullOrEmpty(planMatchers)) {
                    for (Matcher<String> matcher : planMatchers) {
                        assertThat("Invalid plan:\n" + actualPlan, actualPlan, matcher);
                    }
                }
            }
            // Check result.
            CompletableFuture<AsyncSqlCursor<List<Object>>> cursors =
                    qryProc.querySingleAsync(sessionId, context, transactions(), qry, params);

            AsyncSqlCursor<List<Object>> cur = await(cursors);

            checkMetadata(cur.metadata());

            if (metadataMatchers != null) {
                List<ColumnMetadata> columnMetadata = cur.metadata().columns();

                Iterator<ColumnMetadata> valueIterator = columnMetadata.iterator();
                Iterator<ColumnMatcher> matcherIterator = metadataMatchers.iterator();

                while (matcherIterator.hasNext() && valueIterator.hasNext()) {
                    ColumnMatcher matcher = matcherIterator.next();
                    ColumnMetadata actualElement = valueIterator.next();

                    matcher.check(actualElement);
                }

                assertEquals(metadataMatchers.size(), columnMetadata.size(), "Column metadata doesn't match");
            }

            var res = getAllFromCursor(cur);

            if (expectedResult != null) {
                if (Objects.equals(expectedResult, QueryChecker.EMPTY_RES)) {
                    assertEquals(0, res.size(), "Empty result expected");

                    return;
                }

                if (!ordered) {
                    // Avoid arbitrary order.
                    res.sort(new ListComparator());
                    expectedResult.sort(new ListComparator());
                }

                QueryChecker.assertEqualsCollections(expectedResult, res);
            }
        } finally {
            await(qryProc.closeSession(sessionId));
        }
    }

    @Override
    public String toString() {
        return QueryCheckerImpl.class.getSimpleName() + "[sql=" + queryTemplate.originalQueryString() + "]";
    }

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
}
