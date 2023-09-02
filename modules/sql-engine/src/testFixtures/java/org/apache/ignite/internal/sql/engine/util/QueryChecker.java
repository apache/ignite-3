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
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;
import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.core.SubstringMatcher;

/**
 * Query checker.
 */
public abstract class QueryChecker {
    private static final Object[] NULL_AS_VARARG = {null};

    private static final List<List<?>> EMPTY_RES = List.of(List.of());

    /**
     * Ignite table scan matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    public static Matcher<String> containsTableScan(String schema, String tblName) {
        return containsSubPlan("IgniteTableScan(table=[[" + schema + ", " + tblName + "]]");
    }

    /**
     * Ignite index scan matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    public static Matcher<String> containsIndexScan(String schema, String tblName) {
        return matchesOnce(".*IgniteIndexScan\\(table=\\[\\[" + schema + ", " + tblName + "\\]\\],"
                + " tableId=\\[.*\\].*\\)");
    }

    /**
     * Ignite index scan matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @return Matcher.
     */
    public static Matcher<String> containsIndexScan(String schema, String tblName, String idxName) {
        return matchesOnce(".*IgniteIndexScan\\(table=\\[\\[" + schema + ", " + tblName + "\\]\\],"
                + " tableId=\\[.*\\], index=\\[" + idxName + "\\].*\\)");
    }

    /**
     * Ignite table|index scan with projects unmatcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    public static Matcher<String> notContainsProject(String schema, String tblName) {
        return CoreMatchers.not(containsSubPlan("Scan(table=[[" + schema + ", "
                + tblName + "]], " + "requiredColumns="));
    }

    /**
     * {@link #containsProject(String, String, int...)} reverter.
     */
    public static Matcher<String> notContainsProject(String schema, String tblName, int... requiredColumns) {
        return CoreMatchers.not(containsProject(schema, tblName, requiredColumns));
    }

    /**
     * Ignite table|index scan with projects matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @param requiredColumns columns in projection.
     * @return Matcher.
     */
    public static Matcher<String> containsProject(String schema, String tblName, int... requiredColumns) {
        return matches(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", "
                + tblName + "\\]\\], " + ".*requiredColumns=\\[\\{"
                + Arrays.toString(requiredColumns)
                .replaceAll("\\[", "")
                .replaceAll("]", "") + "\\}\\].*");
    }

    /**
     * Ignite table|index scan with only one project matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @param requiredColumns columns in projection.
     * @return Matcher.
     */
    public static Matcher<String> containsOneProject(String schema, String tblName, int... requiredColumns) {
        return matchesOnce(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", "
                + tblName + "\\]\\], " + ".*requiredColumns=\\[\\{"
                + Arrays.toString(requiredColumns)
                .replaceAll("\\[", "")
                .replaceAll("]", "") + "\\}\\].*");
    }

    /**
     * Ignite table|index scan with any project matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    public static Matcher<String> containsAnyProject(String schema, String tblName) {
        return matchesOnce(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", "
                + tblName + "\\]\\],.*requiredColumns=\\[\\{(\\d|\\W|,)+\\}\\].*");
    }

    /**
     * Sub plan matcher.
     *
     * @param subPlan Subplan.
     * @return Matcher.
     */
    public static Matcher<String> containsSubPlan(String subPlan) {
        return CoreMatchers.containsString(subPlan);
    }

    /**
     * Sub string matcher.
     *
     * @param substring Substring.
     * @return Matcher.
     */
    public static Matcher<String> matches(final String substring) {
        return new SubstringMatcher("contains", false, substring) {
            /** {@inheritDoc} */
            @Override
            protected boolean evalSubstringOf(String strIn) {
                strIn = strIn.replaceAll(System.lineSeparator(), "");

                return strIn.matches(substring);
            }
        };
    }

    /**
     * Adds plan matchers.
     */
    @SafeVarargs
    public final QueryChecker matches(Matcher<String>... planMatcher) {
        Collections.addAll(planMatchers, planMatcher);

        return this;
    }

    /** Matches only one occurrence. */
    public static Matcher<String> matchesOnce(final String substring) {
        return new SubstringMatcher("contains once", false, substring) {
            /** {@inheritDoc} */
            @Override
            protected boolean evalSubstringOf(String strIn) {
                strIn = strIn.replaceAll(System.lineSeparator(), "");

                return containsOnce(strIn, substring);
            }
        };
    }

    /** Check only single matching. */
    public static boolean containsOnce(final String s, final CharSequence substring) {
        Pattern pattern = Pattern.compile(substring.toString());
        java.util.regex.Matcher matcher = pattern.matcher(s);

        if (matcher.find()) {
            return !matcher.find();
        }

        return false;
    }

    /**
     * Ignite any index can matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @param idxNames Index names.
     * @return Matcher.
     */
    public static Matcher<String> containsAnyScan(final String schema, final String tblName, String... idxNames) {
        if (nullOrEmpty(idxNames)) {
            return matchesOnce(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", " + tblName + "\\]\\].*");
        }

        return CoreMatchers.anyOf(
                Arrays.stream(idxNames).map(idx -> containsIndexScan(schema, tblName, idx)).collect(Collectors.toList())
        );
    }

    /**
     * Allows to parameterize an SQL query string.
     */
    public interface QueryTemplate {

        /** Template that always returns original query. **/
        static QueryTemplate returnOriginalQuery(String query) {
            return new QueryTemplate() {
                @Override
                public String originalQueryString() {
                    return query;
                }

                @Override
                public String createQuery() {
                    return query;
                }
            };
        }

        /** Returns the original query string. **/
        String originalQueryString();

        /**
         * Produces an SQL query from the original query string.
         */
        String createQuery();
    }

    /** Holder for the current {@link QueryChecker}. */
    private static class QueryCheckerHolder {
        private AtomicReference<QueryChecker> reference = new AtomicReference<>();

        void set(QueryChecker newChecker) {
            reference.getAndUpdate(current -> {
                if (current != null) {
                    throw new IllegalStateException("Found previous QueryChecker: query=" + current.queryTemplate.originalQueryString());
                }

                return newChecker;
            });
        }

        void remove(QueryChecker queryChecker) {
            reference.getAndUpdate(current -> {
                if (queryChecker != current) {
                    throw new IllegalStateException(
                            format("Unexpected QueryChecker instance: expected={}, actual={}",
                                    current == null ? "null" : current.queryTemplate.originalQueryString(),
                                    queryChecker.queryTemplate.originalQueryString()
                            ));
                }

                return null;
            });
        }
    }

    /** Validates no {@link #check()} method call was missed for a {@link QueryChecker}. */
    public static void ensureNoUnusedChecker() {
        // Set to null allowing next tests to pass.
        QueryChecker queryChecker = CURRENT_CHECKER_HOLDER.reference.getAndSet(null);

        assertNull(queryChecker,
                () -> "Found unused QueryChecker: query=" + queryChecker.queryTemplate.originalQueryString());
    }

    private static final QueryCheckerHolder CURRENT_CHECKER_HOLDER = new QueryCheckerHolder();

    private final QueryTemplate queryTemplate;

    private final ArrayList<Matcher<String>> planMatchers = new ArrayList<>();

    private final ArrayList<String> disabledRules = new ArrayList<>();

    private List<List<?>> expectedResult;

    private List<ColumnMatcher> metadataMatchers;

    private boolean ordered;

    private Object[] params = OBJECT_EMPTY_ARRAY;

    private Transaction tx;

    /**
     * Constructor.
     *
     * @param tx Transaction.
     * @param qry Query.
     */
    public QueryChecker(Transaction tx, String qry) {
        this(tx, QueryTemplate.returnOriginalQuery(qry));
    }

    /**
     * Constructor.
     *
     * @param tx Transaction.
     * @param queryTemplate A query template.
     */
    public QueryChecker(Transaction tx, QueryTemplate queryTemplate) {
        this.tx = tx;
        this.queryTemplate = new AddDisabledRulesTemplate(Objects.requireNonNull(queryTemplate), disabledRules);

        //noinspection ThisEscapedInObjectConstruction
        CURRENT_CHECKER_HOLDER.set(this);
    }

    /**
     * Sets ordered.
     *
     * @return This.
     */
    public QueryChecker ordered() {
        ordered = true;

        return this;
    }

    /**
     * Sets params.
     *
     * @return This.
     */
    public QueryChecker withParams(Object... params) {
        // let's interpret null array as simple single null.
        if (params == null) {
            params = NULL_AS_VARARG;
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
    public QueryChecker withParam(Object param) {
        return this.withParams(param);
    }

    /**
     * Disables rules.
     *
     * @param rules Rules to disable.
     * @return This.
     */
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
    public QueryChecker returns(Object... res) {
        assert expectedResult != EMPTY_RES : "Erroneous awaiting results mixing, impossible to simultaneously wait something and nothing";

        if (expectedResult == null) {
            expectedResult = new ArrayList<>();
        }

        // let's interpret null array as simple single null.
        if (res == null) {
            res = NULL_AS_VARARG;
        }

        expectedResult.add(Arrays.asList(res));

        return this;
    }

    /**
     * Check that return empty result.
     *
     * @return This.
     */
    public QueryChecker returnNothing() {
        assert expectedResult == null : "Erroneous awaiting results mixing, impossible to simultaneously wait nothing and something";

        expectedResult = EMPTY_RES;

        return this;
    }

    /** Creates a matcher that matches if the examined string contains the specified string anywhere. */
    public static Matcher<String> containsUnion(boolean all) {
        return CoreMatchers.containsString("IgniteUnionAll(all=[" + all + "])");
    }

    /** Creates a matcher that matches if the examined string contains the specified string anywhere. */
    public static Matcher<String> containsUnion() {
        return CoreMatchers.containsString("IgniteUnionAll(all=");
    }

    /**
     * Sets columns names.
     *
     * @return This.
     */
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
    public QueryChecker columnMetadata(MetadataMatcher... matchers) {
        assert metadataMatchers == null;

        metadataMatchers = Arrays.asList(matchers);

        return this;
    }

    /**
     * Run checks.
     */
    public void check() {
        CURRENT_CHECKER_HOLDER.remove(this);

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

            checkMetadata(cur);

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
                if (Objects.equals(expectedResult, EMPTY_RES)) {
                    assertEquals(0, res.size(), "Empty result expected");

                    return;
                }

                if (!ordered) {
                    // Avoid arbitrary order.
                    res.sort(new ListComparator());
                    expectedResult.sort(new ListComparator());
                }

                assertEqualsCollections(expectedResult, res);
            }
        } finally {
            await(qryProc.closeSession(sessionId));
        }
    }

    protected abstract QueryProcessor getEngine();

    protected abstract IgniteTransactions transactions();

    protected void checkMetadata(AsyncSqlCursor<?> cursor) {

    }

    /**
     * Check collections equals (ordered).
     *
     * @param exp Expected collection.
     * @param act Actual collection.
     */
    private static void assertEqualsCollections(Collection<?> exp, Collection<?> act) {
        assertEquals(exp.size(), act.size(), "Collections sizes are not equal:\nExpected: " + exp + "\nActual:   " + act);

        Iterator<?> it1 = exp.iterator();
        Iterator<?> it2 = act.iterator();

        int idx = 0;

        while (it1.hasNext()) {
            Object expItem = NativeTypeWrapper.unwrap(it1.next());
            Object actualItem = NativeTypeWrapper.unwrap(it2.next());

            if (expItem instanceof Collection && actualItem instanceof Collection) {
                assertEqualsCollections((Collection<?>) expItem, (Collection<?>) actualItem);
            } else if (!Objects.deepEquals(expItem, actualItem)) {
                String expectedStr = displayValue(expItem, true);
                String actualStr = displayValue(actualItem, true);

                fail("Collections are not equal (position " + idx + "):\nExpected: " + expectedStr + "\nActual:   " + actualStr);
            }

            idx++;
        }
    }

    /**
     * Converts the given value to a test-output friendly representation that includes type information.
     */
    private static String displayValue(Object value, boolean includeType) {
        if (value == null) {
            return "<null>";
        } else if (value.getClass().isArray()) {
            Class<?> componentType = value.getClass().getComponentType();
            StringBuilder sb = new StringBuilder();

            // Always include array element type in the output.
            sb.append(componentType);
            sb.append("[");

            // Include element type for Object[] arrays.
            boolean includeElementType = componentType == Object.class;

            int length = Array.getLength(value);
            for (int i = 0; i < length; i++) {
                Object item = Array.get(value, i);
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(displayValue(item, includeElementType));
            }

            sb.append("]");
            return sb.toString();
        } else {
            if (includeType) {
                return value + " <" + value.getClass() + ">";
            } else {
                return value.toString();
            }
        }
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
