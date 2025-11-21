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

import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.table.QualifiedName;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.core.SubstringMatcher;
import org.jetbrains.annotations.Nullable;

/** Query checker interface. */
public interface QueryChecker {
    Object[] NULL_AS_VARARG = {null};

    /** Creates a matcher that matches if the examined string contains the specified string anywhere. */
    static Matcher<String> containsUnion() {
        return matchesOnce("UnionAll");
    }

    /**
     * Ignite table scan matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    static Matcher<String> containsTableScan(String schema, String tblName) {
        return matchesOnce("TableScan.*?table: " + QualifiedName.of(schema, tblName).toCanonicalForm());
    }

    /**
     * Matcher which ensures that node matching the pattern has row count matching provided matcher.
     *
     * @param nodePattern A pattern describing a node of interest.
     * @param rowCountMatcher Matcher for the estimated row count.
     * @return Matcher.
     */
    static Matcher<String> nodeRowCount(String nodePattern, Matcher<Integer> rowCountMatcher) {
        Pattern pattern = Pattern.compile(".*" + nodePattern 
                + ".*?est: \\(rows=(?<rowcount>\\d+).*");

        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof String)) {
                    return false;
                }

                String sanitized = ((String) o).replace("\n", "");
                java.util.regex.Matcher matcher = pattern.matcher(sanitized);

                if (!matcher.matches()) {
                    return false;
                }

                String rowCountString = matcher.group("rowcount");

                return rowCountMatcher.matches(new BigDecimal(rowCountString).intValue());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("node that matches pattern \"" + nodePattern + "\" should have \"rowcount\" that ");
                description.appendDescriptionOf(rowCountMatcher);
            }
        };
    }

    /**
     * Ignite index scan matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    static Matcher<String> containsIndexScan(String schema, String tblName) {
        return matchesOnce("IndexScan.*?table: " + QualifiedName.of(schema, tblName).toCanonicalForm()
                + ".*?searchBounds: ");
    }

    /**
     * Ignite index scan matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @return Matcher.
     */
    static Matcher<String> containsIndexScan(String schema, String tblName, String idxName) {
        return matchesOnce("IndexScan.*?table: " + QualifiedName.of(schema, tblName).toCanonicalForm()
                + ".*?index: " + idxName
                + ".*?searchBounds: ");
    }

    /**
     * Ignite index scan matcher which ignores search bounds.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    static Matcher<String> containsIndexScanIgnoreBounds(String schema, String tblName) {
        return matchesOnce("IndexScan.*?table: " + QualifiedName.of(schema, tblName).toCanonicalForm());
    }

    /**
     * Ignite index scan matcher which ignores search bounds.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @return Matcher.
     */
    static Matcher<String> containsIndexScanIgnoreBounds(String schema, String tblName, String idxName) {
        return matchesOnce("IndexScan.*?table: " + QualifiedName.of(schema, tblName).toCanonicalForm()
                + ".*?index: " + idxName);
    }

    /**
     * Ignite table|index scan with only one project matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @param names Columns in projection.
     * @return Matcher.
     */
    static Matcher<String> containsProject(String schema, String tblName, String... names) {
        return matchesOnce("(Table|Index)Scan.*?table: " + QualifiedName.of(schema, tblName).toCanonicalForm()
                + ".*?fieldNames: \\[" + String.join(", ", List.of(names)) + "\\]");
    }

    /**
     * Sub plan matcher.
     *
     * @param subPlan Subplan.
     * @return Matcher.
     */
    static Matcher<String> containsSubPlan(String subPlan) {
        return containsString(subPlan);
    }

    /**
     * Sub string matcher.
     *
     * @param substring Substring.
     * @return Matcher.
     */
    static Matcher<String> matches(String substring) {
        return new SubstringMatcher("contains", false, substring) {
            /** {@inheritDoc} */
            @Override
            protected boolean evalSubstringOf(String strIn) {
                strIn = strIn.replaceAll(System.lineSeparator(), "");

                return strIn.matches(this.substring);
            }
        };
    }

    @SuppressWarnings("unchecked")
    QueryChecker matches(Matcher<String>... planMatcher);

    /**
     * Check collections equals (ordered).
     *
     * @param exp Expected collection.
     * @param act Actual collection.
     */
    static void assertEqualsCollections(Collection<?> exp, Collection<?> act) {
        if (exp.size() != act.size()) {
            String errorMsg = new IgniteStringBuilder("Collections sizes are not equal:").nl()
                    .app("\tExpected: ").app(exp.size()).app(exp).nl()
                    .app("\t  Actual: ").app(act.size()).app(act).toString();

            throw new AssertionError(errorMsg);
        }

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
    static String displayValue(Object value, boolean includeType) {
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

    /** Matches only one occurrence. */
    static Matcher<String> matchesOnce(String pattern) {
        return new SubstringMatcher("contains once", false, pattern) {
            /** {@inheritDoc} */
            @Override
            protected boolean evalSubstringOf(String strIn) {
                strIn = strIn.replaceAll(System.lineSeparator(), "");

                return containsOnce(strIn, this.substring);
            }
        };
    }

    /** Check only single matching. */
    static boolean containsOnce(String s, CharSequence substring) {
        Pattern pattern = Pattern.compile(substring.toString());
        java.util.regex.Matcher matcher = pattern.matcher(s);

        // Find first, but no more.
        return matcher.find() && !matcher.find();
    }

    /**
     * Ignite any index can matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @param idxNames Index names.
     * @return Matcher.
     */
    static Matcher<String> containsAnyScan(String schema, String tblName, String... idxNames) {
        if (nullOrEmpty(idxNames)) {
            return matchesOnce("(Table|Index)Scan.*?table: " + QualifiedName.of(schema, tblName).toCanonicalForm());
        }

        return anyOf(
                Arrays.stream(idxNames).map(idx -> containsIndexScan(schema, tblName, idx)).collect(Collectors.toList())
        );
    }

    QueryChecker ordered();

    QueryChecker withParams(Object... params);

    QueryChecker withParam(@Nullable Object param);

    QueryChecker withTimeZoneId(ZoneId timeZoneId);

    QueryChecker withDefaultSchema(String schema);

    QueryChecker disableRules(String... rules);

    /** Adds validator that ensures the next row in resultset equals to the one represented by array. */
    QueryChecker returns(Object... res);

    /** Adds validator that ensures the next row in resultset satisfies the given matcher. */
    QueryChecker results(Matcher<List<List<?>>> matcher);

    QueryChecker returnNothing();

    QueryChecker returnSomething();

    QueryChecker returnRowCount(int rowCount);

    QueryChecker columnNames(String... columns);

    QueryChecker columnTypes(Type... columns);

    QueryChecker columnMetadata(MetadataMatcher... matchers);

    void check();

    /**
     * Allows to parameterize an SQL query string.
     */
    interface QueryTemplate {
        /** Returns the original query string. **/
        String originalQueryString();

        /**
         * Produces an SQL query from the original query string.
         */
        String createQuery();
    }
}
