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
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.core.SubstringMatcher;

/** Query checker interface. */
public interface QueryChecker {
    Object[] NULL_AS_VARARG = {null};

    /** Creates a matcher that matches if the examined string contains the specified string anywhere. */
    static Matcher<String> containsUnion(boolean all) {
        return CoreMatchers.containsString("UnionAll(all=[" + all + "])");
    }

    /** Creates a matcher that matches if the examined string contains the specified string anywhere. */
    static Matcher<String> containsUnion() {
        return CoreMatchers.containsString("UnionAll(all=");
    }

    /**
     * Ignite table scan matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    static Matcher<String> containsTableScan(String schema, String tblName) {
        return containsSubPlan("TableScan(table=[[" + schema + ", " + tblName + "]]");
    }

    /**
     * Ignite index scan matcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    static Matcher<String> containsIndexScan(String schema, String tblName) {
        return matchesOnce(".*IndexScan\\(table=\\[\\[" + schema + ", " + tblName + "\\]\\],"
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
    static Matcher<String> containsIndexScan(String schema, String tblName, String idxName) {
        return matchesOnce(".*IndexScan\\(table=\\[\\[" + schema + ", " + tblName + "\\]\\],"
                + " tableId=\\[.*\\], index=\\[" + idxName + "\\].*\\)");
    }

    /**
     * Ignite table|index scan with projects unmatcher.
     *
     * @param schema Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    static Matcher<String> notContainsProject(String schema, String tblName) {
        return CoreMatchers.not(containsSubPlan("Scan(table=[[" + schema + ", "
                + tblName + "]], " + "requiredColumns="));
    }

    /**
     * {@link #containsProject(String, String, int...)} reverter.
     */
    static Matcher<String> notContainsProject(String schema, String tblName, int... requiredColumns) {
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
    static Matcher<String> containsProject(String schema, String tblName, int... requiredColumns) {
        return matches(".*(Table|Index)Scan\\(table=\\[\\[" + schema + ", "
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
    static Matcher<String> containsOneProject(String schema, String tblName, int... requiredColumns) {
        return matchesOnce(".*(Table|Index)Scan\\(table=\\[\\[" + schema + ", "
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
    static Matcher<String> containsAnyProject(String schema, String tblName) {
        return matchesOnce(".*(Table|Index)Scan\\(table=\\[\\[" + schema + ", "
                + tblName + "\\]\\],.*requiredColumns=\\[\\{(\\d|\\W|,)+\\}\\].*");
    }

    /**
     * Sub plan matcher.
     *
     * @param subPlan Subplan.
     * @return Matcher.
     */
    static Matcher<String> containsSubPlan(String subPlan) {
        return CoreMatchers.containsString(subPlan);
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
    static Matcher<String> matchesOnce(String substring) {
        return new SubstringMatcher("contains once", false, substring) {
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
            return matchesOnce(".*(Table|Index)Scan\\(table=\\[\\[" + schema + ", " + tblName + "\\]\\].*");
        }

        return CoreMatchers.anyOf(
                Arrays.stream(idxNames).map(idx -> containsIndexScan(schema, tblName, idx)).collect(Collectors.toList())
        );
    }

    QueryChecker ordered();

    QueryChecker withParams(Object... params);

    QueryChecker withParam(Object param);

    QueryChecker withTimeZoneId(ZoneId timeZoneId);

    QueryChecker disableRules(String... rules);

    QueryChecker returns(Object... res);

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
