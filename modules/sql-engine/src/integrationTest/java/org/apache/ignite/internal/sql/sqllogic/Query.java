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

package org.apache.ignite.internal.sql.sqllogic;

import com.google.common.base.Strings;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.StringUtils;

/**
 * Executes an SQL query and expects the specified result and fails when that result and an output of a query do not match.
 * <pre>
 *     query column_type1[column_type2,.. column_typeN] [sort_type] [equality_label]
 * </pre>
 * The expected results can be in two forms:
 * <ul>
 *     <li>a list of rows where items are separated by {@code TAB} character (#)</li>
 *     <li>a number of rows and a hash of those results.</li>
 * </ul>
 *
 * <p>
 * # CAVEAT: beware that certain IDE settings if configured may cause translation of {@code TAB} characters
 * into multiple {code SPACE} characters.
 * </p>
 * <pre>
 *     # Match the results by their values
 *     query I
 *     SELECT c1 FROM (VALUES(1), (2)) t(c1)
 *     ----
 *     1
 *     2
 * </pre>
 * <pre>
 *     # Match the result by hash
 *     query I
 *     SELECT c1 FROM (VALUES(1), (2)) t(c1)
 *     ----
 *     2 values hashing to 6DDB4095EB719E2A9F0A3F95677D24E0
 * </pre>
 * <b>Expecting results of multiple queries to be equal</b>
 *
 * <p>
 * It is possible to expect the results of multiple to be same using a combination of {@code label} and {@code hash}.
 * </p>
 * <pre>
 *     query I nosort select-1
 *     SELECT 1
 *     ----
 *     1 values hashing to B026324C6904B2A9CB4B88D6D61C81D1
 *
 *     # the query below is expected to have the same result as
 *     # a query labeled select-1 and if it is not the case
 *     # a report error is going to display that fact.
 *     query I nosort select-1
 *     SELECT 1 + 0
 *     ----
 *     1 values hashing to B026324C6904B2A9CB4B88D6D61C81D1
 * </pre>
 */
final class Query extends Command {

    /** Hashing label pattern. */
    private static final Pattern HASHING_PTRN = Pattern.compile("([0-9]+) values hashing to ([0-9a-fA-F]+)");

    /** Comparator for "valuesort" sort mode. */
    private static final Comparator<Object> ITM_COMPARATOR = (r1, r2) -> {
        String s1 = String.valueOf(r1);
        String s2 = String.valueOf(r2);

        return s1.compareTo(s2);
    };

    /** An SQL query string. **/
    private final String sql;

    /** Comparator for "rowsort" sort mode. */
    private final Comparator<List<?>> rowComparator;

    /** A list of column types expected in a result. **/
    private final List<ColumnType> resTypes = new ArrayList<>();

    /** Present if query results are presented in a form of rows. **/
    private final List<List<String>> expectedRes;

    /** Present if query results are presented in a form of a hash string. **/
    private final String expectedHash;

    /** Present if query results are presented in a form of a hash string. Otherwise -1. **/
    private final int expectedRows;

    /** Sorting algo. */
    private SortType sortType = SortType.NOSORT;

    /** Equality label. */
    private String eqLabel;

    Query(Script script, ScriptContext ctx, String[] cmd) throws IOException {
        super(script.scriptPosition());

        String resTypesChars = cmd[1];

        if (cmd.length > 2) {
            sortType = SortType.valueOf(cmd[2].toUpperCase());
        }

        if (cmd.length > 3) {
            eqLabel = cmd[3].toLowerCase();
        }

        for (int i = 0; i < resTypesChars.length(); i++) {
            switch (resTypesChars.charAt(i)) {
                case 'I':
                    resTypes.add(ColumnType.I);

                    break;

                case 'R':
                    resTypes.add(ColumnType.R);

                    break;

                case 'T':
                    resTypes.add(ColumnType.T);

                    break;

                default:
                    throw script.reportInvalidCommand("Unknown type character '" + resTypesChars.charAt(i) + "'", cmd);
            }
        }

        if (CollectionUtils.nullOrEmpty(resTypes)) {
            throw script.reportInvalidCommand("Missing type string", cmd);
        }

        var sqlString = new StringBuilder();

        // Read SQL query
        while (script.ready()) {
            String line = script.nextLine();

            // Check for typos. Otherwise we are going to get very confusing errors down the road.
            if (line.startsWith("--")) {
                if (line.equals("----")) {
                    break;
                } else {
                    throw script.reportError("Invalid query terminator sequence", line);
                }
            }

            if (sqlString.length() > 0) {
                sqlString.append("\n");
            }

            sqlString.append(line);
        }

        this.sql = sqlString.toString();

        // Read expected results
        String s = script.nextLineWithoutTrim();
        Matcher m = Strings.isNullOrEmpty(s) ? null : HASHING_PTRN.matcher(s);

        if (m != null && m.matches()) {
            // Expected results are hashing
            expectedRows = Integer.parseInt(m.group(1));
            expectedHash = m.group(2);
            expectedRes = null;
        } else {
            // Read expected results tuples.
            expectedRes = new ArrayList<>();
            expectedRows = -1;
            expectedHash = null;

            boolean singleValOnLine = false;

            List<String> row = new ArrayList<>();

            while (!Strings.isNullOrEmpty(s)) {
                String[] vals = s.split("\\t");

                if (!singleValOnLine && vals.length == 1 && vals.length != resTypes.size()) {
                    singleValOnLine = true;
                }

                if (vals.length != resTypes.size() && !singleValOnLine) {
                    var message = " [row=\"" + s + "\", types=" + resTypes + ']';
                    throw script.reportError("Invalid columns count at the result", message);
                }

                try {
                    if (singleValOnLine) {
                        String colValRaw = ctx.nullLbl.equals(vals[0]) ? null : vals[0];
                        String colVal = ctx.replaceVars(colValRaw);
                        row.add(colVal);

                        if (row.size() == resTypes.size()) {
                            expectedRes.add(row);

                            row = new ArrayList<>();
                        }
                    } else {
                        for (String val : vals) {
                            String colValRaw = ctx.nullLbl.equals(val) ? null : val;
                            String colVal = ctx.replaceVars(colValRaw);
                            row.add(colVal);
                        }

                        expectedRes.add(row);
                        row = new ArrayList<>();
                    }
                } catch (Exception e) {
                    var message = "[row=\"" + s + "\", types=" + resTypes + ']';
                    throw script.reportError("Cannot parse expected results", message, e);
                }

                s = script.nextLineWithoutTrim();
            }
        }

        this.rowComparator = (r1, r2) -> {
            int rows = r1.size();

            for (int i = 0; i < rows; ++i) {
                String s1 = resultToString(ctx, r1.get(i));
                String s2 = resultToString(ctx, r2.get(i));

                int comp = s1.compareTo(s2);

                if (comp != 0) {
                    return comp;
                }
            }

            return 0;
        };
    }

    @Override
    void execute(ScriptContext ctx) {
        List<List<?>> res = ctx.executeQuery(sql);

        checkResult(ctx, res);
    }

    private void checkResult(ScriptContext ctx, List<List<?>> res) {
        if (sortType == SortType.ROWSORT) {
            res.sort(rowComparator);

            if (expectedRes != null) {
                expectedRes.sort(rowComparator);
            }
        } else if (sortType == SortType.VALUESORT) {
            List<Object> flattenRes = new ArrayList<>();

            res.forEach(flattenRes::addAll);

            flattenRes.sort(ITM_COMPARATOR);

            List<List<?>> resSizeAware = new ArrayList<>();
            int rowLen = resTypes.size();
            List<Object> rowRes = new ArrayList<>(rowLen);

            for (Object item : flattenRes) {
                rowRes.add(item);

                if (--rowLen == 0) {
                    resSizeAware.add(rowRes);
                    rowRes = new ArrayList<>(rowLen);
                    rowLen = resTypes.size();
                }
            }

            res = resSizeAware;
        }

        if (expectedHash != null) {
            checkResultsHashed(ctx, res);
        } else {
            checkResultTuples(ctx, res);
        }
    }

    void checkResultTuples(ScriptContext ctx, List<List<?>> res) {
        if (expectedRes.size() != res.size()) {
            throw new AssertionError("Invalid results rows count at: " + posDesc
                    + ". [expectedRows=" + expectedRes.size() + ", actualRows=" + res.size()
                    + ", expected=" + expectedRes + ", actual=" + res + ']');
        }

        for (int i = 0; i < expectedRes.size(); ++i) {
            List<String> expectedRow = expectedRes.get(i);
            List<?> row = res.get(i);

            if (row.size() != expectedRow.size()) {
                throw new AssertionError("Invalid columns count at: " + posDesc
                        + ". [expected=" + expectedRes + ", actual=" + res + ']');
            }

            for (int j = 0; j < expectedRow.size(); ++j) {
                String expectStrValRaw = expectedRow.get(j);
                String expectStrVal = ctx.replaceVars(expectStrValRaw);

                try {
                    checkEquals(ctx,
                            "Not expected result at: " + posDesc
                                    + ". [row=" + i + ", col=" + j
                                    + ", expected=" + expectStrVal + ", actual=" + resultToString(ctx, row.get(j)) + ']',
                            expectStrVal,
                            row.get(j)
                    );
                } catch (AssertionError ex) {
                    AssertionError extended = new AssertionError("Invalid results: " + res);
                    ex.addSuppressed(extended);
                    throw ex;
                }
            }
        }
    }

    private static void checkEquals(ScriptContext ctx, String msg, String expectedStr, Object actual) {
        if (actual == null && (expectedStr == null || ctx.nullLbl.equalsIgnoreCase(expectedStr))) {
            return;
        }

        if (actual != null ^ expectedStr != null) {
            throw new AssertionError(msg);
        }

        // Alternative values for boolean "0" and "1".
        if (actual instanceof Boolean && expectedStr.equals((Boolean) actual ? "1" : "0")) {
            return;
        }

        if (actual instanceof Number) {
            if ("NaN".equals(expectedStr) || expectedStr.endsWith("Infinity")) {
                if (!expectedStr.equals(String.valueOf(actual))) {
                    throw new AssertionError(msg);
                }
            } else {
                BigDecimal actDec = new BigDecimal(String.valueOf(actual));
                BigDecimal expDec = new BigDecimal(expectedStr);

                if (actDec.compareTo(expDec) != 0) {
                    throw new AssertionError(msg);
                }
            }
        } else if (actual instanceof Map) {
            String actualStr = mapToString(ctx, (Map<? extends Comparable, ?>) actual);

            if (!expectedStr.equals(actualStr)) {
                throw new AssertionError(msg);
            }
        } else {
            if (!String.valueOf(expectedStr).equals(resultToString(ctx, actual))
                    && !("(empty)".equals(expectedStr) && resultToString(ctx, actual).isEmpty())) {
                throw new AssertionError(msg);
            }
        }
    }

    private void checkResultsHashed(ScriptContext ctx, List<List<?>> res) {
        Objects.requireNonNull(res, "empty result set");

        ctx.messageDigest.reset();

        for (List<?> row : res) {
            for (Object col : row) {
                ctx.messageDigest.update(resultToString(ctx, col).getBytes(StandardCharsets.UTF_8));
                ctx.messageDigest.update(Script.LINE_SEPARATOR_BYTES);
            }
        }

        String res0 = StringUtils.toHexString(ctx.messageDigest.digest());

        if (eqLabel != null) {
            if (res0.equals(expectedHash)) {
                ctx.eqResStorage.computeIfAbsent(eqLabel, k -> new ArrayList<>()).add(sql.toString());
            } else {
                Collection<String> eq = ctx.eqResStorage.get(eqLabel);

                if (eq != null) {
                    throw new AssertionError("Unexpected hash result, error at: " + posDesc
                            + ". Results of queries need to be equal: " + eq + "\n and \n" + sql);
                }
            }
        }

        if (!res0.equalsIgnoreCase(expectedHash)) {
            throw new AssertionError("Unexpected hash result, error at: " + posDesc
                    + ", expected=" + expectedHash + ", calculated=" + res0
                    + ", expectedRows=" + expectedRows + ", returnedRows=" + res.size() * res.get(0).size());
        }
    }

    @Override
    public String toString() {
        return "Query ["
                + "sql=" + sql
                + ", resTypes=" + resTypes
                + ", expectedRes=" + expectedRes
                + ", expectedHash='" + expectedHash + '\''
                + ", expectedRows=" + expectedRows
                + ", sortType=" + sortType
                + ", eqLabel='" + eqLabel + '\''
                + ']';
    }

    private enum ColumnType {
        I,
        T,
        R
    }

    private enum SortType {
        ROWSORT,
        VALUESORT,
        NOSORT
    }

    private static String resultToString(ScriptContext ctx, Object res) {
        if (res == null) {
            return ctx.nullLbl;
        } else if (res instanceof byte[]) {
            return ByteString.toString((byte[]) res, 16);
        } else if (res instanceof Map) {
            return mapToString(ctx, (Map<?, ?>) res);
        } else if (res instanceof LocalTime) {
            return ((LocalTime) res).format(DateTimeFormatter.ISO_TIME);
        } else if (res instanceof LocalDateTime) {
            return ((LocalDateTime) res).format(SqlTestUtils.SQL_CONFORMANT_DATETIME_FORMATTER);
        } else {
            return String.valueOf(res);
        }
    }

    private static String mapToString(ScriptContext ctx, Map<?, ?> map) {
        if (map == null) {
            return ctx.nullLbl;
        }

        List<String> entries = (new TreeMap<>(map)).entrySet().stream()
                .map(e -> resultToString(ctx, e.getKey()) + ":" + resultToString(ctx, e.getValue()))
                .collect(Collectors.toList());

        return "{" + String.join(", ", entries) + "}";
    }
}
