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

package org.apache.ignite.internal.sql.docs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

/**
 * Checks that all operators defined in a table of SQL operators are documented.
 *
 * <p>When sql operator table changes, this test should detect the following errors:
 * <ul>
 *     <li>An operator was added</li>
 *     <li>An operator was removed</li>
 *     <li>An operator signature was changed. This is achieved by storing a SHA-1 hash of operator signatures./li>
 * </ul>
 *
 * <p>The list of documented operators is stored in {@link OperatorListTest#OPERATOR_LIST} in the following format:
 * <pre>
 *   F1(&lt;numeric&gt;)
 *   F1(&lt;string&gt;)
 *   #81a63101fa89dfb9b9115265fb519d7dda810aa2
 *
 *   F2(&lt;string&gt;) ***
 *   F2() ***
 *   #7f27a84679525580ba4fa549407c28de073c0f69
 * </pre>
 *
 * <p>In this file a list of signatures for each operator is followed by a SHA-1 hash.
 * When a signature ends with {@code ***} it means that it was not automatically generated and was added manually.
 */
public class OperatorListTest extends BaseIgniteAbstractTest {

    private static final String OPERATOR_LIST = "src/test/resources/docs/operator_list.txt";

    private static final Pattern TYPE_NAME_PATTERN = Pattern.compile("<[^>]*>");

    private final IgniteSqlOperatorTable operatorTable = new IgniteSqlOperatorTable();

    private final List<DocumentedOperators> allOperators = List.of(
            stringFunctions(),
            numericFunctions(),
            dateTimeFunctions(),
            aggregateFunctions(),
            otherFunctions(),
            regexFunctions(),
            jsonFunctions(),
            xmlFunctions(),
            structAndCollectionFunctions(),

            // These operators added but should not included in the docs
            setOperators(),
            logicalOperators()
    );

    @Test
    public void test() throws IOException {
        validateOperatorList(operatorTable, allOperators);

        StringWriter sw = new StringWriter();

        try (PrintWriter pw = new PrintWriter(sw)) {
            allOperators.stream().filter(ops -> !ops.internal).forEach(ops -> {
                pw.println("=== " + ops.name);
                pw.println();
                ops.operators.stream().filter(op -> !op.internal).forEach(op -> printSignature(pw, op));
                pw.println();
            });
            pw.flush();

            Path path = Paths.get(OPERATOR_LIST);
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

            assertEquals(
                    String.join("\n", lines).stripTrailing(),
                    sw.toString().stripTrailing(),
                    "operator list does not match"
            );
        }
    }

    @Test
    public void testValidationFunctionOk() {
        SqlOperator add = SqlStdOperatorTable.PLUS;
        SqlOperator mul = SqlStdOperatorTable.MULTIPLY;
        SqlOperator sub = SqlStdOperatorTable.MINUS;

        SqlOperatorTable table = SqlOperatorTables.of(add, sub, mul);

        DocumentedOperators ops = new DocumentedOperators("Test");
        ops.add(add);
        ops.hide(mul);
        ops.add(sub);

        validateOperatorList(table, List.of(ops));
    }

    @Test
    public void testValidationFunctionFailsWhenSomeOpsAreNotIncluded() {
        SqlOperator add = SqlStdOperatorTable.PLUS;
        SqlOperator mul = SqlStdOperatorTable.MULTIPLY;
        SqlOperator sub = SqlStdOperatorTable.MINUS;

        SqlOperatorTable table = SqlOperatorTables.of(add, sub, mul);

        DocumentedOperators ops = new DocumentedOperators("Test");
        ops.add(add);
        ops.hide(mul);

        // validateOperatorList should fail because minus operator is missing from DocumentedOperators.
        try {
            validateOperatorList(table, List.of(ops));
        } catch (AssertionFailedError err) {

            assertThat("Error:\n" + err.getMessage(), err.getMessage(), Matchers.allOf(
                    containsString("- class: " + sub.getClass().getCanonicalName()),
                    not(containsString("+ class: " + add.getClass().getCanonicalName())),
                    not(containsString("* class: " + mul.getClass().getCanonicalName()))
            ));
            return;
        }

        fail();
    }

    private static void printSignature(PrintWriter pw, DocumentedOperator op) {
        Signatures sigs = Signatures.makeSignatures(op);
        for (String sig : sigs.sigs) {
            pw.print(sig);
            if (sigs.manual) {
                pw.print(" ***");
            }
            pw.println();
        }
        pw.println("#" + sigs.hash);
        pw.println();
    }

    private static void validateOperatorList(SqlOperatorTable operatorTable, List<DocumentedOperators> operators) {
        Set<SqlOperator> annotated = operators.stream()
                .flatMap(ops -> ops.operators.stream().map(f -> f.operator)).collect(Collectors.toSet());

        List<SqlOperator> notDocumented = new ArrayList<>();

        for (SqlOperator operator : operatorTable.getOperatorList()) {
            if (!annotated.contains(operator)) {
                notDocumented.add(operator);
            }
        }

        if (!notDocumented.isEmpty()) {
            StringBuilder sb = new StringBuilder(
                    "Signatures of the following operators are missing from " + OPERATOR_LIST + ". "
                            + "Add these operators to as public (add(OperatorTable.MY_OP)) "
                            + "or include them as internal (call .hide(OperatorTable.MY_OP):");
            sb.append(System.lineSeparator());

            notDocumented.forEach(o -> sb.append(describeOperator(o)).append(System.lineSeparator()));

            fail(sb.toString());
        }
    }

    private static String describeOperator(SqlOperator operator) {
        return operator.getName() + " class: " + operator.getClass().getCanonicalName();
    }

    // Operator definitions

    private static DocumentedOperators stringFunctions() {
        DocumentedOperators ops = new DocumentedOperators("String Functions");

        ops.add(SqlStdOperatorTable.UPPER);
        ops.add(SqlStdOperatorTable.LOWER);
        ops.add(SqlStdOperatorTable.INITCAP);
        ops.add(SqlLibraryOperators.TO_BASE64);
        ops.add(SqlLibraryOperators.FROM_BASE64);
        ops.add(SqlLibraryOperators.MD5);
        ops.add(SqlLibraryOperators.SHA1);
        ops.add(SqlStdOperatorTable.SUBSTRING);
        ops.add(SqlLibraryOperators.LEFT);
        ops.add(SqlLibraryOperators.RIGHT);
        ops.add(SqlStdOperatorTable.REPLACE);
        ops.add(SqlLibraryOperators.TRANSLATE3);
        ops.add(SqlLibraryOperators.CHR);
        ops.add(SqlStdOperatorTable.CHAR_LENGTH);
        ops.add(SqlStdOperatorTable.CHARACTER_LENGTH);
        ops.add(SqlStdOperatorTable.CONCAT).sig("(<string>, <string>, ...");
        ops.add(SqlLibraryOperators.CONCAT_FUNCTION);
        ops.add(SqlStdOperatorTable.OVERLAY);
        ops.add(SqlStdOperatorTable.POSITION);
        ops.add(SqlStdOperatorTable.ASCII);
        ops.add(SqlLibraryOperators.REPEAT);
        ops.add(SqlLibraryOperators.SPACE);
        ops.add(SqlLibraryOperators.STRCMP);
        ops.add(SqlLibraryOperators.SOUNDEX);
        ops.add(SqlLibraryOperators.DIFFERENCE);
        ops.add(SqlLibraryOperators.REVERSE);
        ops.add(SqlStdOperatorTable.TRIM);
        ops.add(SqlLibraryOperators.LTRIM);
        ops.add(SqlLibraryOperators.RTRIM);
        ops.add(IgniteSqlOperatorTable.SUBSTR);
        ops.add(IgniteSqlOperatorTable.LENGTH);
        ops.add(IgniteSqlOperatorTable.OCTET_LENGTH);

        // LIKE and SIMILAR.
        ops.add(SqlStdOperatorTable.LIKE);
        ops.hide(SqlStdOperatorTable.NOT_LIKE);
        ops.add(SqlStdOperatorTable.SIMILAR_TO);
        ops.hide(SqlStdOperatorTable.NOT_SIMILAR_TO);

        return ops;
    }

    private static DocumentedOperators regexFunctions() {
        DocumentedOperators ops = new DocumentedOperators("Regular Expression Functions");

        // POSIX REGEX.
        ops.add(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE, "~*");
        ops.add(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE, "~");
        ops.hide(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE);
        ops.hide(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE);

        ops.add(SqlLibraryOperators.REGEXP_REPLACE_2);
        ops.add(SqlLibraryOperators.REGEXP_REPLACE_3);
        ops.add(SqlLibraryOperators.REGEXP_REPLACE_4);
        ops.add(SqlLibraryOperators.REGEXP_REPLACE_5);
        ops.add(SqlLibraryOperators.REGEXP_REPLACE_6);

        return ops;
    }

    private static DocumentedOperators numericFunctions() {
        DocumentedOperators ops = new DocumentedOperators("Numeric Functions");

        // Math functions.
        ops.add(SqlStdOperatorTable.MOD); // Arithmetic remainder.
        ops.add(SqlStdOperatorTable.EXP); // Euler's number e raised to the power of a value.
        ops.add(SqlStdOperatorTable.POWER);
        ops.add(SqlStdOperatorTable.LN); // Natural logarithm.
        ops.add(SqlStdOperatorTable.LOG10); // The base 10 logarithm.
        ops.add(SqlStdOperatorTable.ABS); // Absolute value.
        ops.add(SqlStdOperatorTable.RAND); // Random.
        ops.add(SqlStdOperatorTable.RAND_INTEGER); // Integer random.
        ops.add(SqlStdOperatorTable.ACOS); // Arc cosine.
        ops.add(SqlStdOperatorTable.ASIN); // Arc sine.
        ops.add(SqlStdOperatorTable.ATAN); // Arc tangent.
        ops.add(SqlStdOperatorTable.ATAN2); // Angle from coordinates.
        ops.add(SqlStdOperatorTable.SQRT); // Square root.
        ops.add(SqlStdOperatorTable.CBRT); // Cube root.
        ops.add(SqlStdOperatorTable.COS); // Cosine
        ops.add(SqlLibraryOperators.COSH); // Hyperbolic cosine.
        ops.add(SqlStdOperatorTable.COT); // Cotangent.
        ops.add(SqlStdOperatorTable.DEGREES); // Radians to degrees.
        ops.add(SqlStdOperatorTable.RADIANS); // Degrees to radians.
        ops.add(IgniteSqlOperatorTable.ROUND); // Fixes return type scale.
        ops.add(SqlStdOperatorTable.SIGN);
        ops.add(SqlStdOperatorTable.SIN); // Sine.
        ops.add(SqlLibraryOperators.SINH); // Hyperbolic sine.
        ops.add(SqlStdOperatorTable.TAN); // Tangent.
        ops.add(SqlLibraryOperators.TANH); // Hyperbolic tangent.
        ops.add(IgniteSqlOperatorTable.TRUNCATE); // Fixes return type scale.
        ops.add(SqlStdOperatorTable.PI);

        return ops;
    }

    private static DocumentedOperators dateTimeFunctions() {
        DocumentedOperators ops = new DocumentedOperators("Date/time Functions");

        // Date and time.
        ops.hide(SqlStdOperatorTable.DATETIME_PLUS);
        ops.hide(SqlStdOperatorTable.MINUS_DATE);
        ops.add(SqlStdOperatorTable.EXTRACT);
        ops.add(SqlStdOperatorTable.FLOOR);
        ops.add(SqlStdOperatorTable.CEIL);
        ops.add(SqlStdOperatorTable.TIMESTAMP_ADD);
        ops.add(SqlStdOperatorTable.TIMESTAMP_DIFF);
        ops.add(SqlStdOperatorTable.LAST_DAY);
        ops.add(SqlLibraryOperators.DAYNAME);
        ops.add(SqlLibraryOperators.MONTHNAME);
        ops.add(SqlStdOperatorTable.DAYOFMONTH);
        ops.add(SqlStdOperatorTable.DAYOFWEEK);
        ops.add(SqlStdOperatorTable.DAYOFYEAR);
        ops.add(SqlStdOperatorTable.YEAR);
        ops.add(SqlStdOperatorTable.QUARTER);
        ops.add(SqlStdOperatorTable.MONTH);
        ops.add(SqlStdOperatorTable.WEEK);
        ops.add(SqlStdOperatorTable.HOUR);
        ops.add(SqlStdOperatorTable.MINUTE);
        ops.add(SqlStdOperatorTable.SECOND);
        ops.add(SqlLibraryOperators.TIMESTAMP_SECONDS); // Seconds since 1970-01-01 to timestamp.
        ops.add(SqlLibraryOperators.TIMESTAMP_MILLIS); // Milliseconds since 1970-01-01 to timestamp.
        ops.add(SqlLibraryOperators.TIMESTAMP_MICROS); // Microseconds since 1970-01-01 to timestamp.
        ops.add(SqlLibraryOperators.UNIX_SECONDS); // Timestamp to seconds since 1970-01-01.
        ops.add(SqlLibraryOperators.UNIX_MILLIS); // Timestamp to milliseconds since 1970-01-01.
        ops.add(SqlLibraryOperators.UNIX_MICROS); // Timestamp to microseconds since 1970-01-01.
        ops.add(SqlLibraryOperators.UNIX_DATE); // Date to days since 1970-01-01.
        ops.add(SqlLibraryOperators.DATE_FROM_UNIX_DATE); // Days since 1970-01-01 to date.
        ops.add(SqlLibraryOperators.DATE)
                .sig("(<string>)")
                .sig("(<integer>, <integer>, <integer>)")
                .sig("(<timestamp>)")
                .sig("(<timestamp with local time zone>)")
                .sig("(<timestamp with local time zone>, <string>)");

        // Current time functions.
        ops.add(SqlStdOperatorTable.CURRENT_TIME);
        ops.add(SqlStdOperatorTable.CURRENT_TIMESTAMP);
        ops.add(SqlStdOperatorTable.CURRENT_DATE);
        ops.add(SqlStdOperatorTable.LOCALTIME);
        ops.add(SqlStdOperatorTable.LOCALTIMESTAMP);

        return ops;
    }

    private static DocumentedOperators aggregateFunctions() {
        DocumentedOperators ops = new DocumentedOperators("Aggregate Functions");

        ops.add(SqlStdOperatorTable.COUNT)
                .sig("(*)")
                .sig("(<any>)");

        ops.add(SqlStdOperatorTable.SUM);
        ops.add(SqlStdOperatorTable.SUM0, "SUM0");
        ops.add(SqlStdOperatorTable.AVG);
        // internal function
        ops.hide(IgniteSqlOperatorTable.DECIMAL_DIVIDE);
        ops.add(SqlStdOperatorTable.MIN);
        ops.add(SqlStdOperatorTable.MAX);
        ops.add(SqlStdOperatorTable.ANY_VALUE);
        ops.add(SqlStdOperatorTable.SINGLE_VALUE);
        ops.hide(SqlStdOperatorTable.FILTER);

        ops.add(SqlStdOperatorTable.EVERY);
        ops.add(SqlStdOperatorTable.SOME);

        ops.hide(SqlInternalOperators.LITERAL_AGG);

        return ops;
    }

    private static DocumentedOperators jsonFunctions() {
        DocumentedOperators ops = new DocumentedOperators("JSON Functions");

        // JSON Operators
        ops.add(SqlStdOperatorTable.JSON_TYPE_OPERATOR);
        ops.add(SqlStdOperatorTable.JSON_VALUE_EXPRESSION);
        ops.add(SqlStdOperatorTable.JSON_VALUE);
        ops.add(SqlStdOperatorTable.JSON_QUERY);
        ops.add(SqlLibraryOperators.JSON_TYPE);
        ops.add(SqlStdOperatorTable.JSON_EXISTS);
        ops.add(SqlLibraryOperators.JSON_DEPTH);
        ops.add(SqlLibraryOperators.JSON_KEYS);
        ops.add(SqlLibraryOperators.JSON_PRETTY);
        ops.add(SqlLibraryOperators.JSON_LENGTH);
        ops.add(SqlLibraryOperators.JSON_REMOVE);
        ops.add(SqlLibraryOperators.JSON_STORAGE_SIZE);
        ops.add(SqlStdOperatorTable.JSON_OBJECT)
                .sig("(<character> : <any>, ...)")
                .sig("(KEY <character>, VALUE <any>, ...)");
        ops.add(SqlStdOperatorTable.JSON_ARRAY);
        ops.add(SqlStdOperatorTable.IS_JSON_VALUE);
        ops.add(SqlStdOperatorTable.IS_JSON_OBJECT);
        ops.add(SqlStdOperatorTable.IS_JSON_ARRAY);
        ops.add(SqlStdOperatorTable.IS_JSON_SCALAR);
        ops.hide(SqlStdOperatorTable.IS_NOT_JSON_VALUE);
        ops.hide(SqlStdOperatorTable.IS_NOT_JSON_OBJECT);
        ops.hide(SqlStdOperatorTable.IS_NOT_JSON_ARRAY);
        ops.hide(SqlStdOperatorTable.IS_NOT_JSON_SCALAR);

        return ops;
    }

    private static DocumentedOperators otherFunctions() {
        DocumentedOperators ops = new DocumentedOperators("Other Functions");

        ops.add(IgniteSqlOperatorTable.GREATEST2, "GREATEST");
        ops.add(IgniteSqlOperatorTable.LEAST2, "LEAST");

        // Other functions and operators.
        ops.add(SqlStdOperatorTable.CAST).sig("(<any> AS <type>)");
        ops.add(SqlLibraryOperators.INFIX_CAST, "::").sig("<any>::<type>");
        ops.add(SqlStdOperatorTable.COALESCE);
        ops.add(SqlLibraryOperators.NVL);
        ops.add(SqlStdOperatorTable.NULLIF);
        ops.add(SqlStdOperatorTable.CASE).sig("(...)");
        ops.add(SqlLibraryOperators.DECODE);
        ops.add(SqlLibraryOperators.LEAST);
        ops.add(SqlLibraryOperators.GREATEST);
        ops.add(SqlLibraryOperators.COMPRESS);
        ops.hide(SqlStdOperatorTable.DEFAULT);
        ops.hide(SqlStdOperatorTable.REINTERPRET);

        // Exists.
        ops.hide(SqlStdOperatorTable.EXISTS);

        // Ignite

        ops.add(IgniteSqlOperatorTable.TYPEOF);
        ops.add(IgniteSqlOperatorTable.RAND_UUID);
        ops.add(IgniteSqlOperatorTable.SYSTEM_RANGE);

        return ops;
    }

    private static DocumentedOperators structAndCollectionFunctions() {
        DocumentedOperators ops = new DocumentedOperators("Collection Functions");

        // ROW constructor
        ops.hide(SqlStdOperatorTable.ROW);

        // Collections.
        ops.add(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR);
        ops.add(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR);

        ops.add(SqlStdOperatorTable.ITEM, "")
                .sig("array[<integer>]")
                .sig("map[<character>]")
                .sig("struct[<integer> | <character>]");

        ops.add(SqlStdOperatorTable.CARDINALITY);
        ops.add(SqlStdOperatorTable.IS_EMPTY);
        ops.hide(SqlStdOperatorTable.IS_NOT_EMPTY);

        return ops;
    }

    private static DocumentedOperators xmlFunctions() {
        DocumentedOperators ops = new DocumentedOperators("XML Functions");

        // XML Operators.
        ops.add(SqlLibraryOperators.EXTRACT_VALUE);
        ops.add(SqlLibraryOperators.XML_TRANSFORM);
        ops.add(SqlLibraryOperators.EXTRACT_XML);
        ops.add(SqlLibraryOperators.EXISTS_NODE);

        return ops;
    }

    private static DocumentedOperators setOperators() {
        DocumentedOperators ops = new DocumentedOperators("Set Operators").hide();

        // Set operators.
        ops.add(SqlStdOperatorTable.UNION);
        ops.add(SqlStdOperatorTable.UNION_ALL);
        ops.add(SqlStdOperatorTable.EXCEPT);
        ops.add(SqlStdOperatorTable.EXCEPT_ALL);
        ops.add(SqlStdOperatorTable.INTERSECT);
        ops.add(SqlStdOperatorTable.INTERSECT_ALL);

        return ops;
    }

    private static DocumentedOperators logicalOperators() {
        DocumentedOperators ops = new DocumentedOperators("Logical Operators").hide();

        // Logical.
        ops.add(SqlStdOperatorTable.AND);
        ops.add(SqlStdOperatorTable.OR);
        ops.add(SqlStdOperatorTable.NOT);
        // Comparisons.
        ops.add(SqlStdOperatorTable.LESS_THAN);
        ops.add(SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
        ops.add(SqlStdOperatorTable.GREATER_THAN);
        ops.add(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
        ops.add(SqlStdOperatorTable.EQUALS);
        ops.add(SqlStdOperatorTable.NOT_EQUALS);
        ops.add(SqlStdOperatorTable.BETWEEN);
        ops.add(SqlStdOperatorTable.NOT_BETWEEN);
        // Arithmetic.
        ops.add(SqlStdOperatorTable.PLUS);
        ops.add(SqlStdOperatorTable.MINUS);
        ops.add(SqlStdOperatorTable.MULTIPLY);
        ops.add(SqlStdOperatorTable.DIVIDE);
        ops.hide(SqlStdOperatorTable.DIVIDE_INTEGER); // Used internally.
        ops.add(SqlStdOperatorTable.PERCENT_REMAINDER);
        ops.add(SqlStdOperatorTable.UNARY_MINUS);
        ops.add(SqlStdOperatorTable.UNARY_PLUS);

        // IS ... operator.
        ops.add(SqlStdOperatorTable.IS_NULL);
        ops.add(SqlStdOperatorTable.IS_NOT_NULL);
        ops.add(SqlStdOperatorTable.IS_TRUE);
        ops.add(SqlStdOperatorTable.IS_NOT_TRUE);
        ops.add(SqlStdOperatorTable.IS_FALSE);
        ops.add(SqlStdOperatorTable.IS_NOT_FALSE);
        ops.add(SqlStdOperatorTable.IS_DISTINCT_FROM);
        ops.add(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM);

        // NULLS ordering.
        ops.add(SqlStdOperatorTable.NULLS_FIRST);
        ops.add(SqlStdOperatorTable.NULLS_LAST);
        ops.add(SqlStdOperatorTable.DESC);

        return ops;
    }

    // Helper classes; operators + signatures.

    /** Collection of operators. */
    private static class DocumentedOperators {

        private final String name;

        private final List<DocumentedOperator> operators = new ArrayList<>();

        private boolean internal;

        /** Creates a collection of operators. */
        private DocumentedOperators(String name) {
            this.name = name;
        }

        /** Adds a public operator. */
        DocumentedOperator add(SqlOperator operator) {
            return addOp(operator, operator.getName(), false);
        }

        /** Adds a public operator under the specified name. */
        DocumentedOperator add(SqlOperator operator, String syntaxName) {
            return addOp(operator, syntaxName, false);
        }

        /** Marks this collection of operators as internal. */
        DocumentedOperators hide() {
            internal = true;
            return this;
        }

        /** Adds an internal operator. */
        void hide(SqlOperator operator) {
            addOp(operator, operator.getName(), true);
        }

        private DocumentedOperator addOp(SqlOperator operator, String syntaxName, boolean internal) {
            DocumentedOperator op = new DocumentedOperator(operator, syntaxName, internal);
            operators.add(op);
            return op;
        }
    }

    /** Operator. */
    private static class DocumentedOperator {
        final SqlOperator operator;

        final String syntaxName;

        final boolean internal;

        final List<Signature> sigs = new ArrayList<>();

        DocumentedOperator(SqlOperator operator, String displayName, boolean internal) {
            this.operator = operator;
            this.syntaxName = displayName;
            this.internal = internal;
        }

        /** Adds a signature. */
        DocumentedOperator sig(String sigStr) {
            Signature sig = new Signature(syntaxName, sigStr);
            sigs.add(sig);
            return this;
        }
    }

    private static class Signatures {

        final List<String> sigs;

        final String hash;

        final boolean manual;

        Signatures(List<String> sigs, String hash, boolean manual) {
            this.sigs = sigs;
            this.hash = hash;
            this.manual = manual;
        }

        private static Signatures makeSignatures(DocumentedOperator op) {
            String opHash = computeOperatorHash(op);

            if (!op.sigs.isEmpty()) {
                List<String> sigs = op.sigs.stream()
                        .map(s -> s.fullSig)
                        .collect(Collectors.toList());

                return new Signatures(sigs, opHash, true);
            }

            try {
                String allowedSignatures = op.operator.getAllowedSignatures();
                String[] signatures = allowedSignatures.replace("'", "")
                        .replace(op.operator.getName(), "")
                        .split("\\n");

                List<String> signaturesList = SignatureFormat.formatAll(SignatureFormat.ARGS, op, signatures);
                List<String> sigs;

                if (signaturesList.isEmpty()) {
                    sigs = List.of(op.syntaxName);
                } else if (signaturesList.size() == 1) {
                    sigs = List.of(SignatureFormat.FULL.format(op, signatures[0]));
                } else {
                    sigs = new ArrayList<>(SignatureFormat.formatAll(SignatureFormat.FULL, op, signatures));
                }

                return new Signatures(sigs, opHash, false);
            } catch (Throwable error) {
                String syntaxName = op.syntaxName + " <UNABLE TO PARSE SIGNATURE: " + error.getMessage() + ">";

                return new Signatures(List.of(syntaxName), opHash, false);
            }
        }
    }

    private static class Signature {

        final String fullSig;

        final String argSig;

        Signature(String opName, String sigStr) {
            Matcher matcher = TYPE_NAME_PATTERN.matcher(sigStr);
            List<String> params = new ArrayList<>();

            while (matcher.find()) {
                String type = matcher.group();
                params.add(type);
            }

            this.fullSig = opName + sigStr;
            this.argSig = params.stream().collect(Collectors.joining(", ", "(", ")"));
        }
    }

    private enum SignatureFormat {
        /** Operator name ands arguments. Example: for {@code f(a, b)} full format produces {@code f(a, b)}. */
        FULL,
        /** Only arguments. Example: for {@code f(a, b}} this format produces {@code (a, b)}. */
        ARGS;

        static List<String> formatAll(SignatureFormat format, DocumentedOperator op, String[] sigs) {
            return Arrays.stream(sigs)
                    .map(OperatorListTest::formatTypeNames)
                    .map(s -> format.format(op, s))
                    .distinct()
                    .collect(Collectors.toList());
        }

        String format(DocumentedOperator op, String sig) {
            switch (this) {
                case FULL:
                    return full(op, sig);
                case ARGS:
                    return args(op, sig);
                default:
                    throw new IllegalArgumentException("Unknown format: " + this);
            }
        }

        private static String full(DocumentedOperator op, String in) {
            String s = formatTypeNames(in);
            SqlSyntax syntax = op.operator.getSyntax();

            if (syntax == SqlSyntax.PREFIX) {
                return op.syntaxName + " " + s;
            } else if (syntax == SqlSyntax.POSTFIX) {
                return s + op.syntaxName;
            } else if (syntax == SqlSyntax.BINARY) {
                String[] args = s.split("\\s+");
                return args[0] + " " + op.syntaxName + " " + args[1];
            } else if (syntax == SqlSyntax.FUNCTION_ID) {
                return op.syntaxName;
            } else {
                return op.syntaxName + s;
            }
        }

        private static String args(DocumentedOperator op, String in) {
            String s = formatTypeNames(in);
            SqlSyntax syntax = op.operator.getSyntax();

            if (syntax == SqlSyntax.BINARY) {
                String[] args = s.split("\\s+");
                return args[0] + " " + args[1];
            } else if (syntax == SqlSyntax.FUNCTION_ID) {
                return "";
            } else {
                return s;
            }
        }
    }

    /** Converts {@code (<TYPE1> AND <TYPE2>)} into {@code FUNC (<type1> AND <type2>)}}. */
    private static String formatTypeNames(String input) {
        Matcher matcher = TYPE_NAME_PATTERN.matcher(input);

        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String matchedSubstring = matcher.group();
            String replacement = matchedSubstring.toLowerCase();

            matcher.appendReplacement(result, replacement);
        }

        matcher.appendTail(result);

        return result.toString();
    }

    /** Computes hash of SQL operator. */
    private static String computeOperatorHash(DocumentedOperator op) {
        SqlOperator sqlOp = op.operator;

        StringBuilder sb = new StringBuilder();

        sb.append("name=").append(op.syntaxName)
                .append("opName=").append(sqlOp.getName())
                .append("syntax=").append(sqlOp.getSyntax())
                .append("kind=").append(sqlOp.kind)
                .append("signatures=");

        try {
            String allowedSigs = sqlOp.getAllowedSignatures();
            sb.append(allowedSigs);
        } catch (Throwable t) {
            // Assume that an operator signature changes, if an error message changes.
            sb.append("<ERROR: ").append(t.getMessage()).append('>');
        }

        // We can not use neither operandTypeChecker nor operandTypeInference because those properties
        // can be provided as lambdas that does not have stable textual representation.

        sb.append("operandCountRange=");
        // Assume that changes to operandCountRange are reflected in its properties.  
        try {
            SqlOperandCountRange range = sqlOp.getOperandCountRange();
            sb.append(range.getMin()).append('-').append(range.getMax());
        } catch (UnsupportedOperationException ignore) {
            // Change from no operandCountRange to some operandCountRange should mean signatures might have been modified.
            sb.append("not_implemented");
        }

        // Include negated operator
        SqlOperator negated = sqlOp.not();
        if (negated != null) {
            sb.append("negated=").append(negated.getName());
        }

        // Include reverse operator
        SqlOperator reverse = sqlOp.reverse();
        if (reverse != null) {
            sb.append("reverse=").append(reverse.getName());
        }

        // Other properties

        sb.append("leftPrecedence=").append(sqlOp.getLeftPrec())
                .append("rightPrecedence=").append(sqlOp.getRightPrec())
                .append("symmetrical=").append(sqlOp.isSymmetrical())
                .append("deterministic=").append(sqlOp.isDeterministic())
                .append("aggregator=").append(sqlOp.isAggregator())
                .append("group=").append(sqlOp.isGroup())
                .append("groupAuxiliary=").append(sqlOp.isGroupAuxiliary())
                .append("allowsFraming=").append(sqlOp.allowsFraming())
                .append("requiresOrder=").append(sqlOp.requiresOrder())
                .append("requiresOver=").append(sqlOp.requiresOver());

        return SqlFunctions.sha1(sb.toString());
    }
}
