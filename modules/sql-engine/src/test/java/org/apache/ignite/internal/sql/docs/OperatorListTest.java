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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Checks that all operators defined in a table of SQL operators have matching signatures.
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
 * Each internal SQL operator (an operator that can not be created from public API) as a {@code [Internal]}
 * on a line prior to their definition.
 */
public class OperatorListTest extends BaseIgniteAbstractTest {

    private static final String OPERATOR_LIST = "src/test/resources/docs/operator_list.txt";

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
        DocumentedOperators.validateOperatorList(operatorTable, allOperators, OPERATOR_LIST);

        StringWriter sw = new StringWriter();

        try (PrintWriter pw = new PrintWriter(sw)) {
            DocumentedOperators.printOperators(pw, allOperators);

            Path path = Paths.get(OPERATOR_LIST);
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

            assertEquals(
                    String.join("\n", lines).stripTrailing(),
                    sw.toString().stripTrailing(),
                    "operator list does not match"
            );
        }
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
        ops.internal(SqlStdOperatorTable.NOT_LIKE);
        ops.add(SqlStdOperatorTable.SIMILAR_TO);
        ops.internal(SqlStdOperatorTable.NOT_SIMILAR_TO);

        return ops;
    }

    private static DocumentedOperators regexFunctions() {
        DocumentedOperators ops = new DocumentedOperators("Regular Expression Functions");

        // POSIX REGEX.
        ops.add(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE, "~*");
        ops.add(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE, "~");
        ops.internal(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE);
        ops.internal(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE);

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
        ops.internal(SqlStdOperatorTable.DATETIME_PLUS);
        ops.internal(SqlStdOperatorTable.MINUS_DATE);
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
        ops.add(IgniteSqlOperatorTable.CURRENT_TIMESTAMP);
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
        ops.internal(IgniteSqlOperatorTable.DECIMAL_DIVIDE);
        ops.add(SqlStdOperatorTable.MIN);
        ops.add(SqlStdOperatorTable.MAX);
        ops.add(SqlStdOperatorTable.ANY_VALUE);
        ops.add(SqlStdOperatorTable.SINGLE_VALUE);
        ops.internal(IgniteSqlOperatorTable.SAME_VALUE);
        ops.internal(SqlStdOperatorTable.FILTER);

        ops.add(SqlStdOperatorTable.EVERY);
        ops.add(SqlStdOperatorTable.SOME);

        ops.add(SqlStdOperatorTable.GROUPING);
        ops.internal(SqlInternalOperators.LITERAL_AGG);

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
        ops.internal(SqlStdOperatorTable.IS_NOT_JSON_VALUE);
        ops.internal(SqlStdOperatorTable.IS_NOT_JSON_OBJECT);
        ops.internal(SqlStdOperatorTable.IS_NOT_JSON_ARRAY);
        ops.internal(SqlStdOperatorTable.IS_NOT_JSON_SCALAR);

        return ops;
    }

    private static DocumentedOperators otherFunctions() {
        DocumentedOperators ops = new DocumentedOperators("Other Functions");

        ops.add(IgniteSqlOperatorTable.GREATEST2, "GREATEST");
        ops.add(IgniteSqlOperatorTable.LEAST2, "LEAST");

        ops.add(IgniteSqlOperatorTable.FIND_PREFIX, "$FIND_PREFIX");
        ops.add(IgniteSqlOperatorTable.NEXT_GREATER_PREFIX, "$NEXT_GREATER_PREFIX");

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
        ops.internal(SqlStdOperatorTable.DEFAULT);
        ops.internal(SqlStdOperatorTable.REINTERPRET).sig("(<t1>)").sig("(<t1, t2>)");

        // Exists.
        ops.internal(SqlStdOperatorTable.EXISTS);

        // NULLS ordering.
        ops.internal(SqlStdOperatorTable.NULLS_FIRST);
        ops.internal(SqlStdOperatorTable.NULLS_LAST);
        ops.internal(SqlStdOperatorTable.DESC);

        // Context variable functions.
        ops.add(SqlStdOperatorTable.CURRENT_USER);

        // Ignite
        ops.add(IgniteSqlOperatorTable.TYPEOF);
        ops.add(IgniteSqlOperatorTable.RAND_UUID);
        ops.add(IgniteSqlOperatorTable.SYSTEM_RANGE);

        return ops;
    }

    private static DocumentedOperators structAndCollectionFunctions() {
        DocumentedOperators ops = new DocumentedOperators("Collection Functions");

        // ROW constructor
        ops.add(SqlStdOperatorTable.ROW);

        // Collections.
        ops.add(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR);
        ops.add(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR);

        ops.add(SqlStdOperatorTable.ITEM, "")
                .sig("array[<integer>]")
                .sig("map[<character>]")
                .sig("struct[<integer> | <character>]");

        ops.add(SqlStdOperatorTable.CARDINALITY);
        ops.add(SqlStdOperatorTable.IS_EMPTY);
        ops.internal(SqlStdOperatorTable.IS_NOT_EMPTY);

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
        DocumentedOperators ops = new DocumentedOperators("Set Operators").exclude();

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
        DocumentedOperators ops = new DocumentedOperators("Logical Operators");

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
        ops.add(SqlStdOperatorTable.BETWEEN, "").sig("BETWEEN <comparable_type> AND <comparable_type>");
        ops.internal(SqlStdOperatorTable.NOT_BETWEEN, "").sig("NOT BETWEEN <comparable_type> AND <comparable_type>");
        // Arithmetic.
        ops.add(SqlStdOperatorTable.PLUS);
        ops.add(SqlStdOperatorTable.MINUS);
        ops.add(SqlStdOperatorTable.MULTIPLY);
        ops.add(SqlStdOperatorTable.DIVIDE);
        ops.internal(SqlStdOperatorTable.DIVIDE_INTEGER); // Used internally.
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

        return ops;
    }
}
