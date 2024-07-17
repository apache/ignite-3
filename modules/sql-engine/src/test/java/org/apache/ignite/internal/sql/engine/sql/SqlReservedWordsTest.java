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

package org.apache.ignite.internal.sql.engine.sql;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.TreeSet;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.ignite.internal.generated.query.calcite.sql.IgniteSqlParserImpl;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.junit.jupiter.api.Test;

/**
 * Test reserved keywords.
 */
public class SqlReservedWordsTest extends AbstractParserTest {
    /** List of keywords reserved in Ignite SQL. */
    private static final Set<String> RESERVED_WORDS = Set.of(
            "ABS",
            "ALL", // UNION ALL
            "ALTER",
            "AND",
            "ANY",
            "ARRAY",
            "ARRAY_MAX_CARDINALITY",
            "AS",
            "ASYMMETRIC", // BETWEEN ASYMMETRIC .. AND ..
            "AVG",
            "BETWEEN",
            "BOTH", // TRIM(BOTH .. FROM ..)
            "BY", // GROUP BY
            "CALL",
            "CARDINALITY",
            "CASE",
            "CAST",
            "CEILING",
            "CHAR",
            "CHARACTER",
            "CHARACTER_LENGTH",
            "CHAR_LENGTH",
            "COALESCE",
            "COLLECT",
            "COLUMN",
            "CONSTRAINT",
            "CONVERT",
            "COUNT",
            "COVAR_POP",
            "COVAR_SAMP",
            "CREATE",
            "CROSS", // CROSS JOIN
            "CUBE",
            "CUME_DIST",
            "CURRENT",
            "CURRENT_CATALOG",
            "CURRENT_DATE",
            "CURRENT_DEFAULT_TRANSFORM_GROUP",
            "CURRENT_PATH",
            "CURRENT_ROLE",
            "CURRENT_ROW",
            "CURRENT_SCHEMA",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
            "CURRENT_USER",
            "DATE",
            "DATETIME",
            "DECIMAL",
            "DEFAULT",
            "DELETE",
            "DENSE_RANK",
            "DESCRIBE",
            "DISTINCT",
            "DROP",
            "ELEMENT",
            "ELSE",
            "EVERY",
            "EXCEPT",
            "EXISTS",
            "EXP",
            "EXPLAIN",
            "EXTEND",
            "EXTRACT",
            "FALSE",
            "FETCH",
            "FILTER",
            "FIRST_VALUE",
            "FLOOR",
            "FOR", // SUBSTRING(.. FROM .. FOR ..)
            "FRIDAY",
            "FROM",
            "FULL", // FULL JOIN
            "FUSION",
            "GROUP",
            "GROUPING",
            "HAVING",
            "HOUR",
            "IF",
            "IN",
            "INDEX",
            "INNER",
            "INSERT",
            "INTERSECT",
            "INTERSECTION",
            "INTERVAL",
            "INTO",
            "IS",
            "JOIN",
            "JSON_SCOPE",
            "LAG",
            "LAST_VALUE",
            "LEAD",
            "LEADING", // TRIM(LEADING .. FROM ..)
            "LEFT", // LEFT JOIN
            "LIKE",
            "LIMIT",
            "LN",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "LOWER",
            "MATCH_RECOGNIZE",
            "MAX",
            "MERGE",
            "MIN",
            "MINUS",
            "MINUTE",
            "MOD",
            "MONDAY",
            "MONTH",
            "MULTISET",
            "NATURAL", // NATURAL JOIN
            "NEW",
            "NEXT",
            "NOT",
            "NTH_VALUE",
            "NTILE",
            "NULL",
            "NULLIF",
            "OCTET_LENGTH",
            "OFFSET",
            "ON",
            "OR",
            "ORDER",
            "OUTER", // OUTER JOIN
            "OVER",
            "PARTITION",
            "PERCENTILE_CONT",
            "PERCENTILE_DISC",
            "PERCENT_RANK",
            "PERIOD",
            "PERMUTE",
            "POWER",
            "PRECISION",
            "PRIMARY",
            "QUALIFY",
            "RANK",
            "REGR_COUNT",
            "REGR_SXX",
            "REGR_SYY",
            "RENAME",
            "RESET",
            "RIGHT",
            "ROLLUP",
            "ROW",
            "ROW_NUMBER",
            "SATURDAY",
            "SECOND",
            "SELECT",
            "SESSION_USER",
            "SET",
            "SOME",
            "SPECIFIC",
            "SQRT",
            "STDDEV_POP",
            "STDDEV_SAMP",
            "STREAM",
            "SUBSTRING",
            "SUM",
            "SUNDAY",
            "SYMMETRIC", // BETWEEN SYMMETRIC .. AND ..
            "SYSTEM_TIME",
            "SYSTEM_USER",
            "TABLE",
            "TABLESAMPLE",
            "THEN",
            "THURSDAY",
            "TIME",
            "TIMESTAMP",
            "TO",
            "TRAILING", // TRIM(TRAILING .. FROM ..)
            "TRUE",
            "TRUNCATE",
            "TUESDAY",
            "UESCAPE",
            "UNION",
            "UNKNOWN",
            "UPDATE",
            "UPPER",
            "UPSERT",
            "USER",
            "USING",
            "VALUE",
            "VALUES",
            "VAR_POP",
            "VAR_SAMP",
            "WEDNESDAY",
            "WHEN",
            "WHERE",
            "WINDOW",
            "WITH",
            "WITHIN",
            "YEAR"
    );

    @Test
    public void testReservedWords() {
        SqlAbstractParserImpl.Metadata md = IgniteSqlParserImpl.FACTORY.getParser(null).getMetadata();

        Set<String> unexpectedReserved = new TreeSet<>();
        Set<String> shouldBeReserved = new TreeSet<>();

        for (String s : md.getTokens()) {
            if (md.isReservedWord(s) && !RESERVED_WORDS.contains(s)) {
                unexpectedReserved.add(s);
            } else if (!md.isReservedWord(s) && RESERVED_WORDS.contains(s)) {
                shouldBeReserved.add(s);
            }
        }

        assertTrue(
                unexpectedReserved.isEmpty(),
                "Unexpected reserved keywords: \n" + formatKeywords(unexpectedReserved)
        );

        assertTrue(
                shouldBeReserved.isEmpty(),
                "Keywords should be reserved: \n" + formatKeywords(shouldBeReserved)
        );
    }

    /**
     * Formatted as config.fmpp:nonReservedKeywordsToAdd keywords.
     */
    private static String formatKeywords(Set<String> keywords) {
        IgniteStringBuilder sb = new IgniteStringBuilder();

        keywords.forEach(s -> sb.app("      \"").app(s).app("\"").nl());

        return sb.toString();
    }
}
