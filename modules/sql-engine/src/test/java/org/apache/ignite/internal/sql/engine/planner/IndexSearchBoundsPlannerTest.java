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

package org.apache.ignite.internal.sql.engine.planner;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Index bounds check tests.
 */
public class IndexSearchBoundsPlannerTest extends AbstractPlannerTest {
    private static final IgniteLogger LOG = Loggers.forClass(IndexSearchBoundsPlannerTest.class);

    private static List<String> NODES = new ArrayList<>(4);

    private IgniteSchema publicSchema;

    @BeforeAll
    public static void init() {
        IntStream.rangeClosed(0, 3).forEach(i -> NODES.add(UUID.randomUUID().toString()));
    }

    @BeforeEach
    public void beforeEach() {
        publicSchema = createSchemaFrom(tableA("TEST"));
    }

    /** Simple case on one field, without multi tuple SEARCH/SARG. */
    @Test
    public void testBoundsOneFieldSingleTuple() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 = 1", exact(1));

        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C3 = 1", exact(1), empty(), empty());

        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C1 <= 3",
                range(1, 3, false, true));

        assertBounds("SELECT * FROM TEST WHERE C1 < 3 AND C1 IS NOT NULL",
                range(null, 3, true, false));

        // Redundant "IS NOT NULL condition".
        assertBounds("SELECT * FROM TEST WHERE C1 > 3 AND C1 IS NOT NULL",
                range(3, "null", false, false));

        // C4 field not in collation.
        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C1 <= 3 AND C4 = 1",
                range(1, 3, false, true),
                empty(),
                empty()
        );

        // Cannot proceed to C3 without C2.
        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C3 = 1",
                exact(1),
                empty(),
                empty()
        );

        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C1 <= 3 AND C3 = 1",
                range(1, 3, false, true),
                empty(),
                empty()
        );
    }

    /** Simple SEARCH/SARG. */
    @Test
    public void testBoundsOneFieldSearch() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3)",
                multi(exact(1), exact(2), exact(3)));

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) AND C1 IS NOT NULL",
                multi(exact(1), exact(2), exact(3)));

        assertBounds("SELECT * FROM TEST WHERE (C1 > 1 AND C1 < 3) OR C1 IN (4, 5) OR C1 = 6 OR C1 > 7",
                multi(
                        range(1, 3, false, false),
                        exact(4),
                        exact(5),
                        exact(6),
                        range(7, "null", false, false)));
    }

    /** Simple SEARCH/SARG, values deduplication. */
    @Test
    public void testBoundsOneFieldSearchDeduplication() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3, 2, 1)",
                multi(exact(1), exact(2), exact(3)));
    }

    /** Simple SEARCH/SARG, range optimization. */
    @Test
    public void testBoundsOneFieldSearchRangeOptimization() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE (C1 > 1 AND C1 < 4) OR (C1 > 3 AND C1 < 5) OR (C1 > 7) OR (C1 > 6)",
                multi(
                        range(1, 5, false, false),
                        range(6, "null", false, false)));

        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C1 < 3 AND C1 <> 2",
                multi(
                        range(1, 2, false, false),
                        range(2, 3, false, false)));

        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND "
                        + "((C2 > '1' AND C2 < '3') OR (C2 > '11' AND C2 < '33') OR C2 > '4')",
                exact(1),
                multi(
                        range("1", "33", false, false),
                        range("4", "null", false, false)));

        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND (C2 > '1' OR C2 < '3')",
                exact(1));
    }

    /** Simple SEARCH/SARG with "IS NULL" condition. */
    @Test
    public void testBoundsOneFieldSearchWithNull() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) OR C1 IS NULL",
                multi(exact("null"), exact(1), exact(2), exact(3)),
                empty(),
                empty()
        );
    }

    /** Tests bounds with DESC ordering. */
    @Test
    public void testBoundsDescOrdering() throws Exception {
        publicSchema = createSchemaFrom(tableA("TEST")
                .andThen(t -> t.sortedIndex()
                        .name("C4")
                        .addColumn("C4", Collation.DESC_NULLS_LAST)
                        .addColumn("C3", Collation.ASC_NULLS_FIRST)
                        .end())
        );

        assertBounds("SELECT * FROM TEST WHERE C4 > 1",
                range(null, 1, true, false));

        assertBounds("SELECT * FROM TEST WHERE C4 < 1",
                range(1, "null", false, false));

        assertBounds("SELECT * FROM TEST WHERE C4 IS NULL", exact("null"));

        assertBounds("SELECT /*+ FORCE_INDEX(c4) */ * FROM TEST WHERE C4 IS NOT NULL",
                range(null, "null", true, false));

        assertBounds("SELECT * FROM TEST WHERE C4 IN (1, 2, 3) AND C3 > 1",
                multi(exact(1), exact(2), exact(3)),
                range(1, null, false, true)
        );

        assertBounds("SELECT * FROM TEST WHERE ((C4 > 1 AND C4 < 5) OR (C4 > 7 AND C4 < 9)) AND C3 = 1",
                multi(
                        range(5, 1, false, false),
                        range(9, 7, false, false))
        );
    }

    /** Tests bounds with conditions on several fields. */
    @Test
    public void testBoundsSeveralFieldsSearch() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C2 IN ('a', 'b')",
                exact(1),
                multi(exact("a"), exact("b"))
        );

        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C2 > 'a'",
                exact(1),
                range("a", "null", false, false)
        );

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) AND C2 = 'a'",
                multi(exact(1), exact(2), exact(3)),
                exact("a")
        );

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) AND C2 IN ('a', 'b')",
                multi(exact(1), exact(2), exact(3)),
                multi(exact("a"), exact("b"))
        );

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) AND C2 IN ('a', 'b') AND C3 IN (4, 5) AND C4 = 1",
                multi(exact(1), exact(2), exact(3)),
                multi(exact("a"), exact("b")),
                multi(exact(4), exact(5))
        );

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) AND C2 IN ('a', 'b') AND C3 > 4",
                multi(exact(1), exact(2), exact(3)),
                multi(exact("a"), exact("b")),
                range(4, "null", false, false)
        );

        // Cannot proceed to the next field after the range condition.
        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C2 = 'a'",
                range(1, "null", false, false),
                empty(),
                empty()
        );

        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C2 > 'a'",
                range(1, "null", false, false),
                empty(),
                empty()
        );

        // TODO https://issues.apache.org/jira/browse/IGNITE-13568 Fix to exact("a")
        assertBounds("SELECT * FROM TEST WHERE C1 >= 1 AND C2 = 'a'",
                range(1, "null", true, false),
                empty()
        );

        // TODO https://issues.apache.org/jira/browse/IGNITE-13568 Fix to range("a", null, false, true)
        assertBounds("SELECT * FROM TEST WHERE C1 >= 1 AND C2 > 'a'",
                range(1, "null", true, false),
                empty()
        );

        assertBounds("SELECT * FROM TEST WHERE C1 >= 1 AND C2 < 'a'",
                range(1, "null", true, false),
                empty()
        );

        assertBounds("SELECT * FROM TEST WHERE C1 >= 1 AND C2 IN ('a', 'b')",
                range(1, "null", true, false),
                empty()
        );

        // Cannot proceed to the next field after SEARCH/SARG with range condition.
        assertBounds("SELECT * FROM TEST WHERE ((C1 > 1 AND C1 < 3) OR C1 > 5) AND C2 = 'a'",
                multi(
                        range(1, 3, false, false),
                        range(5, "null", false, false)),
                empty()
        );
    }

    /** Tests max complexity of SEARCH/SARG to include into index scan. */
    @Test
    public void testBoundsMaxComplexity() throws Exception {
        int limit = RexUtils.MAX_SEARCH_BOUNDS_COMPLEXITY;

        String inVals = String.join(", ", IntStream.range(0, limit + 1).mapToObj(Integer::toString)
                .toArray(String[]::new));

        assertPlan("SELECT * FROM TEST WHERE C1 IN (" + inVals + ")", publicSchema, isTableScan("TEST"));

        inVals = String.join(", ", IntStream.range(0, limit / 10 + 1).mapToObj(Integer::toString)
                .toArray(String[]::new));

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3, 4, 5) AND C2 IN ('a', 'b') AND C3 IN (" + inVals + ")",
                multi(exact(1), exact(2), exact(3), exact(4), exact(5)),
                multi(exact("a"), exact("b")),
                empty()
        );
    }

    /** Tests bounds with wrong literal types. */
    @Test
    public void testBoundsTypeConversion() throws Exception {
        // Implicit cast of all filter values to INTEGER.
        /* assertBounds("SELECT * FROM TEST WHERE C1 IN ('1', '2', '3')",
                multi(exact(1), exact(2), exact(3))
        );

        // Implicit cast of '1' to INTEGER.
        assertBounds("SELECT * FROM TEST WHERE C1 IN ('1', 2, 3)",
                multi(exact(1), exact(2), exact(3))
        );*/

        // Casted to INTEGER type C2 column cannot be used as index bound.
        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C2 > '1'",
                exact(1),
                range('1', "null", false, false)
        );

        // Casted to INTEGER type C2 column cannot be used as index bound.
        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C2 IN (2, 3)",
                exact(1),
                empty()
        );

        // Casted to INTEGER type C2 column cannot be used as index bound.
        assertBounds("SELECT * FROM TEST WHERE CAST(CAST(C1 AS VARCHAR) AS INTEGER) = 1 AND C2 IN (2, 3)",
                exact(1),
                empty()
        );

        // Implicit cast of 2 to VARCHAR.
        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C2 IN (2, '3')",
                exact(1),
                multi(exact("2"), exact("3"))
        );
    }

    /** Tests bounds with dynamic parameters. */
    @Test
    public void testBoundsDynamicParams() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 IN (?, ?)",
                multi(exact("?0"), exact("?1")));

        assertBounds("SELECT * FROM TEST WHERE C1 = ? AND C2 IN ('a', 'b')", List.of(1), publicSchema,
                exact("?0"),
                multi(exact("a"), exact("b"))
        );

        assertBounds("SELECT * FROM TEST WHERE C1 = ? AND C2 > ? AND C2 < ?", List.of(1, 'a', 'w'), publicSchema,
                exact("?0"),
                range("?1", "?2", false, false)
        );
    }

    /** Tests bounds with correlated value. */
    @Test
    public void testBoundsWithCorrelate() throws Exception {
        assertBounds("SELECT (SELECT C1 FROM TEST t2 WHERE t2.C1 = t1.C1) FROM TEST t1",
                exact("$cor0.C1")
        );

        assertBounds(
                "SELECT (SELECT C1 FROM TEST t2 WHERE C1 = 1 AND C2 = 'a' AND C3 IN (t1.C3, 0, 1, 2)) FROM TEST t1",
                exact(1),
                exact("a"),
                empty()
        );
    }

    /** Tests bounds merge. */
    @Test
    public void testBoundsMerge() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(tableA("TEST")
                .andThen(t -> t.sortedIndex()
                        .name("C4")
                        .addColumn("C4", Collation.DESC_NULLS_LAST)
                        .addColumn("C3", Collation.ASC_NULLS_FIRST)
                        .end()
                ));

        assertBounds("SELECT * FROM TEST WHERE C1 > ? AND C1 >= 1", List.of(10), publicSchema,
                range("$GREATEST2(?0, 1)", "null", true, false)
        );

        assertBounds("SELECT * FROM TEST WHERE C1 > ? AND C1 >= ? AND C1 > ?", List.of(10, 10, 10), publicSchema,
                range("$GREATEST2($GREATEST2(?0, ?1), ?2)", "null", true, false)
        );

        assertBounds("SELECT * FROM TEST WHERE C1 > ? AND C1 >= 1 AND C1 < ? AND C1 < ?", List.of(10, 10, 10), publicSchema,
                range("$GREATEST2(?0, 1)", "$LEAST2(?1, ?2)", true, false)
        );

        assertBounds("SELECT * FROM TEST WHERE C1 < ? AND C1 BETWEEN 1 AND 10 ", List.of(10), publicSchema,
                range(1, "$LEAST2(?0, 10)", true, true)
        );

        assertBounds("SELECT * FROM TEST WHERE C1 NOT IN (1, 2) AND C1 >= ?", List.of(10), publicSchema,
                range("?0", "null", true, false)
        );

        assertBounds("SELECT * FROM TEST WHERE C4 > ? AND C4 >= 1 AND C4 < ? AND C4 < ?", List.of(10, 10, 10), publicSchema,
                range("$LEAST2(?1, ?2)", "$GREATEST2(?0, 1)", false, true)
        );
    }

    /** Tests complex bounds expressions. */
    @Test
    public void testBoundsComplex() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 = ? + 10", List.of(1), publicSchema,
                exact("+(?0, 10)")
        );

        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C2 > SUBSTRING(?::VARCHAR, 1, 2) || '3'", List.of("1"), publicSchema,
                exact(1),
                range("||(SUBSTRING(?0, 1, 2), _UTF-8'3')", "null", false, false)
        );

        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C2 > SUBSTRING(C3::VARCHAR, 1, 2) || '3'",
                exact(1),
                empty()
        );

        assertBounds("SELECT (SELECT C1 FROM TEST t2 WHERE t2.C1 = t1.C1 + t1.C3 * ?) FROM TEST t1", List.of(1), publicSchema,
                exact("+($cor0.C1, *($cor0.C3, ?0))")
        );

        assertPlan("SELECT * FROM TEST WHERE C1 = ? + C3", publicSchema, isTableScan("TEST"), List.of(1));

        assertPlan("SELECT (SELECT C1 FROM TEST t2 WHERE t2.C1 < t1.C1 + t2.C1) FROM TEST t1", publicSchema,
                nodeOrAnyChild(isIndexScan("TEST", "C1C2C3")).negate());

        // Here we have two OR sets in CNF, second set can't be used, since it contains condition on C1 and C2 columns,
        // so use only first OR set as bounds.
        assertBounds("SELECT * FROM TEST WHERE C1 in (?, 1, 2) or (C1 = ? and C2 > 'asd')",
                multi(exact("?0"), exact(1), exact(2), exact("?1"))
        );

        assertBounds("SELECT * FROM TEST WHERE C1 in (?, ? + 1, ? * 2)",
                multi(exact("?0"), exact("+(?1, 1)"), exact("*(?2, 2)"))
        );

        // Don't support expanding OR with correlate to bounds.
        assertPlan("SELECT (SELECT C1 FROM TEST t2 WHERE C1 in (t1.C1, 1, ?)) FROM TEST t1", publicSchema,
                nodeOrAnyChild(isIndexScan("TEST", "C1C2C3")).negate());

        // Here "BETWEEN" generates AND condition, and we have two OR sets in CNF, so we can't correctly use range
        // with both upper and lower bounds. So, we use only first OR set as bounds.
        assertBounds("SELECT * FROM TEST WHERE C1 in (?, 1, 2) or C1 between ? and ?",
                multi(exact("?0"), exact(1), exact(2), range("?1", "null", true, false))
        );

        // Check equality condition priority over SEARCH/SARG.
        assertBounds("SELECT * FROM TEST WHERE (C1 BETWEEN 1 AND 10 OR C1 IN (20, 30)) AND C1 = ?",
                exact("?0")
        );
    }

    /**
     * Index bound checks - search key lies out of value range.
     */
    @ParameterizedTest
    @MethodSource("boundsTypeLimits")
    public void testBoundsTypeLimits(RelDataType type, Object value, Predicate<SearchBounds> bounds) throws Exception {
        IgniteSchema schema = createSchemaFrom(
                tableB("TEST2", "C2", type).andThen(addSortIndex("C2")));

        assertBounds("SELECT * FROM test2 WHERE C2 = " + value, List.of(), schema, bounds);
    }

    @ParameterizedTest
    @MethodSource("indexTypeAndNumericsInBounds")
    public void testCorrectNumericIndexInBounds(
            Type indexType,
            RelDataType columnType,
            String valueExpr,
            String boundExpr,
            RelDataType boundExprType) throws Exception {

        UnaryOperator<TableBuilder> tableB = tableB("TEST2", "C2", columnType);

        IgniteSchema schema;
        if (indexType == Type.HASH) {
            schema = createSchemaFrom(tableB.andThen(addHashIndex("C2")));
        } else {
            schema = createSchemaFrom(tableB.andThen(addSortIndex("C2")));
        }

        assertBounds("SELECT * FROM test2 WHERE C2 = " + valueExpr, List.of(), schema, searchBounds -> {
            if (!(searchBounds instanceof ExactBounds)) {
                log.info("SearchBound type does not match. Expected {} but got {}", ExactBounds.class, searchBounds);
                return false;
            }

            ExactBounds exactBounds = (ExactBounds) searchBounds;
            RexNode rexNode = exactBounds.bound();

            if (!SqlTypeUtil.equalSansNullability(boundExprType, rexNode.getType())) {
                log.info("Bound type does not match. Expected {} but got {}", boundExprType, rexNode.getType());
                return false;
            }

            if (!boundExpr.equals(rexNode.toString())) {
                log.info("Bound expr does not match. Expected {} but got {}", boundExpr, rexNode);
                return false;
            }

            return true;
        });
    }

    private static Stream<Arguments> indexTypeAndNumericsInBounds() {
        Stream<Arguments> result = Stream.of();

        for (Type type : Type.values()) {
            Stream<Arguments> idx = numericsInBounds().map(a -> {
                Object[] v = a.get();

                Object[] newArgs = new Object[v.length + 1];
                newArgs[0] = type;
                System.arraycopy(v, 0, newArgs, 1, v.length);

                return arguments(newArgs);
            });
            result = Stream.concat(result, idx);
        }

        return result;
    }

    private static Stream<Arguments> numericsInBounds() {
        return Stream.of(
            // Column type, expr to use in search condition, expected expression in search bounds as RexNode::toString, its type.

            arguments(sqlType(SqlTypeName.TINYINT), "42", "42:TINYINT", sqlType(SqlTypeName.TINYINT)),
            arguments(sqlType(SqlTypeName.TINYINT), "CAST(42 AS TINYINT)", "42:TINYINT", sqlType(SqlTypeName.TINYINT)),
            arguments(sqlType(SqlTypeName.TINYINT), "CAST(42 AS SMALLINT)", "42:TINYINT", sqlType(SqlTypeName.TINYINT)),
            arguments(sqlType(SqlTypeName.TINYINT), "CAST(42 AS INTEGER)", "42:TINYINT", sqlType(SqlTypeName.TINYINT)),
            arguments(sqlType(SqlTypeName.TINYINT), "CAST(42 AS BIGINT)", "42:TINYINT", sqlType(SqlTypeName.TINYINT)),

            arguments(sqlType(SqlTypeName.SMALLINT), "42", "42:SMALLINT", sqlType(SqlTypeName.SMALLINT)),
            arguments(sqlType(SqlTypeName.SMALLINT), "CAST(42 AS TINYINT)", "42:SMALLINT", sqlType(SqlTypeName.SMALLINT)),
            arguments(sqlType(SqlTypeName.SMALLINT), "CAST(42 AS SMALLINT)", "42:SMALLINT", sqlType(SqlTypeName.SMALLINT)),
            arguments(sqlType(SqlTypeName.SMALLINT), "CAST(42 AS INTEGER)", "42:SMALLINT", sqlType(SqlTypeName.SMALLINT)),
            arguments(sqlType(SqlTypeName.SMALLINT), "CAST(42 AS BIGINT)", "42:SMALLINT", sqlType(SqlTypeName.SMALLINT)),

            arguments(sqlType(SqlTypeName.INTEGER), "42", "42", sqlType(SqlTypeName.INTEGER)),
            arguments(sqlType(SqlTypeName.INTEGER), "CAST(42 AS TINYINT)", "42", sqlType(SqlTypeName.INTEGER)),
            arguments(sqlType(SqlTypeName.INTEGER), "CAST(42 AS SMALLINT)", "42", sqlType(SqlTypeName.INTEGER)),
            arguments(sqlType(SqlTypeName.INTEGER), "CAST(42 AS INTEGER)", "42", sqlType(SqlTypeName.INTEGER)),
            arguments(sqlType(SqlTypeName.INTEGER), "CAST(42 AS BIGINT)", "42", sqlType(SqlTypeName.INTEGER)),

            arguments(sqlType(SqlTypeName.BIGINT), "42", "42:BIGINT", sqlType(SqlTypeName.BIGINT)),
            arguments(sqlType(SqlTypeName.BIGINT), "CAST(42 AS TINYINT)", "42:BIGINT", sqlType(SqlTypeName.BIGINT)),
            arguments(sqlType(SqlTypeName.BIGINT), "CAST(42 AS SMALLINT)", "42:BIGINT", sqlType(SqlTypeName.BIGINT)),
            arguments(sqlType(SqlTypeName.BIGINT), "CAST(42 AS INTEGER)", "42:BIGINT", sqlType(SqlTypeName.BIGINT)),
            arguments(sqlType(SqlTypeName.BIGINT), "CAST(42 AS BIGINT)", "42:BIGINT", sqlType(SqlTypeName.BIGINT)),

            arguments(sqlType(SqlTypeName.REAL), "42", "42:REAL", sqlType(SqlTypeName.REAL)),
            arguments(sqlType(SqlTypeName.DOUBLE), "42", "42:DOUBLE", sqlType(SqlTypeName.DOUBLE))

        // TODO https://issues.apache.org/jira/browse/IGNITE-19881 uncomment after this issue is fixed
        //  The optimizer selects TableScan instead of a IndexScan (Real/double columns)
        // arguments(sqlType(SqlTypeName.REAL), "CAST(42 AS DOUBLE)", "42:REAL", sqlType(SqlTypeName.REAL)),
        // arguments(sqlType(SqlTypeName.DOUBLE), "CAST(42 AS REAL)", "42:DOUBLE", sqlType(SqlTypeName.DOUBLE)),
        // TODO https://issues.apache.org/jira/browse/IGNITE-19882 uncomment after this issue is fixed
        //  The optimizer selects TableScan instead of a IndexScan (Decimal columns)
        // arguments(sqlType(SqlTypeName.DECIMAL, 5), "42", "42:DECIMAL(10, 0)", sqlType(SqlTypeName.DECIMAL, 5, 0)),
        // arguments(sqlType(SqlTypeName.DECIMAL, 10, 2), "42", "42:DECIMAL(10, 2)", sqlType(SqlTypeName.DECIMAL, 10, 2)),
        // arguments(sqlType(SqlTypeName.INTEGER), "CAST(42 AS DECIMAL(10))", "42", sqlType(SqlTypeName.INTEGER)),
        );
    }

    private static Stream<Arguments> boundsTypeLimits() {
        RelDataType tinyintType = sqlType(SqlTypeName.TINYINT);
        byte[] tinyIntTypeLimits = {Byte.MIN_VALUE, Byte.MAX_VALUE};
        List<Arguments> tinyInts = List.of(
                arguments(tinyintType, -129, exact(tinyIntTypeLimits[0])),
                arguments(tinyintType, -128, exact(tinyIntTypeLimits[0])),
                arguments(tinyintType, 127, exact(tinyIntTypeLimits[1])),
                arguments(tinyintType, 128, exact(tinyIntTypeLimits[1]))
        );

        RelDataType smallIntType = sqlType(SqlTypeName.SMALLINT);
        short[] smallIntLimits = {Short.MIN_VALUE, Short.MAX_VALUE};
        List<Arguments> smallInts = List.of(
                arguments(smallIntType, (-(int) Math.pow(2, 15) - 1), exact(smallIntLimits[0])),
                arguments(smallIntType, (-(int) Math.pow(2, 15)), exact(smallIntLimits[0])),
                arguments(smallIntType, ((int) Math.pow(2, 15)), exact(smallIntLimits[1])),
                arguments(smallIntType, ((int) Math.pow(2, 15) + 1), exact(smallIntLimits[1]))
        );

        RelDataType intType = sqlType(SqlTypeName.INTEGER);
        int[] intLimits = {Integer.MIN_VALUE, Integer.MAX_VALUE};
        List<Arguments> ints = List.of(
                arguments(intType, (-(long) Math.pow(2, 31) - 1), exact(intLimits[0])),
                arguments(intType, (-(long) Math.pow(2, 31)), exact(intLimits[0])),
                arguments(intType, ((long) Math.pow(2, 31)), exact(intLimits[1])),
                arguments(intType, ((long) Math.pow(2, 31) + 1), exact(intLimits[1]))
        );

        RelDataType bigIntType = sqlType(SqlTypeName.BIGINT);
        BigDecimal[] bigIntTypeLimits = {BigDecimal.valueOf(Long.MIN_VALUE), BigDecimal.valueOf(Long.MAX_VALUE)};
        List<Arguments> bigints = List.of(
                arguments(bigIntType, BigInteger.TWO.pow(63).negate(), exact(bigIntTypeLimits[0]))
        );

        RelDataType decimal3Type = sqlType(SqlTypeName.DECIMAL, 3);
        BigDecimal[] decimal3TypeLimits = {BigDecimal.valueOf(-999), BigDecimal.valueOf(999)};
        List<Arguments> decimal3s = List.of(
                arguments(decimal3Type, "(-1000)::DECIMAL(3)", exact(decimal3TypeLimits[0])),
                arguments(decimal3Type, "(-999)::DECIMAL(3)", exact(decimal3TypeLimits[0])),
                arguments(decimal3Type, "999::DECIMAL(3)", exact(decimal3TypeLimits[1])),
                arguments(decimal3Type, "1000::DECIMAL(3)", exact(decimal3TypeLimits[1]))
        );

        RelDataType decimal53Type = sqlType(SqlTypeName.DECIMAL, 5, 3);
        BigDecimal[] decimal53TypeLimits = {new BigDecimal("-99.999"), new BigDecimal("99.999")};

        List<Arguments> decimal35s = List.of(
                arguments(decimal53Type, "(-100.000)::DECIMAL(5, 3)", exact(decimal53TypeLimits[0])),
                arguments(decimal53Type, "(100.000)::DECIMAL(5, 3)", exact(decimal53TypeLimits[1]))
        );

        // TODO https://issues.apache.org/jira/browse/IGNITE-19858
        //  Cause serialization/deserialization mismatch in AbstractPlannerTest::checkSplitAndSerialization
        //
        //  RelDataType realType = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);
        //  List<Arguments> reals = List.of(
        //          arguments(realType, BigDecimal.valueOf(Float.MAX_VALUE).add(BigDecimal.ONE) + "::REAL", exact(Float.MAX_VALUE)),
        //          arguments(realType, BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.ONE), exact(Double.MAX_VALUE))
        //  );

        return Stream.of(
                tinyInts,
                smallInts,
                ints,
                bigints,
                decimal3s,
                decimal35s
        ).flatMap(Collection::stream);
    }

    private static Predicate<SearchBounds> exact(Object val) {
        Predicate<SearchBounds> p = b -> b instanceof ExactBounds && matchValue(val, ((ExactBounds) b).bound());
        return named(p, format("={}", val));
    }

    private static Predicate<SearchBounds> multi(Predicate<SearchBounds>... predicates) {
        Predicate<SearchBounds> p = b -> b instanceof MultiBounds
                && ((MultiBounds) b).bounds().size() == predicates.length
                && matchBounds(((MultiBounds) b).bounds(), predicates);
        return named(p, Arrays.toString(predicates));
    }

    private void assertBounds(String sql, Predicate<SearchBounds>... predicates) throws Exception {
        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class)
                .and(scan -> matchBounds(scan.searchBounds(), predicates))), List.of());
    }

    private void assertBounds(
            String sql,
            List<Object> params,
            IgniteSchema schema,
            Predicate<SearchBounds>... predicates
    ) throws Exception {
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class)
                .and(scan -> matchBounds(scan.searchBounds(), predicates))), params);
    }

    private static boolean matchBounds(List<SearchBounds> searchBounds, Predicate<SearchBounds>... predicates) {
        for (int i = 0; i < predicates.length; i++) {
            if (!predicates[i].test(searchBounds.get(i))) {
                LOG.info("{} bounds do not not match: {}", searchBounds.get(i), predicates[i]);
                return false;
            }
        }

        return true;
    }

    private static Predicate<SearchBounds> range(
            @Nullable Object lower,
            @Nullable Object upper,
            boolean lowerInclude,
            boolean upperInclude
    ) {
        Predicate<SearchBounds> range = b -> b instanceof RangeBounds
                && matchValue(lower, ((RangeBounds) b).lowerBound())
                && matchValue(upper, ((RangeBounds) b).upperBound())
                && lowerInclude == ((RangeBounds) b).lowerInclude()
                && upperInclude == ((RangeBounds) b).upperInclude();

        String lc = lowerInclude ? "[" : "(";
        String uc = upperInclude ? "]" : ")";

        return named(range, format("{}{}, {}{}", lc, lower, upper, uc));
    }

    private static RelDataType sqlType(SqlTypeName typeName) {
        return TYPE_FACTORY.createSqlType(typeName);
    }

    private static RelDataType sqlType(SqlTypeName typeName, int precision) {
        return TYPE_FACTORY.createSqlType(typeName, precision);
    }

    private static RelDataType sqlType(SqlTypeName typeName, int precision, int scale) {
        return TYPE_FACTORY.createSqlType(typeName, precision, scale);
    }

    private static boolean matchValue(@Nullable Object val, RexNode bound) {
        if (val == null || bound == null) {
            return val == bound;
        }

        String actual = Objects.toString(bound instanceof RexLiteral ? ((RexLiteral) bound).getValueAs(val.getClass()) : bound);
        String expected = Objects.toString(val);
        return expected.equals(actual);
    }

    private static Predicate<SearchBounds> named(Predicate<SearchBounds> p, String name) {
        return new Predicate<>() {
            @Override
            public boolean test(SearchBounds bounds) {
                return p.test(bounds);
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    private static UnaryOperator<TableBuilder> tableA(String tableName) {
        return tableBuilder -> tableBuilder
                .name(tableName)
                .name("TEST")
                .addColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.STRING)
                .addColumn("C3", NativeTypes.INT32)
                .addColumn("C4", NativeTypes.INT32)
                .addColumn("C5", NativeTypes.INT8)
                .distribution(IgniteDistributions.single())
                .size(100)
                .sortedIndex()
                .name("C1C2C3")
                .addColumn("C1", Collation.ASC_NULLS_LAST)
                .addColumn("C2", Collation.ASC_NULLS_LAST)
                .addColumn("C3", Collation.ASC_NULLS_LAST)
                .end();
    }

    private static UnaryOperator<TableBuilder> tableB(String tableName, String column, RelDataType type) {
        return tableBuilder -> tableBuilder
                .name(tableName)
                .addColumn("C1", NativeTypes.INT32)
                .addColumn(column, IgniteTypeFactory.relDataTypeToNative(type))
                .size(400)
                .distribution(IgniteDistributions.single());
    }
}
