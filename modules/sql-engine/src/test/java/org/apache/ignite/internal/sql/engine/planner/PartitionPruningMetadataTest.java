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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.prepare.RelWithSources;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningColumns;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadataExtractor;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link PartitionPruningMetadataExtractor} against optimized expressions.
 */
@WithSystemProperty(key = "FAST_QUERY_OPTIMIZATION_ENABLED", value = "false")
public class PartitionPruningMetadataTest extends AbstractPlannerTest {

    private static final IgniteSchema TABLE_C1 = createSchema(TestBuilders.table().name("T")
            .addKeyColumn("C1", NativeTypes.INT32)
            .addColumn("C2", NativeTypes.INT32, false)
            .distribution(TestBuilders.affinity(List.of(0), 1, 2))
            .build());

    private static final IgniteSchema TABLE_C1_NULLABLE_C2 = createSchema(TestBuilders.table().name("T")
            .addKeyColumn("C1", NativeTypes.INT32)
            .addColumn("C2", NativeTypes.INT32, true)
            .distribution(TestBuilders.affinity(List.of(0), 1, 2))
            .build());

    private static final IgniteSchema TABLE_C1_C2 = createSchema(TestBuilders.table().name("T")
            .addKeyColumn("C1", NativeTypes.INT32)
            .addKeyColumn("C2", NativeTypes.INT32)
            .addColumn("C3", NativeTypes.INT32, false)
            .distribution(TestBuilders.affinity(List.of(0, 1), 1, 2))
            .build());

    private static final IgniteSchema TABLE_C1_C2_NULLABLE_C3 = createSchema(TestBuilders.table().name("T")
            .addKeyColumn("C1", NativeTypes.INT32)
            .addKeyColumn("C2", NativeTypes.INT32)
            .addColumn("C3", NativeTypes.INT32, true)
            .distribution(TestBuilders.affinity(List.of(0, 1), 1, 2))
            .build());

    private static final IgniteSchema TABLE_C1_C2_C3 = createSchema(TestBuilders.table().name("T")
            .addKeyColumn("C1", NativeTypes.INT32)
            .addKeyColumn("C2", NativeTypes.INT32)
            .addKeyColumn("C3", NativeTypes.INT32)
            .addColumn("C4", NativeTypes.INT32, false)
            .distribution(TestBuilders.affinity(List.of(0, 1, 2), 1, 2))
            .build());

    private static final IgniteSchema TABLE_BOOL_C1 = createSchema(TestBuilders.table().name("T")
            .addKeyColumn("C1", NativeTypes.BOOLEAN)
            .addColumn("C2", NativeTypes.INT32, false)
            .distribution(TestBuilders.affinity(List.of(0), 1, 2))
            .build());

    private static final IgniteSchema TABLE_BOOL_C1_C3 = createSchema(TestBuilders.table().name("T")
            .addKeyColumn("C1", NativeTypes.BOOLEAN)
            .addKeyColumn("C2", NativeTypes.BOOLEAN)
            .addColumn("C3", NativeTypes.INT32, false)
            .distribution(TestBuilders.affinity(List.of(0, 1), 1, 2))
            .build());

    private static final IgniteSchema TABLE_C1_BOOLS = createSchema(TestBuilders.table().name("T")
            .addKeyColumn("C1", NativeTypes.INT32)
            .addColumn("C2", NativeTypes.BOOLEAN, false)
            .distribution(TestBuilders.affinity(List.of(0), 1, 2))
            .build());

    private static final IgniteSchema TABLE_ALL_BOOLS_C1 = createSchema(TestBuilders.table().name("T")
            .addKeyColumn("C1", NativeTypes.BOOLEAN)
            .addColumn("C2", NativeTypes.BOOLEAN, false)
            .distribution(TestBuilders.affinity(List.of(0), 1, 2))
            .build());

    @BeforeAll
    @AfterAll
    public static void resetFlag() {
        Commons.resetFastQueryOptimizationFlag();
    }

    /** Basic test cases for partition pruning metadata extractor, select case. */
    @ParameterizedTest(name = "SELECT: {0}")
    @EnumSource(TestCaseBasic.class)
    public void testBasicSelect(TestCaseBasic testCaseSimple) {
        checkPruningMetadata(testCaseSimple.data, SqlKind.SELECT);
    }

    /** Basic test cases for partition pruning metadata extractor, delete case. */
    @ParameterizedTest(name = "DELETE: {0}")
    @EnumSource(TestCaseBasic.class)
    public void testBasicDelete(TestCaseBasic testCaseSimple) {
        checkPruningMetadata(testCaseSimple.data, SqlKind.DELETE);
    }

    /** Basic test cases for partition pruning metadata extractor, insert case. */
    @ParameterizedTest(name = "INSERT: {0}")
    @EnumSource(TestCaseBasicInsert.class)
    public void testBasicInsert(TestCaseBasicInsert testCaseSimple) {
        checkPruningMetadata(testCaseSimple.data, SqlKind.INSERT);
    }

    /** Basic test cases for partition pruning metadata extractor, update case. */
    @ParameterizedTest(name = "UPDATE: {0}")
    @EnumSource(TestCaseBasicUpdate.class)
    public void testBasicUpdate(TestCaseBasicUpdate testCaseSimple) {
        checkPruningMetadata(testCaseSimple.data, SqlKind.UPDATE);
    }

    enum TestCaseBasicInsert {
        // single tuple insert
        CASE_1l("t VALUES (1, 10)", TABLE_C1, "[c1=1]"),
        CASE_1dp("t VALUES (?, ?)", TABLE_C1, "[c1=?0]"),

        // single tuple insert with reversed columns order
        CASE_2l("t(c2, c1) VALUES (10, 1)", TABLE_C1, "[c1=1]"),
        CASE_2dp("t(c2, c1) VALUES (?, ?)", TABLE_C1, "[c1=?1]"),

        // multi tuple insert
        CASE_3l("t VALUES (1, 10), (2, 20)", TABLE_C1, "[c1=1]", "[c1=2]"),
        CASE_3dp("t VALUES (?, ?), (?, ?)", TABLE_C1, "[c1=?0]", "[c1=?2]"),

        // multi tuple insert with reversed columns order
        CASE_4l("t(c2, c1) VALUES (10, 1), (20, 2)", TABLE_C1, "[c1=1]", "[c1=2]"),
        CASE_4dp("t(c2, c1) VALUES (?, ?), (?, ?)", TABLE_C1, "[c1=?1]", "[c1=?3]"),

        // meta is compiled partially from Project rel and partially from single tuple Values rel
        CASE_5l("t SELECT 10, x, x FROM (VALUES (1)) as s(x)", TABLE_C1_C2, "[c1=10, c2=1]"),
        // supposed to be similar, but because of explicit cast all expressions are taken from Project rel
        // https://issues.apache.org/jira/browse/IGNITE-23859 Investigate possibility to remove cast
        // CASE_5dp("t SELECT ?, x, x FROM (VALUES (?::INT)) as s(x)", TABLE_C1_C2, "[c1=?0, c2=?1]"),

        // meta is compiled partially from Project rel and partially from multi-tuple Values rel
        CASE_6l("t SELECT 10, x, x FROM (VALUES (1), (2)) as s(x)", TABLE_C1_C2, "[c1=10, c2=1]", "[c1=10, c2=2]"),
        // supposed to be similar, but because of explicit cast all expressions are taken from Project rel. Also plan contains UnionAll rel
        // https://issues.apache.org/jira/browse/IGNITE-23859 Investigate possibility to remove cast
        // CASE_6dp("t SELECT ?, x, x FROM (VALUES (?::INT), (?::INT)) as s(x)", TABLE_C1_C2, "[c1=?0, c2=?1]", "[c1=?0, c2=?2]"),

        // simple plan with explicit UnionAll rel
        CASE_7l("t SELECT 1, 10 UNION ALL SELECT 2, 20", TABLE_C1, "[c1=1]", "[c1=2]"),
        CASE_7dp("t SELECT ?, 10 UNION ALL SELECT ?, 20", TABLE_C1, "[c1=?0]", "[c1=?1]"),

        // UnionAll rel with project on top. Meta is compiled partially from project on top, partially from every input of UnionAll rel
        CASE_8l("t SELECT 10, x, x FROM (SELECT 1 UNION ALL SELECT 2) s(x)", TABLE_C1_C2, "[c1=10, c2=1]", "[c1=10, c2=2]"),
        // supposed to be similar, but because of explicit cast all expressions are taken from Project rel
        // https://issues.apache.org/jira/browse/IGNITE-23859 Investigate possibility to remove cast
        // CASE_8dp("t SELECT ?, x, x FROM (SELECT ?::INT UNION ALL SELECT ?::INT) s(x)", TABLE_C1_C2, "[c1=?0, c2=?1]", "[c1=?0, c2=?2]"),

        // mixed case, where only one branch contains additional projection
        // https://issues.apache.org/jira/browse/IGNITE-23859 Investigate possibility to remove cast
        // CASE_9l_dp("t SELECT ?, x, x FROM (SELECT 1 UNION ALL SELECT ?::INT) s(x)", TABLE_C1_C2, "[c1=?0, c2=1]", "[c1=?0, c2=?1]"),

        // one of the UnionAll branches contains ALWAYS FALSE predicate, thus must be ignored
        CASE_10l("t SELECT x, x FROM (SELECT 1 UNION ALL SELECT 2 FROM (VALUES (0)) WHERE FALSE UNION ALL SELECT 3) s(x)",
                TABLE_C1, "[c1=1]", "[c1=3]"),
        // https://issues.apache.org/jira/browse/IGNITE-23859 Investigate possibility to remove cast
        // CASE_10dp("t SELECT x, x FROM (SELECT ?::INT UNION ALL SELECT ?::INT FROM (VALUES (0)) WHERE FALSE UNION ALL "
        //        + "SELECT ?::INT) s(x)", TABLE_C1, "[c1=?0]", "[c1=?2]"),

        // single tuple insert with explicit cast
        CASE_12("t VALUES ('1'::smallint, 10)", TABLE_C1, "[c1=1]"),

        // Joins are not supported at the moment
        CASE_13("t SELECT sr1.x, sr2.x FROM system_range(1, 4) sr1,  system_range(1, 4) sr2", TABLE_C1),
        CASE_14("t(c1) VALUES (1), (SELECT 2)", TABLE_C1_NULLABLE_C2),

        // overflow is handled properly
        CASE_15(String.format("t(C1) VALUES (%d)", Long.MAX_VALUE), TABLE_C1_NULLABLE_C2),

        // expressions are not supported at the moment
        CASE_16("t(c1) VALUES (?), (? * 10)", TABLE_C1_NULLABLE_C2),
        CASE_17("t(c1) VALUES (?), (OCTET_LENGTH('TEST'))", TABLE_C1_NULLABLE_C2)
        ;

        private final TestCase data;

        TestCaseBasicInsert(String condition, IgniteSchema schema, String... expected) {
            this.data = new TestCase(condition, schema, expected);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    enum TestCaseBasicUpdate {
        SIMPLE_1a("c1 = 42", TABLE_C1, "[c1=42]"),
        SIMPLE_1b("42 = c1", TABLE_C1, "[c1=42]"),
        SIMPLE_1c("c1 = ?", TABLE_C1, "[c1=?0]"),
        SIMPLE_1d("? = c1", TABLE_C1, "[c1=?0]"),
        SIMPLE_1e("c1 = '42'::INTEGER", TABLE_C1, "[c1=42]"),
        ;
        private final TestCase data;

        TestCaseBasicUpdate(String condition, IgniteSchema schema, String... expected) {
            this.data = new TestCase(condition, schema, expected);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    enum TestCaseBasic {
        // SELECT * FROM t WHERE ... is omitted

        SIMPLE_1a("c1 = 42", TABLE_C1, "[c1=42]"),
        SIMPLE_1b("42 = c1", TABLE_C1, "[c1=42]"),
        SIMPLE_1c("c1 = ?", TABLE_C1, "[c1=?0]"),
        SIMPLE_1d("? = c1", TABLE_C1, "[c1=?0]"),
        SIMPLE_1e("c1 = '42'::INTEGER", TABLE_C1, "[c1=42]"),

        // OR

        OR_1a("c1 = 1 OR c1 = 2", TABLE_C1, "[c1=1]", "[c1=2]"),
        OR_1b("c1 = 1 OR c1 = 1", TABLE_C1, "[c1=1]"),

        OR_1c("c1 = 42 OR c1 = ?", TABLE_C1, "[c1=42]", "[c1=?0]"),
        OR_1d("c1 = 42 OR c1 = ? OR c1 = ?", TABLE_C1, "[c1=42]", "[c1=?0]", "[c1=?1]"),

        OR_1e("c1 = 1 OR c2 = 2", TABLE_C1),
        OR_1f("c1 = 1 OR (c2 = 2 OR c1 = 3)", TABLE_C1),

        OR_2a("c1 = 1 OR c1 > 10", TABLE_C1),

        OR_BOOL_EXPR_1a("c1 = 42 OR true", TABLE_C1),
        OR_BOOL_EXPR_1b("c1 = 42 OR false", TABLE_C1, "[c1=42]"),
        //  filters=[=($t0, 42)], because c2 is not nullable
        OR_BOOL_EXPR_1c("c1 = 42 OR c2 IS NULL", TABLE_C1, "[c1=42]"),
        // filters=[true]
        OR_BOOL_EXPR_1d("c1 = 42 OR c2 IS NOT NULL", TABLE_C1),
        // filters=[OR(=($t0, 42), $t1)]
        OR_BOOL_EXPR_1e("c1 = 42 OR c2 IS TRUE", TABLE_C1_BOOLS),
        // filters=[OR(=($t0, 42), NOT($t1))]
        OR_BOOL_EXPR_1f("c1 = 42 OR c2 IS NOT TRUE", TABLE_C1_BOOLS),
        // filters=[OR(=($t0, 42), NOT($t1))]
        OR_BOOL_EXPR_1g("c1 = 42 OR c2 IS FALSE", TABLE_C1_BOOLS),
        // filters=[OR(=($t0, 42), $t1)]
        OR_BOOL_EXPR_1h("c1 = 42 OR c2 IS NOT FALSE", TABLE_C1_BOOLS),

        // AND

        AND_1a("c1 = 42 AND c2 = 3", TABLE_C1, "[c1=42]"),
        AND_1b("c2 = 3 AND c1 = 42", TABLE_C1, "[c1=42]"),

        AND_1c("c1 = 42 AND c2 = 3", TABLE_C1, "[c1=42]"),
        AND_1d("c2 = 3 AND c1 = 42", TABLE_C1, "[c1=42]"),

        AND_2a("c1 = 42 AND c2 = 3", TABLE_C1_C2, "[c1=42, c2=3]"),
        AND_2b("c1 = 42 AND c2 = 3 AND c3 = 100", TABLE_C1_C2, "[c1=42, c2=3]"),

        // Column has different values -> conflict
        AND_3a("c1 = 1 AND c2 = 2 AND c1 = 2", TABLE_C1_C2),
        AND_3b("c1 = 1 AND c2 = 2 AND c1 = 2 and c2 = 3", TABLE_C1_C2),
        AND_3c("c1 = 1 AND c2 = 2 AND c1 = 2 or c3 = 3", TABLE_C1_C2),

        // Column might have different values.
        // The query probably contains an error c1 = constant and a dynamic parameter.
        AND_3d("c1 = 42 AND c1 = ?", TABLE_C1),
        AND_3e("c1 = 42 AND c1 = ? AND c2 = ?", TABLE_C1_C2),

        AND_4a("c1 = 1 AND c2 = 2 AND c3 = 3 and c4 = 4", TABLE_C1_C2_C3, "[c1=1, c2=2, c3=3]"),
        AND_4b("c1 = 1 AND c2 = 2 AND c3 = 3 OR c3 = 4 AND c2 = 5 AND c1 = 6", TABLE_C1_C2_C3, "[c1=1, c2=2, c3=3]", "[c1=6, c2=5, c3=4]"),

        // Some colocation key columns are missing
        MISSING_KEYS_1a("c1 = 1 AND c2 = 10 OR c1 = 2", TABLE_C1_C2),
        MISSING_KEYS_1b("c1 = 1 OR c1 = 2 AND c2 = 10 ", TABLE_C1_C2),

        MISSING_KEYS_2a("c1 = 1 AND c2 = 10 OR c1 = 2", TABLE_C1_C2),
        MISSING_KEYS_2b("c1 = 1 OR c1 = 2 AND c2 = 10 ", TABLE_C1_C2),

        // AND restrict (additional condition on non colocated column still allows to extract column values)

        AND_RESTRICT_1a("c1 = 42 AND c2 > 10", TABLE_C1, "[c1=42]"),
        AND_RESTRICT_1b("c1 = 42 AND c2 >= 10", TABLE_C1, "[c1=42]"),
        AND_RESTRICT_1c("c1 = 42 AND c2 < 10", TABLE_C1, "[c1=42]"),
        AND_RESTRICT_1d("c1 = 42 AND c2 <= 10", TABLE_C1, "[c1=42]"),
        AND_RESTRICT_1e("c1 = 42 AND c2 != 10", TABLE_C1, "[c1=42]"),
        AND_RESTRICT_1f("c1 = 42 AND c2 <> 10", TABLE_C1, "[c1=42]"),

        // both c2 AND c1 are not nullable.
        AND_BOOL_EXPR_1a("c1 = 42 AND c2 IS NULL", TABLE_C1),
        AND_BOOL_EXPR_1b("c1 = 42 AND c2 IS NOT NULL", TABLE_C1, "[c1=42]"),
        AND_BOOL_EXPR_1c("c1 = 42 AND c2 IS TRUE", TABLE_C1_BOOLS, "[c1=42]"),
        AND_BOOL_EXPR_1d("c1 = 42 AND c2 IS NOT TRUE", TABLE_C1_BOOLS, "[c1=42]"),
        AND_BOOL_EXPR_1e("c1 = 42 AND c2 IS FALSE", TABLE_C1_BOOLS, "[c1=42]"),
        AND_BOOL_EXPR_1f("c1 = 42 AND c2 IS NOT FALSE", TABLE_C1_BOOLS, "[c1=42]"),
        AND_BOOL_EXPR_1g("c1 = 42 AND true IS NOT NULL", TABLE_C1, "[c1=42]"),
        AND_BOOL_EXPR_1h("c1 = 42 AND true", TABLE_C1, "[c1=42]"),
        AND_BOOL_EXPR_1i("c1 = 42 AND false", TABLE_C1),

        AND_OTHER_1a("c1 = 42 AND 5 > 1", TABLE_C1, "[c1=42]"),

        // DISTINCT FROM

        DISTINCT_FROM_1a("c1 IS NOT DISTINCT FROM 42", TABLE_C1, "[c1=42]"),
        DISTINCT_FROM_1b("c1 IS NOT DISTINCT FROM 42 AND c2 = 10", TABLE_C1, "[c1=42]"),
        DISTINCT_FROM_1c("c1 IS NOT DISTINCT FROM 42 OR c2 = 10", TABLE_C1),

        DISTINCT_FROM_2a("c1 IS DISTINCT FROM 42", TABLE_C1),
        DISTINCT_FROM_2b("c1 IS NOT DISTINCT FROM null", TABLE_C1),
        DISTINCT_FROM_2c("c1 IS DISTINCT FROM null", TABLE_C1),

        // IN

        IN_1a("c1 IN (1, 2, 3)", TABLE_C1, "[c1=1]", "[c1=2]", "[c1=3]"),
        IN_1b("c1 NOT IN (1, 2, 3)", TABLE_C1),
        IN_1c("c1 IN (1, 2, c2)", TABLE_C1),

        // BETWEEN

        BETWEEN_1a("c1 BETWEEN 1 AND 10", TABLE_C1),
        // Rewritten to Sarg
        BETWEEN_1b("c1 BETWEEN 1 AND 1", TABLE_C1, "[c1=1]"),

        // IS NULL

        IS_NULL_1a("c1 IS NULL", TABLE_C1),
        // should be converted to false by the optimizer
        IS_NULL_1b("c1 = 42 AND c1 IS NULL", TABLE_C1),
        IS_NULL_1c("c1 = 42 OR c1 IS NOT NULL", TABLE_C1),

        IS_NOT_NULL_1a("c1 IS NOT NULL", TABLE_C1),
        IS_NOT_NULL_1b("c1 = 42 AND c1 IS NOT NULL", TABLE_C1, "[c1=42]"),
        IS_NOT_NULL_1c("c1 = 42 OR c1 IS NOT NULL", TABLE_C1),

        // c2 is nullable.
        IS_NOT_NULL_2a("c1 = 42 AND c2 IS NULL", TABLE_C1_NULLABLE_C2, "[c1=42]"),

        // Negation

        NEGATE_1a("c1 != 42", TABLE_C1),
        // filters=[<>($t0, 42)]
        NEGATE_1b("NOT(c1 = 42)", TABLE_C1),
        // filters=[=($t0, 42)]
        NEGATE_1c("NOT(c1 != 42)", TABLE_C1, "[c1=42]"),

        // filters=[OR(=($t0, 42), =($t1, 10))]
        NEGATE_2a("NOT(c1 != 42 AND c2 != 10)", TABLE_C1_C2),
        // filters=[AND(=($t0, 42), =($t1, 10))]
        NEGATE_2b("NOT(c1 != 42 OR c2 != 10)", TABLE_C1_C2, "[c1=42, c2=10]"),
        // filters=[OR(=($t0, 42), <>($t1, 10))]
        NEGATE_2c("NOT(c1 != 42 AND c2 = 10)", TABLE_C1_C2),
        // filters=[AND(=($t0, 42), =($t1, 10), <>($t2, 1))]
        NEGATE_2d("NOT(c1 != 42 OR c2 != 10 OR c3=1)", TABLE_C1_C2, "[c1=42, c2=10]"),

        NO_META_1a("c1 = c2", TABLE_C1),
        NO_META_1b("c1 = c1", TABLE_C1),
        NO_META_1c("c2 = 42", TABLE_C1),
        NO_META_1d("true", TABLE_C1),
        NO_META_1e("false", TABLE_C1),
        NO_META_1f("c1 = ABS(c1)", TABLE_C1),
        NO_META_1g("c1 = ABS(c2)", TABLE_C1),

        // No metadata, complex expressions:
        NO_META_2a("c1 = c1 + 0", TABLE_C1),
        NO_META_2b("c1 = 0 + c1", TABLE_C1),
        NO_META_2c("c1 = 1 * c1", TABLE_C1),
        NO_META_2d("c1 = 2 * c1", TABLE_C1),
        NO_META_2e("c1 = (10 + c1)", TABLE_C1),
        NO_META_2f("c1 = (10 + c2)", TABLE_C1),

        NO_META_4a("c1 > 10", TABLE_C1),
        NO_META_4b("c1 < 10", TABLE_C1),
        NO_META_4c("c1 <= 10", TABLE_C1),
        NO_META_4d("c1 >= 10", TABLE_C1),

        // We do not know the result of an function expression, so we can not do extract column values
        NO_META_5a("c1 = 42 OR LENGTH('abc') = 3", TABLE_C1),
        NO_META_5b("c1 = 42 OR SUBSTRING(c2::VARCHAR, 2) = 'a'", TABLE_C1),
        NO_META_5c("c1 = 42 OR SUBSTRING(c1::VARCHAR, 2) = '2'", TABLE_C1),

        NO_META_5d("c1 = 42 AND LENGTH(SUBSTRING('aaaaaaa', 2)) > 0", TABLE_C1_C2),
        NO_META_5e("c1 = 42 AND c1 = SUBSTRING(c3::VARCHAR, 2)::INTEGER", TABLE_C1_C2),
        NO_META_5f("c1 = 42 AND c2 = SUBSTRING(c3::VARCHAR, 2)::INTEGER", TABLE_C1_C2),

        CONST_FOLDING_1a("c1 = 10 + 4", TABLE_C1, "[c1=14]"),
        CONST_FOLDING_1b("c1 = ? + 4", TABLE_C1),

        // 0s removed by Calcite.
        CONST_FOLDING_1c("c1 = 0 + 42", TABLE_C1, "[c1=42]"),
        CONST_FOLDING_1d("c1 = 42 + 0", TABLE_C1, "[c1=42]"),
        ;

        private final TestCase data;

        TestCaseBasic(String condition, IgniteSchema schema, String... expected) {
            this.data = new TestCase(condition, schema, expected);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    /** Test cases for bool columns, select case. */
    @ParameterizedTest(name = "SELECT: {0}")
    @EnumSource(TestCaseBool.class)
    public void testBoolSelect(TestCaseBool testCaseBool) {
        checkPruningMetadata(testCaseBool.data, SqlKind.SELECT);
    }

    /** Test cases for bool columns, delete case. */
    @ParameterizedTest(name = "DELETE: {0}")
    @EnumSource(TestCaseBool.class)
    public void testBoolDelete(TestCaseBool testCaseBool) {
        checkPruningMetadata(testCaseBool.data, SqlKind.DELETE);
    }

    enum TestCaseBool {

        BOOL_1a("c1 IS TRUE", TABLE_BOOL_C1, "[c1=true]"),
        BOOL_1b("c1 IS FALSE", TABLE_BOOL_C1, "[c1=false]"),
        BOOL_1c("NOT c1", TABLE_BOOL_C1, "[c1=false]"),
        BOOL_1d("NOT NOT c1", TABLE_BOOL_C1, "[c1=true]"),

        // converted to filters=[true]
        BOOL_1e("c1 OR true", TABLE_BOOL_C1),
        BOOL_1f("c1 OR false", TABLE_BOOL_C1, "[c1=true]"),

        BOOL_2a("c1 AND c2", TABLE_BOOL_C1_C3, "[c1=true, c2=true]"),
        BOOL_2b("c1 AND c2 IS FALSE", TABLE_BOOL_C1_C3, "[c1=true, c2=false]"),
        BOOL_2c("NOT c1 AND c2", TABLE_BOOL_C1_C3, "[c1=false, c2=true]"),
        BOOL_2d("NOT c1 AND NOT c2", TABLE_BOOL_C1_C3, "[c1=false, c2=false]"),

        BOOL_3a("c1 OR c2", TABLE_BOOL_C1_C3),
        BOOL_3b("c1 IS TRUE OR c2", TABLE_BOOL_C1_C3),
        BOOL_3c("c1 OR c2 IS TRUE", TABLE_BOOL_C1_C3),

        BOOL_3d("c1 AND c2 IS TRUE", TABLE_ALL_BOOLS_C1, "[c1=true]"),
        BOOL_3e("c1 AND c2 IS FALSE", TABLE_ALL_BOOLS_C1, "[c1=true]"),

        // filters=[IS NOT NULL($t0)]
        BOOL_4e("c1 = c1", TABLE_BOOL_C1),
        ;

        private final TestCase data;

        TestCaseBool(String condition, IgniteSchema schema, String... expected) {
            this.data = new TestCase(condition, schema, expected);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    /** Test cases for CASE expression, select case. */
    @ParameterizedTest(name = "SELECT: {0}")
    @EnumSource(TestCaseCaseExpr.class)
    public void testCaseExprSelect(TestCaseCaseExpr testCaseBool) {
        checkPruningMetadata(testCaseBool.data, SqlKind.SELECT);
    }

    /** Test cases for CASE expression, delete case. */
    @ParameterizedTest(name = "DELETE: {0}")
    @EnumSource(TestCaseCaseExpr.class)
    public void testCaseExprDelete(TestCaseCaseExpr testCaseBool) {
        checkPruningMetadata(testCaseBool.data, SqlKind.DELETE);
    }

    enum TestCaseCaseExpr {
        // filters=[=($t0, 42)]
        CASE_1a("CASE c1 WHEN 42 THEN true ELSE false END", TABLE_C1, "[c1=42]"),
        // filters=[IS NOT TRUE(=($t0, 2))]
        CASE_1b("CASE c1 WHEN 42 THEN false ELSE true END", TABLE_C1),
        // filters=[=($t0, 42)]
        CASE_1c("CASE c1 WHEN 42 THEN true WHEN 3 THEN false ELSE false END", TABLE_C1, "[c1=42]"),
        // filters=[OR(=($t0, 42), IS NOT TRUE(=($t0, 3)))]
        CASE_1d("CASE c1 WHEN 42 THEN true WHEN 3 THEN false ELSE true END", TABLE_C1),

        // filters=[=($t0, $t1)]
        CASE_2a("CASE c1 WHEN c2 THEN true ELSE false END", TABLE_C1),
        // filters=[IS NOT TRUE(=($t0, $t1))]
        CASE_2b("CASE c1 WHEN c2 THEN false ELSE true END", TABLE_C1),
        // filters=[=($t0, 42)]
        CASE_2c("CASE c1 WHEN 42 THEN true WHEN c2 THEN false ELSE false END", TABLE_C1, "[c1=42]"),
        // filters=[OR(=($t0, 42), IS NOT TRUE(=($t0, $t1)))]
        CASE_2d("CASE c1 WHEN 42 THEN true WHEN c2 THEN false ELSE true END", TABLE_C1),

        // [OR(=($t0, 10), =($t0, 42))]
        CASE_3a("CASE c1 WHEN 42 THEN true WHEN 10 THEN true ELSE false END", TABLE_C1, "[c1=10]", "[c1=42]"),
        ;
        final TestCase data;

        TestCaseCaseExpr(String condition, IgniteSchema schema, String... expected) {
            this.data = new TestCase(condition, schema, expected);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private static class TestCase {

        private final String condition;

        private final IgniteSchema schema;

        private final String[] expected;

        TestCase(String condition, IgniteSchema schema, String... expected) {
            this.condition = condition;
            this.schema = schema;
            this.expected = expected;
        }

        List<Integer> colocationKeys() {
            IgniteTable table = (IgniteTable) schema.tables().get("T");
            return table.distribution().getKeys();
        }

        List<String> columnNames() {
            IgniteTable table = (IgniteTable) schema.tables().get("T");
            List<String> names = new ArrayList<>();
            TableDescriptor tableDescriptor = table.descriptor();

            for (int i = 0; i < tableDescriptor.columnsCount(); i++) {
                ColumnDescriptor col = tableDescriptor.columnDescriptor(i);
                names.add(col.name());
            }

            return names;
        }

        @Override
        public String toString() {
            return condition + " -> " + Arrays.toString(expected);
        }
    }

    private void checkPruningMetadata(TestCase testCase, SqlKind kind) {
        String statement;
        switch (kind) {
            case SELECT:
                statement = "SELECT * FROM t WHERE " + testCase.condition;
                break;
            case DELETE:
                statement = "DELETE FROM t WHERE " + testCase.condition;
                break;
            case INSERT:
                statement = "INSERT INTO " + testCase.condition;
                break;
            case UPDATE:
                statement = "UPDATE t SET C2 = 100 WHERE " + testCase.condition;
                break;
            default:
                throw new UnsupportedOperationException(kind.name());
        }

        List<String> expectedMetadata = Arrays.asList(testCase.expected);
        List<Integer> colocationKeys = testCase.colocationKeys();
        List<String> columnNames = testCase.columnNames();

        log.info("Statement: {}", statement);
        log.info("Keys: {}", colocationKeys.stream().map(columnNames::get).collect(Collectors.toList()));
        log.info("Expected: {}", expectedMetadata);

        IgniteRel igniteRel;
        try {
            igniteRel = physicalPlan(statement, testCase.schema, DISABLE_KEY_VALUE_MODIFY_RULES);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to build a physical plan", e);
        }
        log.info("Plan: {}", RelOptUtil.dumpPlan("\n", igniteRel, SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES));

        extractMetadataAndCheck(igniteRel, columnNames, expectedMetadata);
    }

    private void extractMetadataAndCheck(IgniteRel rel, List<String> columnNames, List<String> expectedMetadata) {
        PartitionPruningMetadataExtractor extractor = new PartitionPruningMetadataExtractor();
        RelWithSources relWithSoucres = Cloner.cloneAndAssignSourceId(rel, rel.getCluster());
        PartitionPruningMetadata actual = extractor.go(relWithSoucres.root());

        List<String> actualMetadata;

        if (actual.data().isEmpty()) {
            actualMetadata = Collections.emptyList();
        } else {
            PartitionPruningColumns columns = actual.data().long2ObjectEntrySet().iterator().next().getValue();

            // replace column indices with column names in lower case
            actualMetadata = PartitionPruningColumns.canonicalForm(columns).stream().map(cols -> cols.stream().map(col -> {
                String columnName = columnNames.get(col.getKey()).toLowerCase(Locale.US);
                return Map.entry(columnName, col.getValue());
            }).collect(Collectors.toList()).toString()).collect(Collectors.toList());
        }

        log.info("Actual metadata: {}", actualMetadata);

        assertEquals(expectedMetadata, actualMetadata, "Pruning metadata");
    }
}
