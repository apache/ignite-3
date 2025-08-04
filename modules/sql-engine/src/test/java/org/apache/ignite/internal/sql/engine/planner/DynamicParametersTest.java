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

import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.DECIMAL_DYNAMIC_PARAM_PRECISION;
import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.DECIMAL_DYNAMIC_PARAM_SCALE;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.StatementChecker;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * Test cases for dynamic parameters.
 */
@WithSystemProperty(key = "FAST_QUERY_OPTIMIZATION_ENABLED", value = "false")
public class DynamicParametersTest extends AbstractPlannerTest {
    @BeforeAll
    @AfterAll
    public static void resetFlag() {
        Commons.resetFastQueryOptimizationFlag();
    }

    /**
     * This test case triggers "Conversion to relational algebra failed to preserve datatypes" assertion,
     * if {@link IgniteSqlValidator} do not set nullability for inferred dynamic parameter type.
     */
    @Test
    public void testInferencePreservesNullability() throws Throwable {
        checkStatement()
                .sql("SELECT 1 - ? + 2", 1)
                .ok()
                .getExecutable().execute();
    }

    /** Arithmetic expressions. */
    @TestFactory
    public Stream<DynamicTest> testArithExprs() {
        return Stream.of(
                sql("SELECT 1 + ?", 1).parameterTypes(nullable(NativeTypes.INT32)).ok(),
                sql("SELECT NULL + ?", 1).parameterTypes(nullable(NativeTypes.INT32)).project("null:INTEGER"),
                sql("SELECT ? + NULL", 1).parameterTypes(nullable(NativeTypes.INT32)).project("null:INTEGER"),

                sql("SELECT 1 + ?", "1").parameterTypes(nullable(NativeTypes.STRING))
                        .fails("Cannot apply '+' to arguments of type '<INTEGER> + <VARCHAR>'"),
                sql("SELECT ? + 1", "1").parameterTypes(nullable(NativeTypes.STRING))
                        .fails("Cannot apply '+' to arguments of type '<VARCHAR> + <INTEGER>'"),

                // NULL is allowed in arithmetic expressions, if another operand is present.
                sql("SELECT ? * 2", new Object[]{null})
                        .parameterTypes(nullable(null))
                        .ok(),

                sql("SELECT 1 + ?", Unspecified.UNKNOWN).parameterTypes(nullable(NativeTypes.INT32)).ok(),
                sql("SELECT ? + 1", Unspecified.UNKNOWN).parameterTypes(nullable(NativeTypes.INT32)).ok(),
                sql("SELECT ? + ?", Unspecified.UNKNOWN, Unspecified.UNKNOWN).fails("Ambiguous operator <UNKNOWN> + <UNKNOWN>"),
                sql("SELECT NULL + ?", Unspecified.UNKNOWN).fails("Ambiguous operator <NULL> + <UNKNOWN>")
        );
    }

    /** Comparison expressions. */
    @TestFactory
    public Stream<DynamicTest> testCmpExprs() {
        return Stream.of(
                // comparison
                sql("SELECT ? > 1", 1).parameterTypes(nullable(NativeTypes.INT32)).ok(),
                sql("SELECT ? > 1", "1").parameterTypes(nullable(NativeTypes.STRING))
                        .fails("Cannot apply '>' to arguments of type '<VARCHAR> > <INTEGER>'"),
                sql("SELECT 1 > ?", "1").parameterTypes(nullable(NativeTypes.STRING))
                        .fails("Cannot apply '>' to arguments of type '<INTEGER> > <VARCHAR>'"),
                sql("SELECT ? > 1", Unspecified.UNKNOWN).parameterTypes(nullable(NativeTypes.INT32)).ok(),
                sql("SELECT 1 > ?", Unspecified.UNKNOWN).parameterTypes(nullable(NativeTypes.INT32)).ok(),
                sql("SELECT ? > ?", Unspecified.UNKNOWN, Unspecified.UNKNOWN).fails("Ambiguous operator <UNKNOWN> > <UNKNOWN>"),
                sql("SELECT ? > NULL", 1).parameterTypes(nullable(NativeTypes.INT32)).project("null:BOOLEAN"),
                sql("SELECT NULL = ?", 1).parameterTypes(nullable(NativeTypes.INT32)).project("null:BOOLEAN"),
                sql("SELECT ? = NULL", 1).parameterTypes(nullable(NativeTypes.INT32)).project("null:BOOLEAN"),

                // NULL is allowed in comparison
                sql("SELECT ? = ?", null, null).project("=(?0, ?1)")
        );
    }

    /**
     * IN expression.
     */
    @TestFactory
    public Stream<DynamicTest> testInExpression() {
        return Stream.of(
                sql("SELECT ? IN ('1', '2')", 1).parameterTypes(nullable(NativeTypes.INT32))
                        .fails("Values passed to IN operator must have compatible types"),
                sql("SELECT ? IN (1, 2)", 1).parameterTypes(nullable(NativeTypes.INT32)).project("OR(=(?0, 1), =(?0, 2))"),

                sql("SELECT ? IN (1)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                sql("SELECT ? IN (?, 1)", Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                sql("SELECT ? IN (?, ?)", Unspecified.UNKNOWN, Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                sql("SELECT 1 IN (?, ?)", Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .project("OR(=(1, ?0), =(1, ?1))"),

                sql("SELECT ? IN ('1')", 2)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .fails("Values passed to IN operator must have compatible types"),

                sql("SELECT ? IN ('1', 2)", 2)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .fails("Values in expression list must have compatible types")
        );

        // TODO https://issues.apache.org/jira/browse/IGNITE-23039 Add support for Sarg serialization/deserialization
        // sql("SELECT ? IN (1, 2)", "1")
        //        .parameterTypes(nullable(NativeTypes.STRING))
        //        .project("SEARCH(CAST(?0):INTEGER, Sarg[1, 2])"),

        // TODO https://issues.apache.org/jira/browse/IGNITE-22084: Sql. Add support for row data type.
        // sql("SELECT (?,?) IN ((1,2))", 1, 2)
        //        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT32))
        //        .project("AND(=(?0, 1), =(?1, 2))"),

        // sql("SELECT (?,?) IN ((1,2))", "1", "2").fails(requireExplicitCast),
        // sql("SELECT (?,?) IN (('1', 2))", 1, "2").fails(requireExplicitCast),
        // sql("SELECT (?,?) IN ((1, '2'))", "1", "2").fails(requireExplicitCast)
    }

    /** CASE expression. */
    @TestFactory
    public Stream<DynamicTest> testCaseWhenExpression() {
        IgniteTypeFactory tf = Commons.typeFactory();
        // String parameter is inferred as VARCHAR with default attributes, but
        // the createSqlType(VARCHAR, DEFAULT_PRECISION) != createSqlType(VARCHAR)
        // have to create rel data type directly instead of building one from a native type.
        RelDataType nullableVarchar = tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.VARCHAR), true);

        return Stream.of(
                sql("SELECT CASE ? = ? WHEN true THEN 1 ELSE 2 END", 1, 1)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT32))
                        .ok(),

                sql("SELECT CASE WHEN ? = '1' THEN ? ELSE ? END", "1", 2, 2.5)
                        .parameterTypes(nullableVarchar, nullable(NativeTypes.INT32), nullable(NativeTypes.DOUBLE))
                        .project("CASE(=(?0, _UTF-8'1'), CAST(?1):DOUBLE, ?2)"),

                sql("SELECT CASE ? = ? WHEN true THEN 1 ELSE 2 END", 1, "1")
                        .parameterTypes(nullable(NativeTypes.INT32), nullableVarchar)
                        .fails("Cannot apply '=' to arguments of type '<INTEGER> = <VARCHAR>'"),

                sql("SELECT CASE WHEN ? = '1' THEN ? ELSE ? END", "1", "2", 2.5)
                        .fails("Illegal mixing of types in CASE or COALESCE statement"),

                sql("SELECT CASE WHEN ? THEN 1 WHEN ? THEN 2 ELSE 3 END", 1, 2)
                        .fails("Expected a boolean type")
        );
    }

    @TestFactory
    public Stream<DynamicTest> testCase() {
        // CREATE TABLE TBL1(ID INT PRIMARY KEY, VAL VARCHAR, NUM INT)
        // select case when (VAL = ?) then 0 else (case when (NUM IS NULL) then ? else ? end) end

        IgniteTypeFactory tf = Commons.typeFactory();
        RelDataType nullableStr = tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType nullType = tf.createSqlType(SqlTypeName.NULL);
        RelDataType nullableInt = tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.INTEGER), true);

        return Stream.of(
                checkStatement()
                        .table("TBL1", "ID", NativeTypes.INT32, "VAL", NativeTypes.STRING, "NUM", NativeTypes.INT32)
                        .sql("select case when (VAL = ?) then 0 else (case when (NUM IS NULL) then ? else ? end) end FROM TBL1",
                                "diff", null, 1)
                        .parameterTypes(nullableStr, nullType, nullableInt)
                        .project("CASE(=($t0, ?0), 0, CASE(IS NULL($t1), CAST(?1):INTEGER, ?2))"),

                checkStatement()
                        .table("TBL1", "ID", NativeTypes.INT32, "VAL", NativeTypes.STRING, "NUM", NativeTypes.INT32)
                        .sql("select case when (VAL = ?) then 0 else (case when (NUM IS NULL) then ? else ? end) end FROM TBL1",
                                Unspecified.UNKNOWN, Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .parameterTypes(nullableStr, nullType, nullableInt)
                        .fails("Illegal mixing of types in CASE or COALESCE statement"),

                checkStatement()
                        .table("TBL1", "ID", NativeTypes.INT32, "VAL", NativeTypes.STRING, "NUM", NativeTypes.INT32)
                        .sql("select case when (VAL = ?) then 0 else (case when (NUM IS NULL) then ? else ? end) end FROM TBL1",
                                "diff", 1, null)
                        .parameterTypes(nullableStr, nullableInt, nullType)
                        .project("CASE(=($t0, ?0), 0, CASE(IS NULL($t1), ?1, CAST(?2):INTEGER))"),

                checkStatement()
                        .table("TBL1", "ID", NativeTypes.INT32, "VAL", NativeTypes.STRING, "NUM", NativeTypes.INT32)
                        .sql("select case when (VAL = ?) then 0 else (case when (NUM IS NULL) then ? else ? end) end FROM TBL1",
                                Unspecified.UNKNOWN, Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .parameterTypes(nullableStr, nullType, nullableInt)
                        .fails("Illegal mixing of types in CASE or COALESCE statement")
        );
    }

    /** Dynamic parameters in independent contexts. */
    @TestFactory
    public Stream<DynamicTest> testStandalone() {
        return Stream.of(
                checkStatement()
                        .sql("SELECT ?", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .ok(),

                checkStatement()
                        .sql("SELECT ?", BigDecimal.ONE)
                        .parameterTypes(nullable(NativeTypes.decimalOf(DECIMAL_DYNAMIC_PARAM_PRECISION, DECIMAL_DYNAMIC_PARAM_SCALE)))
                        .ok(),

                checkStatement()
                        .sql("SELECT ?", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                checkStatement()
                        .sql("SELECT CAST(? AS INTEGER)", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("CAST(?0):INTEGER"),

                checkStatement()
                        .sql("SELECT CAST(? AS DECIMAL(60, 30))", BigDecimal.ONE)
                        .parameterTypes(nullable(NativeTypes.decimalOf(60, 30)))
                        .project("CAST(?0):DECIMAL(60, 30)"),

                checkStatement()
                        .sql("SELECT ?::DECIMAL(60, 30)", BigDecimal.ONE)
                        .parameterTypes(nullable(NativeTypes.decimalOf(60, 30)))
                        .project("CAST(?0):DECIMAL(60, 30)"),

                checkStatement()
                        .sql("SELECT CAST(? AS INTEGER)", "1")
                        .parameterTypes(nullable(NativeTypes.STRING))
                        .project("CAST(?0):INTEGER"),

                checkStatement()
                        .sql("SELECT CAST(? AS INTEGER)", Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("CAST(?0):INTEGER"),

                checkStatement()
                        .sql("SELECT -?", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .ok(),

                checkStatement()
                        .sql("SELECT -?", Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .fails("Ambiguous operator -<UNKNOWN>"),

                checkStatement()
                        .sql("SELECT NOT ?", Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.BOOLEAN))
                        .project("NOT(?0)"),

                checkStatement()
                        .sql("SELECT ? IS NULL", Unspecified.UNKNOWN)
                        .fails("Ambiguous operator <UNKNOWN> IS NULL"),

                checkStatement()
                        .sql("SELECT ? IS NOT NULL", Unspecified.UNKNOWN)
                        .fails("Ambiguous operator <UNKNOWN> IS NOT NULL")
        );
    }

    /** Subqueries in various contexts. */
    @TestFactory
    public Stream<DynamicTest> testSubqueries() {
        return Stream.of(
                checkStatement()
                        .sql("SELECT (SELECT ?)", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .ok(),

                checkStatement()
                        .sql("SELECT (SELECT ?)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                // Predicates

                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("SELECT * FROM t1 WHERE int_col = SOME(SELECT ?)", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .ok(),

                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("SELECT * FROM t1 WHERE int_col = SOME(SELECT ?)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("SELECT * FROM t1 WHERE int_col = ANY(SELECT ?)", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .ok(),

                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("SELECT * FROM t1 WHERE int_col = ANY(SELECT ?)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                // DML

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("INSERT INTO t1 VALUES (1), ((SELECT ?))", 1)
                        .ok(),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("INSERT INTO t1 VALUES (1), ((SELECT ?))", Unspecified.UNKNOWN)
                        .fails("Values passed to VALUES operator must have compatible types"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT64)
                        .sql("UPDATE t1 SET c1 = (SELECT ?)", 1)
                        .ok(),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT64)
                        .sql("UPDATE t1 SET c1 = (SELECT ?)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter")
        );
    }

    /** BETWEEN operator. */
    @TestFactory
    public Stream<DynamicTest> testBetween() {
        return Stream.of(
                sql("SELECT 1 BETWEEN ? AND ?", 1, 10)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT32))
                        .ok(),

                sql("SELECT 1 BETWEEN ? AND ?", 1, Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT32))
                        .ok(),

                sql("SELECT 1 BETWEEN ? AND ?", Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT32))
                        .ok(),

                sql("SELECT ? BETWEEN ? AND ?", 1, Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT32), nullable(NativeTypes.INT32))
                        .ok(),

                sql("SELECT 1 BETWEEN ? AND ?", Unspecified.UNKNOWN, 10L)
                        .parameterTypes(nullable(NativeTypes.INT64), nullable(NativeTypes.INT64))
                        .ok(),

                sql("SELECT ? BETWEEN ? AND ?", Unspecified.UNKNOWN, 1, 10)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT32), nullable(NativeTypes.INT32))
                        .ok(),

                sql("SELECT ? BETWEEN ? AND ?", Unspecified.UNKNOWN, 1, Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT32), nullable(NativeTypes.INT32))
                        .ok(),

                sql("SELECT ? BETWEEN ? AND ?", Unspecified.UNKNOWN, Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter")
        );
    }

    /** Aggregate functions. */
    @TestFactory
    public Stream<DynamicTest> testAggregateFunctions() {
        return Stream.of(
                checkStatement()
                        .sql("SELECT MAX(?)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                checkStatement()
                        .sql("SELECT MIN(?)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                checkStatement()
                        .sql("SELECT AVG(?)", Unspecified.UNKNOWN)
                        .fails("Ambiguous operator AVG(<UNKNOWN>)"),

                checkStatement()
                        .sql("SELECT COUNT(?)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter")
        );
    }

    /** NULLIF function. */
    @TestFactory
    public Stream<DynamicTest> testNullIf() {
        // NULLIF($1, $2) is rewritten as case CASE WHEN $1 = $2 THEN NULL ELSE $1 END
        return Stream.of(
                checkStatement()
                        .sql("SELECT NULLIF(?, ?)", Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .fails("Ambiguous operator <UNKNOWN> = <UNKNOWN>"),

                checkStatement()
                        .sql("SELECT NULLIF(?, 1)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                checkStatement()
                        .sql("SELECT NULLIF(?, ?)", 1, "1")
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.STRING))
                        .fails("Cannot apply '=' to arguments of type '<INTEGER> = <VARCHAR>'"),

                checkStatement()
                        .sql("SELECT NULLIF(?, ?)", 1, 1L)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT64))
                        .project("CASE(=(CAST(?0):BIGINT, ?1), null:INTEGER, ?0)"),

                checkStatement()
                        .sql("SELECT NULLIF(CAST(? AS INTEGER), 1)", Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("CASE(=(?0, 1), null:INTEGER, CAST(?0):INTEGER)")
        );
    }

    /** COALESCE function. */
    @TestFactory
    public Stream<DynamicTest> testCoalesce() {
        // COALESCE($1, $2) is rewritten into:  CASE WHEN $1 IS NOT NULL THEN $1 ELSE $2 END
        return Stream.of(
                checkStatement()
                        .sql("SELECT COALESCE(?)", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("?0"),

                checkStatement()
                        .sql("SELECT COALESCE(?)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                checkStatement()
                        .sql("SELECT COALESCE(?)", Unspecified.UNKNOWN)
                        .fails("Unable to determine type of a dynamic parameter"),

                checkStatement()
                        .sql("SELECT COALESCE(?, ?)", null, null)
                        .parameterTypes(nullable(null), nullable(null))
                        .project("CASE(IS NOT NULL(?0), ?0, ?1)"),

                checkStatement()
                        .sql("SELECT COALESCE(?, 1)", Unspecified.UNKNOWN)
                        .fails("Ambiguous operator <UNKNOWN> IS NOT NULL"),

                checkStatement()
                        .sql("SELECT COALESCE(1, ?)", Unspecified.UNKNOWN)
                        .fails("Illegal mixing of types in CASE or COALESCE statement"),

                checkStatement()
                        .sql("SELECT COALESCE(CAST(? AS INTEGER), 1)", 2)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("CASE(IS NOT NULL(?0), CAST(?0):INTEGER NOT NULL, 1)")
        );
    }

    /** Dynamic params in INSERT statement. */
    @TestFactory
    public Stream<DynamicTest> testInsertDynamicParams() {
        return Stream.of(
                checkStatement()
                        .disableRules(DISABLE_KEY_VALUE_MODIFY_RULES)
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("INSERT INTO t1 VALUES (?)", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("?0"),

                checkStatement()
                        .disableRules(DISABLE_KEY_VALUE_MODIFY_RULES)
                        .table("t1", "c1", NativeTypes.INT64)
                        .sql("INSERT INTO t1 VALUES (?)", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("CAST(?0):BIGINT"),

                checkStatement()
                        .disableRules(DISABLE_KEY_VALUE_MODIFY_RULES)
                        .table("t1", "c1", NativeTypes.INT64)
                        .sql("INSERT INTO t1 VALUES (?)", Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT64))
                        .project("?0"),

                checkStatement()
                        .disableRules(DISABLE_KEY_VALUE_MODIFY_RULES)
                        .table("t1", "c1", NativeTypes.INT64)
                        .sql("INSERT INTO t1 VALUES (?)", new Object[]{null})
                        .parameterTypes(new NativeType[]{null})
                        .project("CAST(?0):BIGINT"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("INSERT INTO t1 VALUES (?), (2), (?)", 1, Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT32))
                        .project("?0"),

                checkStatement()
                        .disableRules(DISABLE_KEY_VALUE_MODIFY_RULES)
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("INSERT INTO t1 VALUES (?), (2), (?)", Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT32))
                        .project("?0"),

                // compatible type

                checkStatement()
                        .disableRules(DISABLE_KEY_VALUE_MODIFY_RULES)
                        .table("t1", "c1", NativeTypes.INT64)
                        .sql("INSERT INTO t1 VALUES (?)", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("CAST(?0):BIGINT"),

                checkStatement()
                        .disableRules(DISABLE_KEY_VALUE_MODIFY_RULES)
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("INSERT INTO t1 VALUES (?)", "10")
                        .parameterTypes(nullable(NativeTypes.STRING))
                        .fails("Cannot assign to target field 'C1' of type INTEGER from source field 'EXPR$0' of type VARCHAR"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32)
                        .sql("INSERT INTO t1 (c1, c2) SELECT c1, ? FROM t2", "10")
                        .parameterTypes(nullable(NativeTypes.STRING))
                        .fails("Cannot assign to target field 'C2' of type INTEGER from source field 'EXPR$1' of type VARCHAR")
        );
    }

    /**
     * UPDATE statement.
     */
    @TestFactory
    public Stream<DynamicTest> testUpdate() {
        return Stream.of(
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("UPDATE t1 SET c1 = ?", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("$t0", "?0"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT64)
                        .sql("UPDATE t1 SET c1 = ?, c2 = ?", 1, Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT64))
                        .project("$t0", "$t1", "?0", "?1"),

                // compatible type
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT64)
                        .sql("UPDATE t1 SET c1 = ?", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("$t0", "CAST(?0):BIGINT"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT64)
                        .sql("UPDATE t1 SET c1 = ?", "10")
                        .parameterTypes(nullable(NativeTypes.STRING))
                        .fails("Cannot assign to target field 'C1' of type BIGINT from source field 'EXPR$0' of type VARCHAR"),

                // null
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("UPDATE t1 SET c1 = ?", new Object[]{null})
                        .parameterTypes(new NativeType[]{null})
                        .project("$t0", "CAST(?0):INTEGER")
        );
    }

    /**
     * NOT MATCHED arm of MERGE statement.
     */
    @TestFactory
    public Stream<DynamicTest> testMergeNotMatched() {
        return Stream.of(
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("$0", "$1", "?0"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .project("$0", "$1", "?0"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, "1")
                        .parameterTypes(nullable(NativeTypes.STRING))
                        .fails("Cannot assign to target field 'C3' of type INTEGER from source field 'EXPR$2' of type VARCHAR"),

                // null

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, new Object[]{null})
                        .parameterTypes(new NativeType[]{null})
                        .project("$0", "$1", "CAST(?0):INTEGER")
        );
    }

    /** Both arms of MERGE statement. */
    @TestFactory
    public Stream<DynamicTest> testMergeFull() {
        return Stream.of(
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT64)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN MATCHED THEN UPDATE SET c2 = ? "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, 1, 2L)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT64))
                        .project("$3", "$4", "?1", "$0", "$1", "$2", "?0"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT64)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN MATCHED THEN UPDATE SET c2 = ? "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, 1, Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.INT64))
                        .project("$3", "$4", "?1", "$0", "$1", "$2", "?0"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN MATCHED THEN UPDATE SET c2 = ? "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, 1)";
                            return sql;
                        }, "1")
                        .parameterTypes(nullable(NativeTypes.STRING))
                        .fails("Cannot assign to target field 'C2' of type INTEGER from source field 'EXPR$0' of type VARCHAR"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN MATCHED THEN UPDATE SET c2 = ? "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, 1, "1")
                        .parameterTypes(nullable(NativeTypes.INT32), nullable(NativeTypes.STRING))
                        .fails("Cannot assign to target field 'C3' of type INTEGER from source field 'EXPR$2' of type VARCHAR")
        );
    }

    /**
     * Dynamic parameters in LIMIT / OFFSET.
     */
    @TestFactory
    public Stream<DynamicTest> testLimitOffset() {
        Consumer<StatementChecker> setup = (checker) -> {
            checker.table("t1", "c1", NativeTypes.INT32);
        };

        return Stream.of(
                checkStatement(setup)
                        .sql("SELECT * FROM t1 LIMIT ?", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .ok(),

                checkStatement(setup)
                        .sql("SELECT * FROM t1 LIMIT ?", Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT64))
                        .ok(),

                checkStatement(setup)
                        .sql("SELECT * FROM t1 LIMIT ?", "a")
                        .fails("Incorrect type of a dynamic parameter. Expected <BIGINT> but got <VARCHAR>"),

                checkStatement(setup)
                        .sql("SELECT * FROM t1 OFFSET ?", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .ok(),

                checkStatement(setup)
                        .sql("SELECT * FROM t1 OFFSET ?", Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.INT64))
                        .ok(),

                checkStatement(setup)
                        .sql("SELECT * FROM t1 OFFSET ?", "a")
                        .fails("Incorrect type of a dynamic parameter. Expected <BIGINT> but got <VARCHAR>"),

                checkStatement(setup)
                        .sql("SELECT * FROM t1 LIMIT ?", (Object) null)
                        .fails("Illegal value of fetch / limit"),

                checkStatement(setup)
                        .sql("SELECT * FROM t1 OFFSET ? ROWS", (Object) null)
                        .fails("Illegal value of offset")
        );
    }

    /**
     * Function calls.
     */
    @TestFactory
    public Stream<DynamicTest> testFunction() {
        return Stream.of(
                checkStatement()
                        .sql("SELECT SUBSTRING(?, 1)", "abc")
                        .parameterTypes(nullable(NativeTypes.STRING))
                        .ok(),

                checkStatement()
                        .sql("SELECT SUBSTRING(?, 1)", Unspecified.UNKNOWN)
                        .fails("Ambiguous operator SUBSTRING(<UNKNOWN>, <INTEGER>)"),

                checkStatement()
                        .sql("SELECT SUBSTRING('aaaa', ?)", Unspecified.UNKNOWN)
                        .fails("Ambiguous operator SUBSTRING(<CHAR(4)>, <UNKNOWN>)"),

                // nested function call - OK
                checkStatement()
                        .sql("SELECT SUBSTRING(SUBSTRING(?, 1), 2)", "123456")
                        .project("SUBSTRING(SUBSTRING(?0, 1), 2)"),

                // nested function call with implicit parameter casting
                checkStatement()
                        .sql("SELECT SUBSTRING(SUBSTRING(?, 1), 2)", 123456)
                        .fails("Cannot apply 'SUBSTRING' to arguments of type 'SUBSTRING(<INTEGER> FROM <INTEGER>)'")
        );
    }

    /** Set operations. */
    @TestFactory
    public Stream<DynamicTest> testSetOps() {
        return Stream.of(
                sql("SELECT 1 UNION SELECT ?", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .ok(),

                sql("SELECT 1 UNION SELECT ?", Unspecified.UNKNOWN)
                        .fails("Type mismatch in column 1 of UNION"),

                sql("SELECT ? UNION SELECT 1", 1)
                        .parameterTypes(nullable(NativeTypes.INT32))
                        .ok(),

                sql("SELECT ? UNION SELECT ?", 1L, 2)
                        .parameterTypes(nullable(NativeTypes.INT64), nullable(NativeTypes.INT32))
                        .ok(),

                sql("SELECT ? UNION SELECT 1", Unspecified.UNKNOWN)
                        .fails("Type mismatch in column 1 of UNION"),

                sql("SELECT ? UNION SELECT ?", 1, Unspecified.UNKNOWN)
                        .fails("Type mismatch in column 1 of UNION")
        );
    }

    /**
     * Custom data types.
     */
    @TestFactory
    public Stream<DynamicTest> testCustomType() {
        // IgniteCustomType: All dynamic parameters belong to the same SqlTypeFamily ANY.
        // Cast operations from/to ANY type considered legal in Calcite.

        Consumer<StatementChecker> setup = (checker) -> {
            checker.table("t1", "uuid_col", NativeTypes.UUID, "str_col", NativeTypes.stringOf(4));
        };

        return Stream.of(
                checkStatement(setup)
                        .sql("SELECT uuid_col = ? FROM t1", "uuid_str")
                        .fails("Cannot apply '=' to arguments of type '<UUID> = <VARCHAR>'"),

                checkStatement(setup)
                        .sql("SELECT ? = uuid_col FROM t1", "uuid_str")
                        .fails("Cannot apply '=' to arguments of type '<VARCHAR> = <UUID>'"),

                // IN

                checkStatement(setup)
                        .sql("SELECT uuid_col IN ('a') FROM t1")
                        .fails("Values passed to IN operator must have compatible types"),

                checkStatement(setup)
                        .sql("SELECT str_col IN ('a'::UUID) FROM t1")
                        .fails("Values passed to IN operator must have compatible types"),

                checkStatement(setup)
                        .sql("SELECT uuid_col IN (?) FROM t1", UUID.randomUUID())
                        .parameterTypes(nullable(NativeTypes.UUID))
                        .project("=($t0, ?0)"),

                checkStatement(setup)
                        .sql("SELECT uuid_col IN (?) FROM t1", Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.UUID))
                        .project("=($t0, ?0)"),

                // CASE

                checkStatement(setup)
                        .sql("SELECT CASE WHEN ? THEN 1 WHEN ? THEN 2 ELSE 3 END", new UUID(0, 1), new UUID(0, 2))
                        .fails("Expected a boolean type"),

                checkStatement(setup)
                        .sql("SELECT CASE RAND_UUID() WHEN ? THEN 1 WHEN ? THEN 2 ELSE 3 END", new UUID(0, 1), 2)
                        .fails("Cannot apply '=' to arguments of type '<UUID> = <INTEGER>'"),

                checkStatement(setup)
                        .sql("SELECT CASE RAND_UUID() WHEN ? THEN 1 WHEN ? THEN 2 ELSE 3 END", new UUID(0, 1), new UUID(0, 2))
                        .parameterTypes(nullable(NativeTypes.UUID), nullable(NativeTypes.UUID))
                        .ok(),

                checkStatement(setup)
                        .sql("SELECT CASE RAND_UUID() WHEN ? THEN 1 WHEN ? THEN 2 ELSE 3 END", new UUID(0, 1), Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.UUID), nullable(NativeTypes.UUID))
                        .ok(),

                checkStatement(setup)
                        .sql("SELECT CASE RAND_UUID() WHEN ? THEN 1 WHEN ? THEN 2 ELSE 3 END", Unspecified.UNKNOWN, Unspecified.UNKNOWN)
                        .parameterTypes(nullable(NativeTypes.UUID), nullable(NativeTypes.UUID))
                        .ok(),

                checkStatement()
                        .sql("SELECT COALESCE(?, 'UUID'::UUID)", new UUID(0, 0))
                        .parameterTypes(nullable(NativeTypes.UUID))
                        .project("CASE(IS NOT NULL(?0), CAST(?0):UUID NOT NULL, CAST(_UTF-8'UUID'):UUID NOT NULL)"),

                checkStatement()
                        .sql("SELECT NULLIF(?, 'UUID'::UUID)", new UUID(0, 0))
                        .parameterTypes(nullable(NativeTypes.UUID))
                        .project("CASE(=(?0, CAST(_UTF-8'UUID'):UUID NOT NULL), null:UUID, ?0)"),

                // Set operations

                checkStatement(setup)
                        .sql("SELECT ? UNION SELECT uuid_col FROM t1", new UUID(0, 0))
                        .ok(),

                checkStatement(setup)
                        .sql("SELECT ? UNION SELECT uuid_col FROM t1", 1)
                        .fails("Type mismatch in column 1 of UNION"),

                checkStatement(setup)
                        .sql("SELECT ? UNION SELECT uuid_col FROM t1", "str")
                        .fails("Type mismatch in column 1 of UNION")
        );
    }

    private static RelDataType nullable(@Nullable NativeType type) {
        IgniteTypeFactory tf = Commons.typeFactory();
        if (type == null) {
            return tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.NULL), true);
        }

        RelDataType relDataType = TypeUtils.native2relationalType(tf, type, true);

        // For var length types inferred without precision.
        if (type instanceof VarlenNativeType) {
            SqlTypeName typeName = relDataType.getSqlTypeName();
            return tf.createTypeWithNullability(tf.createSqlType(typeName), true);
        }

        return relDataType;
    }
}
