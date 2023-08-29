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

import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator;
import org.apache.ignite.internal.sql.engine.util.StatementChecker;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * Test cases for dynamic parameters.
 */
public class DynamicParametersTest extends AbstractPlannerTest {

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
                sql("SELECT 1 + ?", 1).ok(),
                sql("SELECT NULL + ?", 1).project("null:INTEGER"),
                sql("SELECT ? + NULL", 1).project("null:INTEGER"),

                sql("SELECT 1 + ?", "1").fails("Values passed to + operator must have compatible types"),
                sql("SELECT ? + 1", "1").fails("Values passed to + operator must have compatible types"),

                // NULL is allowed in arithmetic expressions
                sql("SELECT ? * 2", new Object[]{null}).ok()
        );
    }

    /** Comparison expressions. */
    @TestFactory
    public Stream<DynamicTest> testCmpExprs() {
        return Stream.of(
                // comparison
                sql("SELECT ? > 1", 1).ok(),

                sql("SELECT ? > 1", "1").fails("Values passed to > operator must have compatible types"),
                sql("SELECT 1 > ?", "1").fails("Values passed to > operator must have compatible types"),

                sql("SELECT ? > NULL", 1).project("null:BOOLEAN"),
                sql("SELECT NULL = ?", 1).project("null:BOOLEAN"),
                sql("SELECT ? = NULL", 1).project("null:BOOLEAN"),

                // NULL is allowed in comparison
                sql("SELECT ? = ?", null, null).project("=(?0, ?1)")
        );
    }

    /**
     * IN expression.
     */
    @TestFactory
    public Stream<DynamicTest> testInExpression() {
        String error =
                "Values passed to IN operator must have compatible types. Dynamic parameter requires adding explicit type cast";
        return Stream.of(
                sql("SELECT ? IN ('1', '2')", 1).project("OR(=(?0, 1), =(?0, 2))"),
                sql("SELECT ? IN (1, 2)", "1").fails(error),
                sql("SELECT ? IN ('1', 2)", 2).project("OR(=(?0, 1), =(?0, 2))"),

                sql("SELECT (?,?) IN ((1,2))", 1, 2).project("AND(=(?0, 1), =(?1, 2))"),

                sql("SELECT (?,?) IN ((1,2))", "1", "2").fails(error),
                sql("SELECT (?,?) IN (('1', 2))", 1, "2").fails(error),
                sql("SELECT (?,?) IN ((1, '2'))", "1", "2").fails(error)
        );
    }

    /** CASE expression. */
    @TestFactory
    public Stream<DynamicTest> testCaseWhenExpression() {
        return Stream.of(
                sql("SELECT CASE ? = ? WHEN true THEN 1 ELSE 2 END", 1, 1).ok(),

                sql("SELECT CASE WHEN ? = '1' THEN ? ELSE ? END", "1", 2, 2.5)
                        .project("CASE(=(?0, _UTF-8'1'), CAST(?1):DOUBLE, ?2)"),

                sql("SELECT CASE ? = ? WHEN true THEN 1 ELSE 2 END", 1, "1")
                        .fails("Values passed to = operator must have compatible types"),

                sql("SELECT CASE WHEN ? = '1' THEN ? ELSE ? END", "1", "2", 2.5)
                        .fails("Illegal mixing of types in CASE or COALESCE statement"),

                sql("SELECT CASE WHEN ? THEN 1 WHEN ? THEN 2 ELSE 3 END", 1, 2).fails("Expected a boolean type")
        );
    }

    /**
     * Dynamic parameters: int_col = ?:str behave the same way as str_col = ?:int.
     * See a comment in IgniteTypeCoercion doBinaryComparisonCoercion.
     */
    @TestFactory
    public Stream<DynamicTest> testWherePredicate() {
        return Stream.of(
                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("SELECT * FROM t1 WHERE int_col = ?", "1").fails("Values passed to = operator must have compatible types"),

                checkStatement()
                        .table("t1", "str_col", NativeTypes.STRING)
                        .sql("SELECT * FROM t1 WHERE str_col = ?", 1).fails("Values passed to = operator must have compatible types")
        );
    }


    /** Dynamic params in INSERT statement. */
    @TestFactory
    public Stream<DynamicTest> testInsertDynamicParams() {
        return Stream.of(
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("INSERT INTO t1 VALUES (?)", 1)
                        .project("?0"),

                // compatible type

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT64)
                        .sql("INSERT INTO t1 VALUES (?)", 1)
                        .project("CAST(?0):BIGINT"),


                // Incompatible types in dynamic params

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("INSERT INTO t1 VALUES (?)", "10")
                        .fails("Values passed to VALUES operator must have compatible types"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32)
                        .sql("INSERT INTO t1 (c1, c2) SELECT c1, ? FROM t2", "10")
                        .fails("Values passed to VALUES operator must have compatible types")
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
                        .project("$t0", "?0"),

                // compatible type
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT64)
                        .sql("UPDATE t1 SET c1 = ?", 1)
                        .project("$t0", "CAST(?0):BIGINT"),

                // null
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("UPDATE t1 SET c1 = ?", new Object[]{null})
                        .project("$t0", "CAST(?0):INTEGER"),

                // Incompatible types in dynamic params

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("UPDATE t1 SET c1 = ?", "10")
                        .fails("Assignment from VARCHAR to INTEGER can not be performed")
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
                        .project("$0", "$1", "?0"),

                // null

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, new Object[]{null})
                        .project("$0", "$1", "CAST(?0):INTEGER"),

                // incompatible types

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, "1")
                        .fails("Values passed to VALUES operator must have compatible types")
        );
    }

    /** Both arms of MERGE statement. */
    @TestFactory
    public Stream<DynamicTest> testMergeFull() {
        return Stream.of(
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN MATCHED THEN UPDATE SET c2 = ? "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, 1, 2)
                        .project("$3", "$4", "?1", "$0", "$1", "$2", "?0"),

                // Incompatible types

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN MATCHED THEN UPDATE SET c2 = ? "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, 1)";
                            return sql;
                        }, "1")
                        .fails("Assignment from VARCHAR to INTEGER can not be performed"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN MATCHED THEN UPDATE SET c2 = ? "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, ?)";
                            return sql;
                        }, 1, "1")
                        .fails("Values passed to VALUES operator must have compatible types")
        );
    }

    /**
     * Function calls.
     */
    @TestFactory
    public Stream<DynamicTest> testFunction() {
        return Stream.of(
                // nested function call - OK
                checkStatement()
                        .sql("SELECT SUBSTRING(SUBSTRING(?, 1), 2)", "123456")
                        .project("SUBSTRING(SUBSTRING(?0, 1), 2)"),

                // nested function call - invalid dynamic parameter
                checkStatement()
                        .sql("SELECT SUBSTRING(SUBSTRING(?, 1), 2)", 123456)
                        .fails("Values passed to SUBSTRING operator must have compatible types")
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
                        .fails("Values passed to = operator must have compatible types"),

                checkStatement(setup)
                        .sql("SELECT ? = uuid_col FROM t1", "uuid_str")
                        .fails("Values passed to = operator must have compatible types"),

                // IN

                checkStatement(setup)
                        .sql("SELECT uuid_col IN ('a') FROM t1")
                        .project("=($t0, CAST(_UTF-8'a'):UUID NOT NULL)"),

                checkStatement(setup)
                        .sql("SELECT str_col IN ('a'::UUID) FROM t1")
                        .project("=(CAST($t0):UUID, CAST(_UTF-8'a'):UUID NOT NULL)"),

                // CASE

                checkStatement(setup)
                        .sql("SELECT CASE WHEN ? THEN 1 WHEN ? THEN 2 ELSE 3 END", new UUID(0, 1), new UUID(0, 2))
                        .fails("Expected a boolean type"),

                checkStatement(setup)
                        .sql("SELECT CASE RAND_UUID() WHEN ? THEN 1 WHEN ? THEN 2 ELSE 3 END", new UUID(0, 1), 2)
                        .fails("Values passed to = operator must have compatible types"),

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
}
