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

package org.apache.ignite.internal.sql.engine.datatypes.tests;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test cases for {@code JOIN} operator.
 *
 * @param <T> A storage type of a data type.
 */
public abstract class BaseJoinDataTypeTest<T extends Comparable<T>> extends BaseDataTypeTest<T> {

    /** Creates join tables. */
    @BeforeAll
    public void createJoinTables() {
        Stream<TestTypeArguments<T>> args = TestTypeArguments.unary(testTypeSpec, dataSamples, dataSamples.min());
        args.forEach(arg -> {
            String typeName = arg.typeName(0);
            String createTable = format("create table t_join_{}(id integer primary key, test_key {})", typeName, typeName);
            runSql(createTable);
        });
    }

    @AfterEach
    public void cleanJoinTables() {
        Stream<TestTypeArguments<T>> args = TestTypeArguments.unary(testTypeSpec, dataSamples, dataSamples.min());
        args.forEach(arg -> {
            String typeName = arg.typeName(0);
            String delete = format("DELETE FROM t_join_{}", typeName);
            runSql(delete);
        });
    }

    /** Equijoins. Joins with equality predicate in condition. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("equiJoin")
    public void testEquiJoin(String name, TestTypeArguments<T> arguments, String joinType) {
        String insert = format("INSERT INTO t_join_{} (id, test_key) VALUES(1, {})", arguments.typeName(0), arguments.valueExpr(0));
        runSql(insert);

        String join = format("SELECT * FROM t {} JOIN t_join_{} ON t.test_key = t_join_{}.test_key",
                joinType, arguments.typeName(0), arguments.typeName(0));
        checkQuery(join).check();
    }

    private Stream<Arguments> equiJoin() {
        Stream<TestTypeArguments<T>> unary = TestTypeArguments.unary(testTypeSpec, dataSamples, dataSamples.min());

        return unary.map(arg -> arg.withLabel(testTypeSpec.typeName() + " " + arg.typeName(0))).flatMap(arg -> {
            return Stream.of(
                    // Empty
                    Arguments.of("JOIN " + arg, arg, " "),
                    Arguments.of("INNER JOIN " + arg, arg, "INNER"),
                    Arguments.of("LEFT JOIN " + arg, arg, "LEFT"),
                    Arguments.of("RIGHT JOIN " + arg, arg, "RIGHT"),
                    Arguments.of("FULL JOIN " + arg,  arg, "FULL"));
        });
    }

    /** Non equijoins. Joins with non equality predicate in condition. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nonEquiJoin")
    public void testNonEquijoin(String name, TestTypeArguments<T> arguments, String joinExpr) {
        String insert1 = format("INSERT INTO t (id, test_key) VALUES(1, {})", arguments.valueExpr(0));
        runSql(insert1);

        String insert2 = format("INSERT INTO t_join_{} (id, test_key) VALUES(2, {})", arguments.typeName(1), arguments.valueExpr(1));
        runSql(insert2);

        String insert3 = format("INSERT INTO t_join_{} (id, test_key) VALUES(3, {})", arguments.typeName(2), arguments.valueExpr(2));
        runSql(insert3);

        String joinCondition = format(joinExpr, arguments.typeName(2));
        String join = format("SELECT * FROM t {} t.test_key > t_join_{}.test_key", joinCondition, arguments.typeName(2));
        checkQuery(join).check();
    }

    /** Antijoin. Join with {@code not equals} in condition. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nonEquiJoin")
    public void testAntiJoin(String name, TestTypeArguments<T> arguments, String joinExpr) {
        String insert1 = format("INSERT INTO t (id, test_key) VALUES(1, {})", arguments.valueExpr(0));
        runSql(insert1);

        String insert2 = format("INSERT INTO t_join_{} (id, test_key) VALUES(2, {})", arguments.typeName(1), arguments.valueExpr(1));
        runSql(insert2);

        String insert3 = format("INSERT INTO t_join_{} (id, test_key) VALUES(3, {})", arguments.typeName(2), arguments.valueExpr(2));
        runSql(insert3);

        String joinCondition = format(joinExpr, arguments.typeName(2));
        String join = format("SELECT * FROM t {} t.test_key != t_join_{}.test_key", joinCondition, arguments.typeName(2));
        checkQuery(join).check();
    }

    private Stream<Arguments> nonEquiJoin() {
        Stream<TestTypeArguments<T>> args = TestTypeArguments.nary(testTypeSpec, dataSamples, values.get(0), values.get(1), values.get(2));

        return args.map(arg -> arg.withLabel(arg.typeName(0) + " " + arg.typeName(1))).flatMap(arg -> {
            return Stream.of(
                    Arguments.of("JOIN " + arg, arg, "JOIN t_join_{} ON "),
                    Arguments.of("INNER JOIN " + arg, arg, "INNER JOIN t_join_{} ON "),
                    Arguments.of("LEFT JOIN " + arg, arg, "LEFT JOIN t_join_{} ON "),
                    Arguments.of("RIGHT JOIN " + arg, arg, "RIGHT JOIN t_join_{} ON "),
                    Arguments.of("FULL JOIN " + arg, arg, "FULL JOIN t_join_{} ON "),
                    Arguments.of("CARTESIAN PRODUCT", arg, ", t_join_{} WHERE "));
        });
    }
}
