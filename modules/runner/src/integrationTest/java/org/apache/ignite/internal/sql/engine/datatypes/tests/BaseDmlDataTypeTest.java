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

import static org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs.VARBINARY_TYPE;
import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of test cases for DML operations.
 *
 * @param <T> A storage type of a data type.
 */
public abstract class BaseDmlDataTypeTest<T extends Comparable<T>> extends BaseDataTypeTest<T> {

    /** {@code INSERT}. */
    @ParameterizedTest
    @MethodSource("dml")
    public void testInsert(TestTypeArguments<T> arguments) {
        runSql(format("INSERT INTO t VALUES (1, {})", arguments.valueExpr(0)));

        checkQuery("SELECT test_key FROM t WHERE id=1").returns(arguments.value(0)).check();
    }

    /** {@code INSERT} with dynamic parameters is not allowed for compatible types. */
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testInsertFromDynamicParameterFromConvertible(TestTypeArguments<T> arguments) {
        // TODO: fix in scope of https://issues.apache.org/jira/browse/IGNITE-20226
        if (testTypeSpec == VARBINARY_TYPE) {
            return;
        }

        var t = assertThrows(IgniteException.class, () -> {
            runSql("INSERT INTO t VALUES (1, ?)", arguments.argValue(0));
        });

        assertThat(t.getMessage(), containsString("Values passed to VALUES operator must have compatible types"));
    }

    /** {@code DELETE} by key. */
    @Test
    public void testDelete() {
        T value1 = values.get(0);

        runSql("INSERT INTO t VALUES (1, $0)");
        runSql("INSERT INTO t VALUES (2, $1)");
        runSql("INSERT INTO t VALUES (3, $2)");

        runSql("DELETE FROM t WHERE test_key=?", value1);

        checkQuery("SELECT id FROM t").returns(2).returns(3).check();
    }

    /** {@code UPDATE} from a literal of a compatible type. */
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testUpdateFromLiteral(TestTypeArguments<T> arguments) {
        String insert = format("INSERT INTO t VALUES (1, {})", arguments.valueExpr(0));
        runSql(insert);

        checkQuery(format("UPDATE t SET test_key = {} WHERE id=1", arguments.valueExpr(0)))
                .returns(1L)
                .check();

        checkQuery("SELECT test_key FROM t WHERE id=1")
                .returns(arguments.value(0))
                .check();
    }

    /** {@code UPDATE} from a dynamic parameter. */
    @Test
    public void testUpdateFromDynamicParam() {
        runSql("INSERT INTO t VALUES (1, ?)", dataSamples.min());

        checkQuery("UPDATE t SET test_key = ? WHERE id=1")
                .withParams(dataSamples.max())
                .returns(1L)
                .check();

        checkQuery("SELECT test_key FROM t WHERE id=1")
                .returns(dataSamples.max())
                .check();
    }

    /** {@code UPDATE} is not allowed for dynamic parameter of compatible type. */
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testUpdateFromDynamicParameterFromConvertible(TestTypeArguments<T> arguments) {
        // TODO: fix in scope of https://issues.apache.org/jira/browse/IGNITE-20226
        if (testTypeSpec == VARBINARY_TYPE) {
            return;
        }

        String insert = format("INSERT INTO t VALUES (1, {})", arguments.valueExpr(0));
        runSql(insert);

        var t = assertThrows(IgniteException.class, () -> {
            checkQuery("UPDATE t SET test_key = ? WHERE id=1")
                    .withParams(arguments.argValue(0))
                    .returns(1L)
                    .check();
        });

        String error = format("Dynamic parameter requires adding explicit type cast",
                testTypeSpec.typeName());

        assertThat(t.getMessage(), containsString(error));
    }

    /** Type mismatch in {@code INSERT}s {@code VALUES}.*/
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testDisallowMismatchTypesOnInsert(TestTypeArguments<T> arguments) {
        // TODO: fix in scope of https://issues.apache.org/jira/browse/IGNITE-20226
        if (testTypeSpec == VARBINARY_TYPE) {
            return;
        }

        var query = format("INSERT INTO t (id, test_key) VALUES (10, null), (20, {})", arguments.valueExpr(0));
        var t = assertThrows(IgniteException.class, () -> runSql(query));

        assertThat(t.getMessage(), containsString("Values passed to VALUES operator must have compatible types"));
    }

    /**
     * Type mismatch in {@code INSERT}s {@code VALUES} with dynamic parameters.
     */
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testDisallowMismatchTypesOnInsertDynamicParam(TestTypeArguments<T> arguments) {
        // TODO: fix in scope of https://issues.apache.org/jira/browse/IGNITE-20226
        if (testTypeSpec == VARBINARY_TYPE) {
            return;
        }

        Object value1 = arguments.argValue(0);

        var query = "INSERT INTO t (id, test_key) VALUES (1, null), (2, ?)";
        var t = assertThrows(IgniteException.class, () -> runSql(query, value1));
        assertThat(t.getMessage(), containsString("Values passed to VALUES operator must have compatible types"));
    }

    /**
     * Type mismatch in {@code INSERT}s {@code VALUES} with dynamic parameters.
     */
    @ParameterizedTest
    @MethodSource("convertedFrom")
    public void testDisallowMismatchTypesOnInsertDynamicParam2(TestTypeArguments<T> arguments) {
        Object value1 = arguments.argValue(0);

        var query = "INSERT INTO t (id, test_key) VALUES (1, 'str'), (2, ?)";
        var t = assertThrows(IgniteException.class, () -> runSql(query, value1));
        assertThat(t.getMessage(), containsString("Values passed to VALUES operator must have compatible types"));
    }

    private Stream<TestTypeArguments<T>> dml() {
        return TestTypeArguments.unary(testTypeSpec, dataSamples, values.get(0));
    }
}
