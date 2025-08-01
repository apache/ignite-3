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

    /** {@code DELETE} by key. */
    @Test
    public void testDelete() {
        Object value = testTypeSpec.unwrapIfNecessary(values.get(0));

        runSql("INSERT INTO t VALUES (1, $0)");
        runSql("INSERT INTO t VALUES (2, $1)");
        runSql("INSERT INTO t VALUES (3, $2)");

        runSql("DELETE FROM t WHERE test_key=?", value);

        checkQuery("SELECT id FROM t").returns(2).returns(3).check();
    }

    @Test
    public void testInsertDefaultLiteral() {
        runSql("CREATE TABLE test_table (id INT PRIMARY KEY, val <type> DEFAULT $0)");
        runSql("INSERT INTO test_table(id) VALUES (0)");

        assertQuery("SELECT val FROM test_table WHERE id = ?")
                .withParam(0)
                .returns(values.get(0))
                .check();
    }

    /** {@code UPDATE} from a dynamic parameter. */
    @Test
    public void testUpdateFromDynamicParam() {
        runSql("INSERT INTO t VALUES (1, ?)", testTypeSpec.unwrapIfNecessary(dataSamples.min()));

        Object max = testTypeSpec.unwrapIfNecessary(dataSamples.max());

        checkQuery("UPDATE t SET test_key = ? WHERE id=1")
                .withParams(max)
                .returns(1L)
                .check();

        checkQuery("SELECT test_key FROM t WHERE id=1")
                .returns(max)
                .check();
    }

    private Stream<TestTypeArguments<T>> dml() {
        return TestTypeArguments.unary(testTypeSpec, dataSamples, values.get(0));
    }
}
