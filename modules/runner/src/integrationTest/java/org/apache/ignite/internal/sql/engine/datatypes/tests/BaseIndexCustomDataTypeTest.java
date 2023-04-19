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

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests with indexed custom data type column.
 *
 * @param <T> A storage type for a custom data type.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-19128")
public abstract class BaseIndexCustomDataTypeTest<T extends Comparable<T>> extends BaseCustomDataTypeTest<T> {

    @BeforeAll
    public void addIndexSimpleIndex() throws InterruptedException {
        runSql("create index t_test_key_idx on t (test_key)");

        waitForIndex("t_test_key_idx");
    }

    @BeforeEach
    public void insertData() {
        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");
    }

    /**
     * Key lookup.
     */
    @Test
    public void testKeyLookUp() {
        T value1 = values.get(0);

        checkQuery("SELECT * FROM t WHERE test_key = $0")
                .returns(1, value1)
                .check();

        checkQuery("SELECT * FROM t WHERE test_key iS NOT DISTINCT FROM $0")
                .returns(1, value1)
                .check();
    }

    /**
     * {@code not EQ} lookup.
     */
    @Test
    public void testNotEqLookUp() {
        T value2 = values.get(1);
        T value3 = values.get(2);

        checkQuery("SELECT * FROM t WHERE test_key != $0 ORDER BY id")
                .returns(2, value2)
                .returns(3, value3)
                .check();

        checkQuery("SELECT * FROM t WHERE test_key IS DISTINCT FROM $0")
                .returns(2, value2)
                .returns(3, value3)
                .check();
    }

    /**
     * Range lookup with one bound.
     */
    @Test
    public void testRangeLookUpSingleBound() {
        T value1 = values.get(0);
        T value2 = values.get(1);
        T value3 = values.get(2);

        checkQuery("SELECT * FROM t WHERE test_key > $0 ORDER BY id")
                .returns(2, value2)
                .returns(3, value3)
                .check();

        checkQuery("SELECT * FROM t WHERE test_key >= $1 ORDER BY id")
                .returns(2, value2)
                .returns(3, value3)
                .check();

        checkQuery("SELECT * FROM t WHERE test_key < $2 ORDER BY id")
                .returns(1, value1)
                .returns(2, value2)
                .check();

        checkQuery("SELECT * FROM t WHERE test_key <= $2 ORDER BY id")
                .returns(1, value1)
                .returns(2, value2)
                .returns(3, value3)
                .check();
    }


    /**
     * Range lookup with two bounds.
     */
    @Test
    public void testRangeLookUpTwoBounds() {
        T value2 = values.get(1);
        T value3 = values.get(2);

        checkQuery("SELECT * FROM t WHERE test_key > $0 AND test_key < $2 ORDER BY id")
                .returns(2, value2)
                .check();

        checkQuery("SELECT * FROM t WHERE test_key >= $1 AND test_key <= $2 ORDER BY id")
                .returns(2, value2)
                .returns(3, value3)
                .check();
    }

    /**
     * {@code BETWEEN} operator in index.
     */
    @Test
    public void testRangeLookUpBetween() {
        Assumptions.assumeTrue(testTypeSpec.hasLiterals(), "BETWEEN only works for types that has literals");

        T value2 = values.get(1);

        checkQuery("SELECT * FROM t WHERE test_key > BETWEEN $0_lit AND $2_lit ORDER BY id")
                .returns(2, value2)
                .check();
    }

    /**
     * Out of range lookup.
     */
    @Test
    public void testOutOfRangeLookUp() {
        checkQuery("SELECT * FROM t WHERE test_key < $0")
                .returnNothing()
                .check();
    }

    /**
     * {@code IN} operator.
     */
    @Test
    public void testInLookUp() {
        T value1 = values.get(0);
        T value3 = values.get(2);

        checkQuery("SELECT * FROM t WHERE test_key IN ($0, $2) ORDER BY id")
                .returns(1, value1)
                .returns(3, value3)
                .check();
    }

    /**
     * {@code NOT IN} operator.
     */
    @Test
    public void testNotInLookUp() {
        T value2 = values.get(1);

        checkQuery("SELECT * FROM t WHERE test_key NOT IN ($0, $2) ORDER BY id")
                .returns(2, value2)
                .check();
    }

    /**
     * Compound index: primary key + custom data type.
     */
    @ParameterizedTest
    @MethodSource("compoundIndex")
    public void testCompoundIndex(TestTypeArguments<T> arguments) throws InterruptedException {
        sql("drop index if exists t_test_key_pk_idx");
        sql("create index if not exists t_test_key_pk_idx on t (id, test_key)");

        waitForIndex("t_test_key_pk_idx");

        runSql("insert into t values(100, $0)");

        String query = format("select id, test_key from t where test_key = {} and id = 100", arguments.valueExpr(0));
        checkQuery(query)
                .returns(100, arguments.value(0))
                .check();
    }

    /**
     * Dynamic parameter in index search.
     */
    @Test
    public void testIndexDynParam() {
        assertQuery("SELECT * FROM t WHERE test_key=?")
                .withParams(values.get(0))
                .check();
    }

    private Stream<TestTypeArguments<T>> compoundIndex() {
        return TestTypeArguments.unary(testTypeSpec, dataSamples, values.get(0));
    }
}
