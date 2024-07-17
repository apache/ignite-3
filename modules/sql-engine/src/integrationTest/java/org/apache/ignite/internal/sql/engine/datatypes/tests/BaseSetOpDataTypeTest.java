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

import org.junit.jupiter.api.Test;

/**
 * Test cases for set operators for a data type.
 *
 * @param <T> A storage type of a data type.
 */
public abstract class BaseSetOpDataTypeTest<T extends Comparable<T>> extends BaseDataTypeTest<T> {

    /**
     * {@code UNION} operator.
     */
    @Test
    public void testUnion() {
        T value1 = values.get(0);
        T value2 = values.get(1);
        T value3 = values.get(2);

        insertValues();

        checkQuery("SELECT id, test_key FROM (SELECT id, test_key FROM t UNION SELECT id, test_key FROM t) tmp ORDER BY id ASC")
                .returns(1, value1)
                .returns(2, value2)
                .returns(3, value3)
                .check();
    }

    /**
     * {@code UNION ALL} operator.
     */
    @Test
    public void testUnionAll() {
        T value1 = values.get(0);
        T value2 = values.get(1);
        T value3 = values.get(2);

        insertValues();

        checkQuery("SELECT id, test_key FROM (SELECT id, test_key FROM t UNION ALL SELECT id, test_key FROM t) tmp ORDER BY id ASC")
                .returns(1, value1)
                .returns(1, value1)
                .returns(2, value2)
                .returns(2, value2)
                .returns(3, value3)
                .returns(3, value3)
                .check();
    }

    /**
     * {@code INTERSECT} operator.
     */
    @Test
    public void testIntersect() {
        T value1 = values.get(0);
        T value3 = values.get(2);

        insertValues();

        String intercept = "SELECT id, test_key FROM ("
                + "SELECT id, test_key FROM t INTERSECT SELECT id, test_key FROM t WHERE id IN (1, 3)"
                + ") tmp ORDER BY id ASC";

        checkQuery(intercept)
                .returns(1, value1)
                .returns(3, value3)
                .check();
    }

    /**
     * {@code INTERSECT} and {@code INTERSECT ALL} operators.
     */
    @Test
    public void testIntersectAll() {
        T value1 = values.get(0);
        T value3 = values.get(2);

        insertValues();

        String interceptAll = "SELECT id, test_key FROM ("
                + "SELECT id, test_key FROM t INTERSECT ALL SELECT id, test_key FROM t WHERE id IN (1, 1, 3, 3)"
                + ") tmp ORDER BY id ASC";

        checkQuery(interceptAll)
                .returns(1, value1)
                .returns(3, value3)
                .check();
    }

    /**
     * {@code EXCEPT} operator.
     */
    @Test
    public void testExcept() {
        T value2 = values.get(1);

        insertValues();

        String except = "SELECT id, test_key FROM ("
                + "SELECT id, test_key FROM t EXCEPT SELECT id, test_key FROM t WHERE id IN (1, 3)"
                + ") tmp ORDER BY id ASC";

        checkQuery(except)
                .returns(2, value2)
                .check();

        String exceptAll = "SELECT id, test_key FROM ("
                + "SELECT id, test_key FROM t EXCEPT ALL SELECT id, test_key FROM t WHERE id IN (1, 1, 3, 3)"
                + ") tmp ORDER BY id ASC";

        checkQuery(exceptAll)
                .returns(2, value2)
                .check();
    }

    /**
     * {@code EXCEPT ALL} operator.
     */
    @Test
    public void testExceptAll() {
        T value2 = values.get(1);

        insertValues();

        String exceptAll = "SELECT id, test_key FROM ("
                + "SELECT id, test_key FROM t EXCEPT ALL SELECT id, test_key FROM t WHERE id IN (1, 1, 3, 3)"
                + ") tmp ORDER BY id ASC";

        checkQuery(exceptAll)
                .returns(2, value2)
                .check();
    }
}
