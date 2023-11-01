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

package org.apache.ignite.internal.sql.engine.util;

import java.util.Arrays;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactory.RowHashFunction;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactoryImpl.SimpleHashFunction;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactoryImpl.TypesAwareHashFunction;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Basic tests for hash functions, which can be created using {@link HashFunctionFactory}.
 */
class HashFunctionsTest {
    /**
     * Ensures that the hash function accepts {@code null} values in row.
     */
    @ParameterizedTest
    @EnumSource
    public void checkNull(HashFunc func) {
        func.hash(new Object[]{null}, 0);
    }

    /**
     * Ensures that the hash function accepts empty row.
     */
    @ParameterizedTest
    @EnumSource
    public void checkEmpty(HashFunc func) {
        Assertions.assertEquals(0, func.hash(new Object[]{}));
    }

    /**
     * Ensures that the hash is computed according to the specified field ordinals
     * and that the order of the ordinal numbers matters.
     */
    @ParameterizedTest
    @EnumSource
    public void checkOrder(HashFunc func) {
        Object[] row = {100, 200, 100};

        Assertions.assertNotEquals(
                func.hash(row, 0, 1),
                func.hash(row, 1, 0)
        );

        Assertions.assertEquals(
                func.hash(row, 0, 2),
                func.hash(row, 2, 0)
        );
    }

    enum HashFunc {
        SIMPLE,
        TYPE_AWARE;

        /**
         * Compute hash.
         *
         * @param row Row to process.
         * @param keys Ordinal numbers of key fields.
         * @return Composite hash for the specified fields.
         */
        int hash(Object[] row, int... keys) {
            RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;
            RowHashFunction<Object[]> func;

            switch (this) {
                case SIMPLE:
                    func = new SimpleHashFunction<>(keys, rowHandler);

                    break;

                case TYPE_AWARE:
                    NativeType[] fieldTypes = new NativeType[keys.length];

                    Arrays.fill(fieldTypes, NativeTypes.INT32);

                    func = new TypesAwareHashFunction<>(keys, fieldTypes, rowHandler);

                    break;

                default:
                    throw new UnsupportedOperationException();
            }

            return func.hashOf(row);
        }
    }
}
