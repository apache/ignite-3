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

package org.apache.ignite.table;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class to verify various requirements to Table API.
 */
public class TableAPIConsistencyTest {

    /**
     * Test validates that if method public method requires {@link Transaction} parameter then overloaded
     * method without such parameter should exists.
     */
    @ParameterizedTest
    @ValueSource(classes = {KeyValueView.class, RecordView.class, CriteriaQuerySource.class})
    public void methodsWithOptionalTxShouldHaveSimpleOverload(Class<?> clazz) {
        for (Method mtd : clazz.getDeclaredMethods()) {
            var params = mtd.getParameters();

            int txParIdx = txParamIdx(params);

            if (txParIdx != -1){
                Class<?>[] altParams = Arrays.stream(params)
                        .filter(p -> p != params[txParIdx])
                        .map(Parameter::getType)
                        .toArray(Class[]::new);

                assertDoesNotThrow(
                        () -> clazz.getDeclaredMethod(mtd.getName(), altParams),
                        () -> "Method " + mtd + " should have override without transaction parameter"
                );
            }
        }
    }

    private int txParamIdx(Parameter[] params){
        for (int i = 0; i < params.length; i++) {
            if (params[i].getType() == Transaction.class){
                return i;
            }
        }

        return -1;
    }

}
