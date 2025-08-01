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

package org.apache.ignite.internal.table;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Base class for {@link KeyValueView key-value view} API integration tests.
 */
abstract class ItKeyValueViewApiBaseTest extends ItTableViewApiUnifiedBaseTest {
    /**
     * Returns a factory for creating a set of test cases for the specific key-value view.
     */
    abstract TestCaseFactory getFactory(String viewName);

    List<Arguments> generateKeyValueTestArguments(String tableName, Class<?> keyClass, Class<?> valueClass) {
        return generateKeyValueTestArguments(tableName, keyClass, valueClass, "");
    }

    List<Arguments> generateKeyValueTestArguments(String tableName, Class<?> keyClass, Class<?> valueClass, String nameSuffix) {
        TestCaseFactory caseFactory = getFactory(tableName);

        List<Arguments> arguments = new ArrayList<>(TestCaseType.values().length);

        for (TestCaseType type : TestCaseType.values()) {
            arguments.add(Arguments.of(Named.of(
                    type.description() + nameSuffix,
                    caseFactory.create(type, keyClass, valueClass)
            )));
        }

        return arguments;
    }

    abstract static class TestCaseFactory {
        final String tableName;

        TestCaseFactory(String tableName) {
            this.tableName = tableName;
        }

        <K, V> BaseTestCase<K, V> create(TestCaseType type, Class<K> keyClass, Class<V> valueClass) {
            switch (type) {
                case EMBEDDED:
                    return create(false, false, keyClass, valueClass);
                case EMBEDDED_ASYNC:
                    return create(true, false, keyClass, valueClass);
                case THIN:
                    return create(false, true, keyClass, valueClass);
                case THIN_ASYNC:
                    return create(true, true, keyClass, valueClass);
                default:
                    throw new IllegalArgumentException("Unknown test case type: " + type);
            }
        }

        abstract <K, V> BaseTestCase<K, V> create(boolean async, boolean thin, Class<K> keyClass, Class<V> valueClass);
    }

    @SuppressWarnings("MethodMayBeStatic")
    static class BaseTestCase<K, V> {
        final boolean async;
        final boolean thin;
        final KeyValueView<K, V> view;

        BaseTestCase(boolean async, boolean thin, KeyValueView<K, V> view) {
            this.async = async;
            this.thin = thin;
            this.view = view;
        }

        KeyValueView<K, V> view() {
            return view;
        }

        void checkNullKeyError(Executable run) {
            checkNpeMessage(run, "key");
        }

        void checkNullKeysError(Executable run) {
            checkNpeMessage(run, "keys");
        }

        void checkNullPairsError(Executable run) {
            checkNpeMessage(run, "pairs");
        }

        @SuppressWarnings("ThrowableNotThrown")
        static void checkNpeMessage(Executable run, String message) {
            IgniteTestUtils.assertThrows(NullPointerException.class, run, message);
        }
    }
}
