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
import org.apache.ignite.table.RecordView;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Base class for record view API integration tests.
 */
abstract class ItRecordViewApiBaseTest extends ItViewsApiUnifiedTest {
    abstract TestCaseFactory getFactory(String tableName);

    <T> List<Arguments> generateRecordViewTestArguments(String tableName, Class<T> recordClass) {
        TestCaseFactory caseFactory = getFactory(tableName);

        List<Arguments> arguments = new ArrayList<>(TestCaseType.values().length);

        for (TestCaseType type : TestCaseType.values()) {
            arguments.add(Arguments.of(Named.of(
                    type.description(),
                    caseFactory.create(type, recordClass)
            )));
        }

        return arguments;
    }

    abstract static class TestCaseFactory {
        final String tableName;

        TestCaseFactory(String tableName) {
            this.tableName = tableName;
        }

        <V> TestCase<V> create(TestCaseType type, Class<V> recordClass) {
            switch (type) {
                case EMBEDDED:
                    return create(false, false, recordClass);
                case EMBEDDED_ASYNC:
                    return create(true, false, recordClass);
                case THIN:
                    return create(false, true, recordClass);
                case THIN_ASYNC:
                    return create(true, true, recordClass);
                default:
                    throw new IllegalArgumentException("Unknown test case type: " + type);
            }
        }

        abstract <V> TestCase<V> create(boolean async, boolean thin, Class<V> recordClass);
    }

    static class TestCase<V> {
        final boolean async;
        final boolean thin;
        final RecordView<V> view;

        TestCase(boolean async, boolean thin, RecordView<V> view) {
            this.async = async;
            this.thin = thin;
            this.view = view;
        }

        RecordView<V> view() {
            return view;
        }
    }
}
