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

package org.apache.ignite.internal.schema.marshaller;

import static org.apache.ignite.internal.schema.marshaller.Records.schema;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.same;

import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;

class AssertMarshaller {
    private AssertMarshaller() {}

    private static final MarshallerFactory factory = new ReflectionMarshallerFactory();

    static <T> void assertMarshaller(T expected) {
        RecordMarshaller<T> m = (RecordMarshaller<T>) factory.create(schema, expected.getClass());
        Row row = Row.wrapBinaryRow(schema, m.marshal(expected));
        T actual = m.unmarshal(row);

        assertThat(actual, not(same(expected)));
        assertThat(actual, equalTo(expected));
    }

    static <K, V> void assertMarshaller(K expectedKey, V expectedVal) {
        KvMarshaller<K, V> m = (KvMarshaller<K, V>) factory.create(schema, expectedKey.getClass(), expectedVal.getClass());
        Row row = Row.wrapBinaryRow(schema, m.marshal(expectedKey, expectedVal));
        K actualKey = m.unmarshalKey(row);
        V actualVal = m.unmarshalValue(row);

        assertThat(actualKey, not(same(expectedKey)));
        assertThat(actualKey, equalTo(expectedKey));

        assertThat(actualVal, not(same(expectedVal)));
        assertThat(actualVal, equalTo(expectedVal));
    }

    static <T, E extends Throwable> void assertMarshallerThrows(Class<E> expectedType, String msgSubstring, T expected) {
        IgniteTestUtils.assertThrows(expectedType, () -> assertMarshaller(expected), msgSubstring);
    }

    static <K, V, E extends Throwable> void assertMarshallerThrows(
            Class<E> expectedType,
            String msgSubstring,
            K expectedKey,
            V expectedVal
    ) {
        IgniteTestUtils.assertThrows(expectedType, () -> assertMarshaller(expectedKey, expectedVal), msgSubstring);
    }


    static <T> void assertView(Table table, T expected) {
        RecordView<T> view = (RecordView<T>) table.recordView(expected.getClass());

        view.insert(null, expected);
        T actual = view.get(null, expected);

        assertThat(actual, not(same(expected)));
        assertThat(actual, equalTo(expected));
    }

    static <K, V> void assertView(Table table, K expectedKey, V expectedVal) {
        KeyValueView<K, V> view = (KeyValueView<K, V>) table.keyValueView(expectedKey.getClass(), expectedVal.getClass());
        view.put(null, expectedKey, expectedVal);
        V actual = view.get(null, expectedKey);

        assertThat(actual, not(same(expectedVal)));
        assertThat(actual, equalTo(expectedVal));
    }

    static <T, E extends Throwable> void assertViewThrows(Class<E> expectedType, String msgSubstring, Table table, T expected) {
        IgniteTestUtils.assertThrowsWithCause(() -> assertView(table, expected), expectedType, msgSubstring);
    }

    static <K, V, E extends Throwable> void assertViewThrows(
            Class<E> expectedType,
            String msgSubstring,
            Table table,
            K expectedKey,
            V expectedVal
    ) {
        IgniteTestUtils.assertThrowsWithCause(() -> assertView(table, expectedKey, expectedVal), expectedType, msgSubstring);
    }
}
