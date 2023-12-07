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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.internal.table.criteria.CriteriaElement.equalTo;
import static org.apache.ignite.internal.table.criteria.Criterias.columnValue;
import static org.apache.ignite.internal.table.criteria.Criterias.not;
import static org.apache.ignite.table.criteria.CriteriaQueryOptions.builder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.sql.ClosableCursor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Thin client criteria query test.
 */
@SuppressWarnings("resource")
public class ItThinClientCriteriaQueryTest extends ItAbstractThinClientTest {
    /** {@inheritDoc} */
    @Override
    @BeforeAll
    void beforeAll(TestInfo testInfo, @WorkDirectory Path workDir) throws InterruptedException {
        super.beforeAll(testInfo, workDir);

        populateData(client(), TABLE_NAME);
    }

    @Test
    public void testBasicQueryCriteriaRecordBinaryView() {
        var view = client().tables().table(TABLE_NAME).recordView();

        var res = view.queryCriteria(null, null).getAll();
        assertThat(res, hasSize(15));

        res = view.queryCriteria(null, columnValue(COLUMN_KEY, equalTo(2))).getAll();
        assertThat(res, hasSize(1));
        assertThat(res, hasItem(tupleValue(COLUMN_KEY, Matchers.equalTo(2))));

        res = view.queryCriteria(null, not(columnValue(COLUMN_KEY, equalTo(2)))).getAll();
        assertThat(res, hasSize(14));
        assertThat(res, not(hasItem(tupleValue(COLUMN_KEY, Matchers.equalTo(2)))));

        var ars = view.queryCriteriaAsync(null, null, builder().pageSize(2).build()).join();
        assertEquals(2, ars.currentPageSize());
    }

    @Test
    public void testBasicQueryCriteriaRecordPojoView() {
        RecordView<TestPojo> view = client().tables().table(TABLE_NAME).recordView(TestPojo.class);

        var res = view.queryCriteria(null, null).getAll();
        assertThat(res, hasSize(15));

        res = view.queryCriteria(null, columnValue(COLUMN_KEY, equalTo(2))).getAll();
        assertThat(res, hasSize(1));
        assertThat(res, hasItem(hasProperty(COLUMN_KEY, Matchers.equalTo(2))));

        res = view.queryCriteria(null, not(columnValue(COLUMN_KEY, equalTo(2)))).getAll();
        assertThat(res, hasSize(14));
        assertThat(res, not(hasItem(hasProperty(COLUMN_KEY, Matchers.equalTo(2)))));

        var ars = view.queryCriteriaAsync(null, null, builder().pageSize(2).build()).join();
        assertEquals(2, ars.currentPageSize());
    }

    @Test
    public void testBasicQueryCriteriaKeyValueBinaryView() {
        var view = client().tables().table(TABLE_NAME).keyValueView();

        var res = toMap(view.queryCriteria(null, null));
        assertThat(res, aMapWithSize(15));

        res = toMap(view.queryCriteria(null, columnValue(COLUMN_KEY, equalTo(2))));
        assertThat(res, aMapWithSize(1));
        assertThat(res, hasEntry(tupleValue(COLUMN_KEY, Matchers.equalTo(2)), tupleValue(COLUMN_VAL, Matchers.equalTo("2"))));

        res = toMap(view.queryCriteria(null, not(columnValue(COLUMN_KEY, equalTo(2)))));
        assertThat(res, aMapWithSize(14));
        assertThat(res, not(hasEntry(tupleValue(COLUMN_KEY, Matchers.equalTo(2)), tupleValue(COLUMN_VAL, Matchers.equalTo("2")))));
    }

    @Test
    public void testBasicQueryCriteriaKeyValueView() {
        var view = client().tables().table(TABLE_NAME).keyValueView(TestPojoKey.class, TestPojo.class);

        var res = toMap(view.queryCriteria(null, null));
        assertThat(res, aMapWithSize(15));

        res = toMap(view.queryCriteria(null, columnValue(COLUMN_KEY, equalTo(2))));
        assertThat(res, aMapWithSize(1));
        assertThat(res, hasEntry(hasProperty(COLUMN_KEY, Matchers.equalTo(2)), hasProperty(COLUMN_VAL, Matchers.equalTo("2"))));

        res = toMap(view.queryCriteria(null, not(columnValue(COLUMN_KEY, equalTo(2)))));
        assertThat(res, aMapWithSize(14));
        assertThat(res, not(hasEntry(hasProperty(COLUMN_KEY, Matchers.equalTo(2)), hasProperty(COLUMN_VAL, Matchers.equalTo("2")))));
    }

    private static void populateData(IgniteClient client, String tableName) {
        var keyValueView = client.tables().table(tableName).keyValueView();

        for (int val = 0; val < 15; val++) {
            Tuple key = Tuple.create().set(COLUMN_KEY, val % 100);
            Tuple value = Tuple.create().set(COLUMN_VAL, String.valueOf(val));

            keyValueView.put(null, key, value);
        }
    }

    private static <K, V> Map<K, V> toMap(ClosableCursor<Entry<K, V>> cursor) {
        return cursor.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * Creates a matcher for matching tuple value.
     *
     * @param valueMatcher Matcher for matching tuple value.
     * @return Matcher for matching tuple value.
     */
    private static <T> Matcher<Tuple> tupleValue(String columnName, Matcher<T> valueMatcher) {
        return new FeatureMatcher<>(valueMatcher, "A tuple with value", "value") {
            @Override
            protected @Nullable T featureValueOf(Tuple actual) {
                return actual.value(columnName);
            }
        };
    }

    /**
     * Test class.
     */
    public static class TestPojoKey {
        int key;

        public void setKey(int key) {
            this.key = key;
        }

        public int getKey() {
            return key;
        }
    }
}
