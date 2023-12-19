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

import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;
import static org.apache.ignite.table.criteria.Criteria.not;
import static org.apache.ignite.table.criteria.CriteriaQueryOptions.builder;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ClosableCursor;
import org.apache.ignite.sql.async.AsyncClosableCursor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
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
        RecordView<Tuple> view = client().tables().table(TABLE_NAME).recordView();

        List<Tuple> res = Lists.newArrayList(view.queryCriteria(null, null));
        assertThat(res, containsInAnyOrder(
                tupleValue(COLUMN_KEY, is(0)),
                tupleValue(COLUMN_KEY, is(1)),
                tupleValue(COLUMN_KEY, is(2))
        ));

        res = Lists.newArrayList(view.queryCriteria(null, not(columnValue(COLUMN_KEY, equalTo(2)))));
        assertThat(res, containsInAnyOrder(
                tupleValue(COLUMN_KEY, is(0)),
                tupleValue(COLUMN_KEY, is(1))
        ));

        AsyncClosableCursor<Tuple> ars = view.queryCriteriaAsync(null, null, builder().pageSize(2).build()).join();
        assertEquals(2, ars.currentPageSize());
    }

    @Test
    public void testBasicQueryCriteriaRecordPojoView() {
        RecordView<TestPojo> view = client().tables().table(TABLE_NAME).recordView(TestPojo.class);

        List<TestPojo> res = Lists.newArrayList(view.queryCriteria(null, null));
        assertThat(res, containsInAnyOrder(
                hasProperty(COLUMN_KEY, is(0)),
                hasProperty(COLUMN_KEY, is(1)),
                hasProperty(COLUMN_KEY, is(2))
        ));

        res = Lists.newArrayList(view.queryCriteria(null, columnValue(COLUMN_KEY, equalTo(2))));
        assertThat(res, containsInAnyOrder(
                hasProperty(COLUMN_KEY, is(2))
        ));

        res = Lists.newArrayList(view.queryCriteria(null, not(columnValue(COLUMN_KEY, equalTo(2)))));
        assertThat(res, containsInAnyOrder(
                hasProperty(COLUMN_KEY, is(0)),
                hasProperty(COLUMN_KEY, is(1))
        ));

        var ars = view.queryCriteriaAsync(null, null, builder().pageSize(2).build()).join();
        assertEquals(2, ars.currentPageSize());
    }

    @Test
    public void testBasicQueryCriteriaKeyValueBinaryView() {
        KeyValueView<Tuple, Tuple> view = client().tables().table(TABLE_NAME).keyValueView();

        Map<Tuple, Tuple> res = toMap(view.queryCriteria(null, null));
        assertThat(res, allOf(
                aMapWithSize(3),
                hasEntry(tupleValue(COLUMN_KEY, is(0)), tupleValue(COLUMN_VAL, is("0"))),
                hasEntry(tupleValue(COLUMN_KEY, is(1)), tupleValue(COLUMN_VAL, is("1"))),
                hasEntry(tupleValue(COLUMN_KEY, is(2)), tupleValue(COLUMN_VAL, is("2")))
        ));

        res = toMap(view.queryCriteria(null, columnValue(COLUMN_KEY, equalTo(2))));
        assertThat(res, allOf(
                aMapWithSize(1),
                hasEntry(tupleValue(COLUMN_KEY, is(2)), tupleValue(COLUMN_VAL, is("2")))
        ));

        res = toMap(view.queryCriteria(null, not(columnValue(COLUMN_KEY, equalTo(2)))));
        assertThat(res, allOf(
                aMapWithSize(2),
                hasEntry(tupleValue(COLUMN_KEY, is(0)), tupleValue(COLUMN_VAL, is("0"))),
                hasEntry(tupleValue(COLUMN_KEY, is(1)), tupleValue(COLUMN_VAL, is("1")))
        ));
    }

    @Test
    public void testBasicQueryCriteriaKeyValueView() {
        var view = client().tables().table(TABLE_NAME).keyValueView(TestPojoKey.class, TestPojo.class);

        Map<TestPojoKey, TestPojo> res = toMap(view.queryCriteria(null, null));
        assertThat(res, allOf(
                aMapWithSize(3),
                hasEntry(hasProperty(COLUMN_KEY, is(0)), hasProperty(COLUMN_VAL, is("0"))),
                hasEntry(hasProperty(COLUMN_KEY, is(1)), hasProperty(COLUMN_VAL, is("1"))),
                hasEntry(hasProperty(COLUMN_KEY, is(2)), hasProperty(COLUMN_VAL, is("2")))
        ));

        res = toMap(view.queryCriteria(null, columnValue(COLUMN_KEY, equalTo(2))));
        assertThat(res, allOf(
                aMapWithSize(1),
                hasEntry(hasProperty(COLUMN_KEY, is(2)), hasProperty(COLUMN_VAL, is("2")))
        ));

        res = toMap(view.queryCriteria(null, not(columnValue(COLUMN_KEY, equalTo(2)))));
        assertThat(res, allOf(
                aMapWithSize(2),
                hasEntry(hasProperty(COLUMN_KEY, is(0)), hasProperty(COLUMN_VAL, is("0"))),
                hasEntry(hasProperty(COLUMN_KEY, is(1)), hasProperty(COLUMN_VAL, is("1")))
        ));
    }

    private static void populateData(Ignite ignite, String tableName) {
        RecordView<Tuple> table = ignite.tables().table(tableName).recordView();

        for (int val = 0; val < 3; val++) {
            table.insert(null, Tuple.create(Map.of(COLUMN_KEY, val % 100, "val", String.valueOf(val % 100))));
        }
    }

    private static <K, V> Map<K, V> toMap(ClosableCursor<Entry<K, V>> cursor) {
        return Lists.newArrayList(cursor).stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
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
