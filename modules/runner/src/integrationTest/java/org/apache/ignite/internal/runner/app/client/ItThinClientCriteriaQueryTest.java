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

import static org.apache.ignite.table.criteria.CriteriaBuilder.columnName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

import java.util.Map;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.mapper.Mapper;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Thin client criteria query test.
 */
@SuppressWarnings("resource")
public class ItThinClientCriteriaQueryTest extends ItAbstractThinClientTest {
    @BeforeEach
    public void setUp() {
        populateData();
    }

    @Test
    public void testBasicQueryCriteriaRecordBinaryView() {
        RecordView<Tuple> view = client().tables().table(TABLE_NAME).recordView();

        try (var cursor = view.queryCriteria(null, null)) {
            assertThat(cursor.getAll(), hasSize(3));
        }

        try (var cursor = view.queryCriteria(null, columnName(COLUMN_KEY).equal(2))) {
            assertThat(cursor.getAll(), hasItem(tupleValue(COLUMN_KEY, equalTo(2))));
        }

        try (var cursor = view.queryCriteria(null, Criteria.not(Criteria.equal(COLUMN_KEY, 2)))) {
            assertThat(cursor.getAll(), not(hasItem(tupleValue(COLUMN_KEY, equalTo(2)))));
        }
    }

    @Test
    public void testBasicQueryCriteriaRecordPojoView() {
        RecordView<TestPojo> view = client().tables().table(TABLE_NAME).recordView(TestPojo.class);

        try (var cursor = view.queryCriteria(null, null)) {
            assertThat(cursor.getAll(), hasSize(3));
        }

        try (var cursor = view.queryCriteria(null, columnName(COLUMN_KEY).equal(2))) {
            assertThat(cursor.getAll(), hasItem(hasProperty(COLUMN_KEY, equalTo(2))));
        }

        try (var cursor = view.queryCriteria(null, Criteria.not(Criteria.equal(COLUMN_KEY, 2)))) {
            assertThat(cursor.getAll(), not(hasItem(hasProperty(COLUMN_KEY, equalTo(2)))));
        }
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16116")
    @Test
    public void testBasicQueryCriteriaKvBinaryView() {
        KeyValueView<Tuple, Tuple> view = client().tables().table(TABLE_NAME).keyValueView();

        try (var cursor = view.queryCriteria(null, null)) {
            assertThat(cursor.getAll(), hasSize(3));
        }

        try (var cursor = view.queryCriteria(null, columnName(COLUMN_KEY).equal(2))) {
            assertThat(cursor.getAll(), hasItem(hasProperty(COLUMN_KEY, equalTo(2))));
        }

        try (var cursor = view.queryCriteria(null, Criteria.not(Criteria.equal(COLUMN_KEY, 2)))) {
            assertThat(cursor.getAll(), not(hasItem(hasProperty(COLUMN_KEY, equalTo(2)))));
        }
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16116")
    @Test
    public void testBasicQueryCriteriaKvPojoView() {
        var view = client().tables().table(TABLE_NAME).keyValueView(Mapper.of(Integer.class), Mapper.of(TestPojo.class));
        
        try (var cursor = view.queryCriteria(null, null)) {
            assertThat(cursor.getAll(), hasSize(3));
        }

        try (var cursor = view.queryCriteria(null, columnName(COLUMN_KEY).equal(2))) {
            assertThat(cursor.getAll(), hasItem(hasKey(2)));
        }

        try (var cursor = view.queryCriteria(null, Criteria.not(Criteria.equal(COLUMN_KEY, 2)))) {
            assertThat(cursor.getAll(), not(hasItem(hasKey(2))));
        }
    }

    private void populateData() {
       var table = client().tables().table(TABLE_NAME).recordView();

        table.insert(null, Tuple.create(Map.of(COLUMN_KEY, 1, COLUMN_VAL, "1")));
        table.insert(null, Tuple.create(Map.of(COLUMN_KEY, 2, COLUMN_VAL, "2")));
        table.insert(null, Tuple.create(Map.of(COLUMN_KEY, 3, COLUMN_VAL, "3")));
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
}
