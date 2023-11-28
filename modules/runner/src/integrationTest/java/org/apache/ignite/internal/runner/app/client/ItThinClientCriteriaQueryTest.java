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
import static org.apache.ignite.table.criteria.CriteriaQueryOptions.builder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Map;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.Criteria;
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

        populateData();
    }

    @Test
    public void testBasicQueryCriteriaRecordBinaryView() {
        var view = client().tables().table(TABLE_NAME).recordView();

        var res = view.queryCriteria(null, null).getAll();
        assertThat(res, hasSize(3));

        res = view.queryCriteria(null, columnName(COLUMN_KEY).equal(2)).getAll();
        assertThat(res, hasSize(1));
        assertThat(res, hasItem(tupleValue(COLUMN_KEY, equalTo(2))));

        res = view.queryCriteria(null, Criteria.not(Criteria.equal(COLUMN_KEY, 2))).getAll();
        assertThat(res, hasSize(2));
        assertThat(res, not(hasItem(tupleValue(COLUMN_KEY, equalTo(2)))));

        var ars = view.queryCriteriaAsync(null, null, builder().pageSize(2).build()).join();
        assertEquals(2, ars.currentPageSize());
    }

    @Test
    public void testBasicQueryCriteriaRecordPojoView() {
        RecordView<TestPojo> view = client().tables().table(TABLE_NAME).recordView(TestPojo.class);

        var res = view.queryCriteria(null, null).getAll();
        assertThat(res, hasSize(3));

        res = view.queryCriteria(null, columnName(COLUMN_KEY).equal(2)).getAll();
        assertThat(res, hasSize(1));
        assertThat(res, hasItem(hasProperty(COLUMN_KEY, equalTo(2))));

        res = view.queryCriteria(null, Criteria.not(Criteria.equal(COLUMN_KEY, 2))).getAll();
        assertThat(res, hasSize(2));
        assertThat(res, not(hasItem(hasProperty(COLUMN_KEY, equalTo(2)))));

        var ars = view.queryCriteriaAsync(null, null, builder().pageSize(2).build()).join();
        assertEquals(2, ars.currentPageSize());
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
