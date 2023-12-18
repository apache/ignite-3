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

import static org.apache.ignite.internal.util.CollectionUtils.toList;
import static org.apache.ignite.table.criteria.CriteriaQueryOptions.builder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.sql.async.AsyncClosableCursor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
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
        RecordView<Tuple> view = client().tables().table(TABLE_NAME).recordView();

        List<Tuple> res = toList(view.queryCriteria(null, null));
        assertThat(res, hasSize(3));

        AsyncClosableCursor<Tuple> ars = view.queryCriteriaAsync(null, null, builder().pageSize(2).build()).join();
        assertEquals(2, ars.currentPageSize());
    }

    @Test
    public void testBasicQueryCriteriaRecordPojoView() {
        RecordView<TestPojo> view = client().tables().table(TABLE_NAME).recordView(TestPojo.class);

        List<TestPojo> res = toList(view.queryCriteria(null, null));
        assertThat(res, allOf(hasSize(3), containsInAnyOrder(
                hasProperty(COLUMN_KEY, is(1)),
                hasProperty(COLUMN_KEY, is(2)),
                hasProperty(COLUMN_KEY, is(3))
        )));

        AsyncClosableCursor<TestPojo> ars = view.queryCriteriaAsync(null, null, builder().pageSize(2).build()).join();
        assertEquals(2, ars.currentPageSize());
    }

    private void populateData() {
        var table = client().tables().table(TABLE_NAME).recordView();

        table.insert(null, Tuple.create(Map.of(COLUMN_KEY, 1, COLUMN_VAL, "1")));
        table.insert(null, Tuple.create(Map.of(COLUMN_KEY, 2, COLUMN_VAL, "2")));
        table.insert(null, Tuple.create(Map.of(COLUMN_KEY, 3, COLUMN_VAL, "3")));
    }
}
