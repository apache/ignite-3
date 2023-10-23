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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.ItSystemViewsTest.KnownSystemView.SYSTEM_VIEWS;
import static org.apache.ignite.internal.sql.engine.ItSystemViewsTest.KnownSystemView.SYSTEM_VIEW_COLUMNS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * End-to-end tests to verify system views.
 */
@SuppressWarnings("DataFlowIssue")
public class ItSystemViewsTest extends ClusterPerClassIntegrationTest {
    enum KnownSystemView {
        SYSTEM_VIEWS("SYSTEM", "SYSTEM_VIEWS"),
        SYSTEM_VIEW_COLUMNS("SYSTEM", "SYSTEM_VIEW_COLUMNS"),
        ;

        private final String schema;
        private final String name;

        KnownSystemView(String schema, String name) {
            this.schema = schema;
            this.name = name;
        }
    }

    @BeforeAll
    @Override
    void beforeAll(TestInfo testInfo) {
        super.beforeAll(testInfo);

        IgniteTestUtils.await(systemViewManager().completeRegistration());
    }

    @ParameterizedTest
    @EnumSource(KnownSystemView.class)
    public void systemViewWithGivenNameExists(KnownSystemView view) {
        try (Session session = CLUSTER_NODES.get(0).sql().sessionBuilder().defaultSchema(view.schema).build()) {
            ResultSet<SqlRow> rs = session.execute(null, format("SELECT count(*) FROM {}", view.name));

            // for this test it's enough to check presence of the row,
            // because we are interested in whether the view is available for
            // querying at all
            assertThat(rs.hasNext(), is(true));
        }
    }

    @ParameterizedTest
    @EnumSource(KnownSystemView.class)
    public void systemViewWithGivenNamePresentedInSystemViewsView(KnownSystemView view) {
        try (Session session = CLUSTER_NODES.get(0).sql().sessionBuilder().defaultSchema(SYSTEM_VIEWS.schema).build()) {
            ResultSet<SqlRow> rs = session.execute(null,
                    format("SELECT count(*) FROM {} WHERE schema = '{}' AND name = '{}'",
                            SYSTEM_VIEWS.name, view.schema, view.name));

            assertThat(rs.hasNext(), is(true));

            SqlRow row = rs.next();

            assertThat(row.value(0), is(1L));
        }
    }

    @Test
    public void systemViewsViewMetadataTest() {
        KnownSystemView view = SYSTEM_VIEWS;

        try (Session session = CLUSTER_NODES.get(0).sql().sessionBuilder().defaultSchema(view.schema).build()) {
            ResultSet<SqlRow> rs = session.execute(null, format("SELECT * FROM {}", view.name));

            ResultSetMetadata metadata = rs.metadata();

            assertMetadata(metadata, List.of(
                    new MetadataMatcher()
                            .name("ID")
                            .type(ColumnType.INT32)
                            .nullable(true),

                    new MetadataMatcher()
                            .name("SCHEMA")
                            .type(ColumnType.STRING)
                            .precision(Short.MAX_VALUE)
                            .nullable(true),

                    new MetadataMatcher()
                            .name("NAME")
                            .type(ColumnType.STRING)
                            .precision(Short.MAX_VALUE)
                            .nullable(true),

                    new MetadataMatcher()
                            .name("TYPE")
                            .type(ColumnType.STRING)
                            .precision(Short.MAX_VALUE)
                            .nullable(true)
            ));
        }
    }

    @Test
    public void systemViewColumnsViewMetadataTest() {
        KnownSystemView view = SYSTEM_VIEW_COLUMNS;

        try (Session session = CLUSTER_NODES.get(0).sql().sessionBuilder().defaultSchema(view.schema).build()) {
            ResultSet<SqlRow> rs = session.execute(null, format("SELECT * FROM {}", view.name));

            ResultSetMetadata metadata = rs.metadata();

            assertMetadata(metadata, List.of(
                    new MetadataMatcher()
                            .name("VIEW_ID")
                            .type(ColumnType.INT32)
                            .nullable(true),

                    new MetadataMatcher()
                            .name("NAME")
                            .type(ColumnType.STRING)
                            .precision(Short.MAX_VALUE)
                            .nullable(true),

                    new MetadataMatcher()
                            .name("TYPE")
                            .type(ColumnType.STRING)
                            .precision(Short.MAX_VALUE)
                            .nullable(true),

                    new MetadataMatcher()
                            .name("NULLABLE")
                            .type(ColumnType.BOOLEAN)
                            .nullable(true),

                    new MetadataMatcher()
                            .name("PRECISION")
                            .type(ColumnType.INT32)
                            .nullable(true),

                    new MetadataMatcher()
                            .name("SCALE")
                            .type(ColumnType.INT32)
                            .nullable(true),

                    new MetadataMatcher()
                            .name("LENGTH")
                            .type(ColumnType.INT32)
                            .nullable(true)
            ));

        }
    }

    private static void assertMetadata(ResultSetMetadata actual, List<MetadataMatcher> matchers) {
        assertThat(actual, notNullValue());
        assertThat(actual.columns(), hasSize(matchers.size()));

        for (int i = 0; i < matchers.size(); i++) {
            matchers.get(i).check(actual.columns().get(i));
        }
    }
}
