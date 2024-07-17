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

import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * End-to-end tests to verify system views.
 */
public class ItSystemViewsTest extends BaseSqlIntegrationTest {
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

        String canonicalName() {
            return schema + "." + name;
        }
    }

    @Override
    @BeforeAll
    protected void beforeAll(TestInfo testInfo) {
        super.beforeAll(testInfo);

        IgniteTestUtils.await(systemViewManager().completeRegistration());
    }

    @ParameterizedTest
    @EnumSource(KnownSystemView.class)
    public void systemViewWithGivenNameExists(KnownSystemView view) {
        assertQuery(format("SELECT count(*) FROM {}", view.canonicalName()))
                // for this test it's enough to check presence of the row,
                // because we are interested in whether the view is available for
                // querying at all
                .returnSomething()
                .check();
    }

    @ParameterizedTest
    @EnumSource(KnownSystemView.class)
    public void systemViewWithGivenNamePresentedInSystemViewsView(KnownSystemView view) {
        assertQuery(format("SELECT * FROM {} WHERE schema = ? AND name = ?", SYSTEM_VIEWS.canonicalName()))
                .withParams(view.schema, view.name)
                .returnSomething()
                .check();
    }

    @Test
    public void systemViewsViewMetadataTest() {
        assertQuery(format("SELECT * FROM {}", SYSTEM_VIEWS.canonicalName()))
                .columnMetadata(
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
                )
                .check();
    }

    @Test
    public void systemViewColumnsViewMetadataTest() {
        assertQuery(format("SELECT * FROM {}", SYSTEM_VIEW_COLUMNS.canonicalName()))
                .columnMetadata(
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
                                .nullable(true))
                .check();
    }

    @Test
    public void systemViewAndTableCrossQuery() {
        KnownSystemView view = SYSTEM_VIEWS;

        sql("CREATE TABLE known_views(view_id INT primary key, name varchar(256))");
        sql(format("INSERT INTO known_views SELECT id, SUBSTRING(name, 256) FROM {}", view.canonicalName()));

        assertQuery(format("SELECT * FROM known_views kv JOIN {} sv"
                + " ON kv.view_id = sv.id WHERE sv.name = ?", view.canonicalName()))
                .withParam(view.name)
                .returnSomething()
                .check();
    }
}
