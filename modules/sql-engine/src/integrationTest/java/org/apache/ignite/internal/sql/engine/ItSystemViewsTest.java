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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.ItSystemViewsTest.KnownSystemView.SYSTEM_VIEWS;
import static org.apache.ignite.internal.sql.engine.ItSystemViewsTest.KnownSystemView.SYSTEM_VIEW_COLUMNS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.time.Instant;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.registry.QueryExecutionPhase;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * End-to-end tests to verify system views.
 */
public class ItSystemViewsTest extends BaseSqlMultiStatementTest {
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

    @BeforeAll
    void beforeAll() {
        await(systemViewManager().completeRegistration());
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

    @Test
    public void runningScript() {
        // TODO Add tests for TX_CONTROL
        // TODO Add tests for edge cases
        Ignite initiator = CLUSTER.aliveNode();
        ClockService clockService = unwrapIgniteImpl(initiator).clockService();

        String queryText = "SELECT x FROM TABLE(SYSTEM_RANGE(0, 2));"
                + "SELECT x FROM TABLE(SYSTEM_RANGE(3, 5));";

        long timeBefore = clockService.now().getPhysical();
        List<AsyncSqlCursor<InternalSqlRow>> cursors = fetchAllCursors(runScript(queryText));
        long timeAfter = clockService.now().getPhysical();

        assertThat(cursors, hasSize(2));
        assertThat(queryProcessor().runningQueries(), hasSize(3));

        // Verify script query info.
        {
            String sql = "SELECT * FROM SYSTEM.SQL_QUERIES WHERE TYPE='SCRIPT'";
            List<List<Object>> res = sql(initiator, null, null, null, sql);

            assertThat(res, hasSize(1));

            verifyQueryInfo(res.get(0), initiator.name(), null, queryText, timeBefore, timeAfter,
                    is(nullValue(CharSequence.class)), "SCRIPT", null);
        }

        // Verify script statement query info.
        {
            String sql = "SELECT * FROM SYSTEM.SQL_QUERIES "
                    + "WHERE PARENT_ID=(SELECT ID FROM SYSTEM.SQL_QUERIES WHERE TYPE='SCRIPT') "
                    + "ORDER BY STATEMENT_NUM";

            List<List<Object>> res = sql(initiator, null, null, null, sql);

            assertThat(res, hasSize(2));

            for (int i = 0; i < res.size(); i++) {
                List<Object> row = res.get(i);

                verifyQueryInfo(row, initiator.name(), null, null, timeBefore, timeAfter,
                        hasLength(36), SqlQueryType.QUERY.name(), i + 1);
            }
        }

        // Closing cursors.
        await(cursors.get(0).closeAsync());
        assertThat(queryProcessor().runningQueries(), hasSize(2));

        await(cursors.get(1).closeAsync());
        assertThat(queryProcessor().runningQueries(), hasSize(0));
    }

    @Test
    public void runningQueries() {
        String query = "SELECT * FROM SQL_QUERIES ORDER BY START_TIME";

        // Verify metadata.
        assertQuery(query)
                .withDefaultSchema("SYSTEM")
                .columnMetadata(
                        new MetadataMatcher().name("INITIATOR_NODE").type(ColumnType.STRING),
                        new MetadataMatcher().name("ID").type(ColumnType.STRING).precision(36),
                        new MetadataMatcher().name("PHASE").type(ColumnType.STRING),
                        new MetadataMatcher().name("TYPE").type(ColumnType.STRING),
                        new MetadataMatcher().name("SCHEMA").type(ColumnType.STRING),
                        new MetadataMatcher().name("SQL").type(ColumnType.STRING),
                        new MetadataMatcher().name("START_TIME").type(ColumnType.TIMESTAMP),
                        new MetadataMatcher().name("TRANSACTION_ID").type(ColumnType.STRING).precision(36),
                        new MetadataMatcher().name("PARENT_ID").type(ColumnType.STRING).precision(36),
                        new MetadataMatcher().name("STATEMENT_NUM").type(ColumnType.INT32)
                )
                .returnRowCount(1)
                .check();

        Ignite initiator = CLUSTER.aliveNode();

        ClockService clockService = unwrapIgniteImpl(initiator).clockService();


        String schema = "SYSTEM";

        // Test with explicit tx.
        InternalTransaction tx = (InternalTransaction) initiator.transactions().begin();

        try {
            long tsBefore = clockService.now().getPhysical();

            List<List<Object>> res = sql(initiator, tx, schema, null, query);

            long tsAfter = clockService.now().getPhysical();

            assertThat(res, hasSize(1));

            verifyQueryInfo(res.get(0), initiator.name(), schema, query, tsBefore, tsAfter,
                    equalTo(tx.id().toString()), SqlQueryType.QUERY.name(), null);
        } finally {
            tx.rollback();
        }

        // Implicit tx.
        {
            long tsBefore = clockService.now().getPhysical();

            List<List<Object>> res = sql(initiator, null, schema, null, query);

            long tsAfter = clockService.now().getPhysical();

            assertThat(res, hasSize(1));

            verifyQueryInfo(res.get(0), initiator.name(), schema, query, tsBefore, tsAfter,
                    hasLength(36), SqlQueryType.QUERY.name(), null);
        }
    }

    private static void verifyQueryInfo(
            List<Object> row,
            String nodeName,
            @Nullable String schema,
            @Nullable String query,
            long tsBefore,
            long tsAfter,
            Matcher<CharSequence> txIdMatcher,
            String queryType,
            @Nullable Integer statementNum
    ) {
        int idx = 0;

        // INITIATOR_NODE
        assertThat(row.get(idx++), equalTo(nodeName));

        // ID
        assertThat((String) row.get(idx++), hasLength(36));

        // PHASE
        assertThat(row.get(idx++), equalTo(QueryExecutionPhase.EXECUTION.name()));

        // TYPE
        assertThat(row.get(idx++), equalTo(queryType));

        // SCHEMA
        assertThat(row.get(idx++), equalTo(schema == null ? SqlCommon.DEFAULT_SCHEMA_NAME : schema));

        // SQL
        assertThat(row.get(idx++), query == null ? is(notNullValue()) : equalTo(query));

        // START_TIME
        assertThat(((Instant) row.get(idx++)).toEpochMilli(), Matchers.allOf(greaterThanOrEqualTo(tsBefore), lessThanOrEqualTo(tsAfter)));

        // TRANSACTION_ID
        assertThat((String) row.get(idx++), txIdMatcher);

        // PARENT_ID
        assertThat((String) row.get(idx++), statementNum == null ? is(nullValue(CharSequence.class)) : hasLength(36));

        // STATEMENT_NUM
        assertThat(row.get(idx++), statementNum == null ? is(nullValue()) : equalTo(statementNum));
    }
}
