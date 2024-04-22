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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify {@link StatementBuilderImpl}.
 */
public class StatementBuilderImplTest {
    private static final String QUERY = "select 1";

    private final StatementBuilderImpl builder = new StatementBuilderImpl();

    @Test
    public void queryCannotBeNull() {
        //noinspection ThrowableNotThrown
        assertThrows(
                NullPointerException.class,
                () -> builder.build(),
                "Parameter 'query' cannot be null"
        );
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void pageSizeMustBeGreaterThanZero() {
        assertThrows(
                IllegalArgumentException.class,
                () -> builder.pageSize(0),
                "Page size must be positive: 0"
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> builder.pageSize(-1),
                "Page size must be positive: -1"
        );
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void queryTimeoutMustBeGreaterThanZero() {
        assertThrows(
                IllegalArgumentException.class,
                () -> builder.queryTimeout(0, TimeUnit.SECONDS),
                "Timeout must be positive: 0"
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> builder.queryTimeout(-1, TimeUnit.SECONDS),
                "Timeout must be positive: -1"
        );

        assertThrows(
                NullPointerException.class,
                () -> builder.queryTimeout(1, null),
                "timeUnit"
        );
    }

    @Test
    public void checkDefaultParameters() throws Exception {
        try (Statement statement = builder.query(QUERY).build()) {
            assertThat(statement.defaultSchema(), is(SqlQueryProcessor.DEFAULT_SCHEMA_NAME));
            assertThat(statement.query(), is(QUERY));
            assertThat(statement.timeZoneId(), is(ZoneId.systemDefault()));
            assertThat(statement.pageSize(), is(IgniteSqlImpl.DEFAULT_PAGE_SIZE));
            assertThat(statement.queryTimeout(TimeUnit.NANOSECONDS), is(0L));
        }
    }

    @Test
    public void testFillAttributes() {
        long timeout = 17;
        int pageSize = 128;
        String schema = "SYSTEM";
        ZoneId timeZone = ZoneId.of("GMT+3");

        Statement statement = builder
                .query(QUERY)
                .queryTimeout(timeout, TimeUnit.MINUTES)
                .pageSize(pageSize)
                .defaultSchema(schema)
                .timeZoneId(timeZone)
                .build();

        assertThat(statement.query(), is(QUERY));
        assertThat(statement.defaultSchema(), is(schema));
        assertThat(statement.queryTimeout(TimeUnit.MINUTES), is(timeout));
        assertThat(statement.pageSize(), is(pageSize));
        assertThat(statement.timeZoneId(), is(timeZone));
    }

    @Test
    public void testTimeoutUnitsConversion() {
        int timeout = 36;

        Statement statement = builder
                .query(QUERY)
                .queryTimeout(timeout, TimeUnit.MINUTES)
                .build();

        assertThat(
                statement.queryTimeout(TimeUnit.NANOSECONDS),
                is(TimeUnit.MINUTES.toNanos(timeout))
        );
    }

    @Test
    public void testToBuilder() {
        long timeout = 17;
        int pageSize = 128;
        String schema = "SYSTEM";
        ZoneId timeZone = ZoneId.of("GMT+1");

        Statement statement1 = builder
                .query(QUERY)
                .pageSize(pageSize - 1)
                .queryTimeout(timeout, TimeUnit.SECONDS)
                .build();

        Statement statement2 = statement1.toBuilder()
                .timeZoneId(timeZone)
                .pageSize(pageSize + 1)
                .queryTimeout(timeout, TimeUnit.MINUTES)
                .build();

        Statement statement3 = statement2.toBuilder()
                .query(QUERY + "3")
                .pageSize(pageSize)
                .defaultSchema(schema)
                .queryTimeout(timeout, TimeUnit.HOURS)
                .build();

        assertThat(statement1.query(), is(QUERY));
        assertThat(statement1.defaultSchema(), is(SqlQueryProcessor.DEFAULT_SCHEMA_NAME));
        assertThat(statement1.queryTimeout(TimeUnit.SECONDS), is(timeout));
        assertThat(statement1.pageSize(), is(pageSize - 1));
        assertThat(statement1.timeZoneId(), is(ZoneId.systemDefault()));

        assertThat(statement2.query(), is(QUERY));
        assertThat(statement2.defaultSchema(), is(SqlQueryProcessor.DEFAULT_SCHEMA_NAME));
        assertThat(statement2.queryTimeout(TimeUnit.MINUTES), is(timeout));
        assertThat(statement2.pageSize(), is(pageSize + 1));
        assertThat(statement2.timeZoneId(), is(timeZone));

        assertThat(statement3.query(), is(QUERY + "3"));
        assertThat(statement3.defaultSchema(), is(schema));
        assertThat(statement3.queryTimeout(TimeUnit.HOURS), is(timeout));
        assertThat(statement3.pageSize(), is(pageSize));
        assertThat(statement3.timeZoneId(), is(timeZone));
    }

    @Test
    public void testBuilderCanBeReused() {
        Statement statement1 = builder.query(QUERY).build();
        Statement statement2 = builder.defaultSchema("SYSTEM").build();

        assertNotSame(statement1, statement2);

        assertThat(statement1.query(), is(QUERY));
        assertThat(statement2.query(), is(QUERY));

        assertThat(statement1.defaultSchema(), is(SqlQueryProcessor.DEFAULT_SCHEMA_NAME));
        assertThat(statement2.defaultSchema(), is("SYSTEM"));
    }
}
