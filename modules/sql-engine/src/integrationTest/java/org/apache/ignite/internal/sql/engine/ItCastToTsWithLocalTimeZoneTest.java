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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Set of tests to ensure correctness of CAST expression to TIMESTAMP WITH TIME ZONE for
 * type pairs supported by cast specification.
 */
@WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
public class ItCastToTsWithLocalTimeZoneTest extends BaseSqlIntegrationTest {
    @BeforeAll
    static void createTable() {
        sql("CREATE TABLE test (val TIMESTAMP WITH LOCAL TIME ZONE)");
        sql("CREATE TABLE src (id INT PRIMARY KEY, s VARCHAR(100), ts TIMESTAMP, d DATE, t TIME)");
    }

    @AfterAll
    static void dropTable() {
        sql("DROP TABLE IF EXISTS test");
        sql("DROP TABLE IF EXISTS src");
    }

    @AfterEach
    void clearTable() {
        sql("DELETE FROM test");
        sql("DELETE FROM src");
    }

    @Test
    void implicitCastOfLiteralsOnInsert() {
        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("INSERT INTO test VALUES ('1970-01-01 12:00:00')").withTimeZoneId(zone).check();
            assertQuery("INSERT INTO test VALUES (timestamp '1970-01-01 13:00:00')").withTimeZoneId(zone).check();
            assertQuery("INSERT INTO test VALUES (date '1970-01-01')").withTimeZoneId(zone).check();
            assertQuery("INSERT INTO test VALUES (time '12:00:00')").withTimeZoneId(zone).check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("INSERT INTO test VALUES ('1970-01-01 14:00:00')").withTimeZoneId(zone).check();
            assertQuery("INSERT INTO test VALUES (timestamp '1970-01-01 15:00:00')").withTimeZoneId(zone).check();
            assertQuery("INSERT INTO test VALUES (date '1970-01-01')").withTimeZoneId(zone).check();
            assertQuery("INSERT INTO test VALUES (time '14:00:00')").withTimeZoneId(zone).check();
        }

        assertQuery("SELECT * FROM test")
                .returns(Instant.parse("1970-01-01T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T09:00:00Z"))
                .returns(Instant.parse("1969-12-31T20:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T06:00:00Z"))
                .returns(Instant.parse("1970-01-01T07:00:00Z"))
                .returns(Instant.parse("1969-12-31T16:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                .check();
    }

    @Test
    void explicitCastOfLiteralsOnInsert() {
        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("INSERT INTO test VALUES (CAST('1970-01-01 12:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(timestamp '1970-01-01 13:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(date '1970-01-01' as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(time '12:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("INSERT INTO test VALUES (CAST('1970-01-01 14:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(timestamp '1970-01-01 15:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(date '1970-01-01' as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(time '14:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .check();
        }

        assertQuery("SELECT * FROM test")
                .returns(Instant.parse("1970-01-01T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T09:00:00Z"))
                .returns(Instant.parse("1969-12-31T20:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T06:00:00Z"))
                .returns(Instant.parse("1970-01-01T07:00:00Z"))
                .returns(Instant.parse("1969-12-31T16:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                .check();
    }

    @Test
    void explicitCastOfLiteralsOnSelect() {
        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("SELECT CAST('1970-01-01 12:00:00' as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1970-01-01T08:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(timestamp '1970-01-01 13:00:00' as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1970-01-01T09:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(date '1970-01-01' as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1969-12-31T20:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(time '12:00:00' as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                    .check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("SELECT CAST('1970-01-01 14:00:00' as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1970-01-01T06:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(timestamp '1970-01-01 15:00:00' as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1970-01-01T07:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(date '1970-01-01' as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1969-12-31T16:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(time '14:00:00' as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                    .check();
        }
    }

    @Test
    void explicitCastOfLiteralsOnMultiInsert() {
        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("INSERT INTO test VALUES " 
                    + "(CAST('1970-01-01 12:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))," 
                    + "(CAST(timestamp '1970-01-01 13:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))," 
                    + "(CAST(date '1970-01-01' as TIMESTAMP WITH LOCAL TIME ZONE))," 
                    + "(CAST(time '12:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone).check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("INSERT INTO test VALUES " 
                    + "(CAST('1970-01-01 14:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))," 
                    + "(CAST(timestamp '1970-01-01 15:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))," 
                    + "(CAST(date '1970-01-01' as TIMESTAMP WITH LOCAL TIME ZONE))," 
                    + "(CAST(time '14:00:00' as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone).check();
        }

        assertQuery("SELECT * FROM test")
                .returns(Instant.parse("1970-01-01T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T09:00:00Z"))
                .returns(Instant.parse("1969-12-31T20:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T06:00:00Z"))
                .returns(Instant.parse("1970-01-01T07:00:00Z"))
                .returns(Instant.parse("1969-12-31T16:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                .check();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22779")
    void implicitCastOfDynParamsOnInsert() {
        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("INSERT INTO test VALUES (?)")
                    .withTimeZoneId(zone)
                    .withParam("1970-01-01 12:00:00")
                    .check();
            assertQuery("INSERT INTO test VALUES (?)")
                    .withTimeZoneId(zone)
                    .withParam(LocalDateTime.parse("1970-01-01T13:00:00"))
                    .check();
            assertQuery("INSERT INTO test VALUES (?)")
                    .withTimeZoneId(zone)
                    .withParam(LocalDate.parse("1970-01-01"))
                    .check();
            assertQuery("INSERT INTO test VALUES (?)")
                    .withTimeZoneId(zone)
                    .withParam(LocalTime.parse("12:00:00"))
                    .check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("INSERT INTO test VALUES (?)")
                    .withTimeZoneId(zone)
                    .withParam("1970-01-01 14:00:00")
                    .check();
            assertQuery("INSERT INTO test VALUES (?)")
                    .withTimeZoneId(zone)
                    .withParam(LocalDateTime.parse("1970-01-01T15:00:00"))
                    .check();
            assertQuery("INSERT INTO test VALUES (?)")
                    .withTimeZoneId(zone)
                    .withParam(LocalDate.parse("1970-01-01"))
                    .check();
            assertQuery("INSERT INTO test VALUES (?)")
                    .withTimeZoneId(zone)
                    .withParam(LocalTime.parse("14:00:00"))
                    .check();
        }

        assertQuery("SELECT * FROM test")
                .returns(Instant.parse("1970-01-01T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T09:00:00Z"))
                .returns(Instant.parse("1969-12-31T20:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T06:00:00Z"))
                .returns(Instant.parse("1970-01-01T07:00:00Z"))
                .returns(Instant.parse("1969-12-31T16:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                .check();
    }

    @Test
    void implicitCastOfSourceTableOnInsert() {
        sql("INSERT INTO src VALUES " 
                + "(1, '1970-01-01 12:00:00', timestamp '1970-01-01 13:00:00', date '1970-01-01', time '12:00:00')," 
                + "(2, '1970-01-01 14:00:00', timestamp '1970-01-01 15:00:00', date '1970-01-01', time '14:00:00')"
        );

        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("INSERT INTO test SELECT s FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT ts FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT d FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT t FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("INSERT INTO test SELECT s FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT ts FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT d FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT t FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .check();
        }

        assertQuery("SELECT * FROM test")
                .returns(Instant.parse("1970-01-01T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T09:00:00Z"))
                .returns(Instant.parse("1969-12-31T20:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T06:00:00Z"))
                .returns(Instant.parse("1970-01-01T07:00:00Z"))
                .returns(Instant.parse("1969-12-31T16:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                .check();
    }

    @Test
    void explicitCastOfDynParamsOnInsert() {
        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("INSERT INTO test VALUES (CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .withParam("1970-01-01 12:00:00")
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .withParam(LocalDateTime.parse("1970-01-01T13:00:00"))
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .withParam(LocalDate.parse("1970-01-01"))
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .withParam(LocalTime.parse("12:00:00"))
                    .check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("INSERT INTO test VALUES (CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .withParam("1970-01-01 14:00:00")
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .withParam(LocalDateTime.parse("1970-01-01T15:00:00"))
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .withParam(LocalDate.parse("1970-01-01"))
                    .check();
            assertQuery("INSERT INTO test VALUES (CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .withParam(LocalTime.parse("14:00:00"))
                    .check();
        }

        assertQuery("SELECT * FROM test")
                .returns(Instant.parse("1970-01-01T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T09:00:00Z"))
                .returns(Instant.parse("1969-12-31T20:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T06:00:00Z"))
                .returns(Instant.parse("1970-01-01T07:00:00Z"))
                .returns(Instant.parse("1969-12-31T16:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                .check();
    }

    @Test
    void explicitCastOfDynParamsOnSelect() {
        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("SELECT CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .withParam("1970-01-01 12:00:00")
                    .returns(Instant.parse("1970-01-01T08:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .withParam(LocalDateTime.parse("1970-01-01T13:00:00"))
                    .returns(Instant.parse("1970-01-01T09:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .withParam(LocalDate.parse("1970-01-01"))
                    .returns(Instant.parse("1969-12-31T20:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .withParam(LocalTime.parse("12:00:00"))
                    .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                    .check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("SELECT CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .withParam("1970-01-01 14:00:00")
                    .returns(Instant.parse("1970-01-01T06:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .withParam(LocalDateTime.parse("1970-01-01T15:00:00"))
                    .returns(Instant.parse("1970-01-01T07:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .withParam(LocalDate.parse("1970-01-01"))
                    .returns(Instant.parse("1969-12-31T16:00:00Z"))
                    .check();
            assertQuery("SELECT CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)")
                    .withTimeZoneId(zone)
                    .withParam(LocalTime.parse("14:00:00"))
                    .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                    .check();
        }
    }

    @Test
    void explicitCastOfDynParamsOnMultiInsert() {
        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("INSERT INTO test VALUES " 
                    + "(CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))," 
                    + "(CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))," 
                    + "(CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))," 
                    + "(CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .withParams(
                            "1970-01-01 12:00:00",
                            LocalDateTime.parse("1970-01-01T13:00:00"),
                            LocalDate.parse("1970-01-01"),
                            LocalTime.parse("12:00:00")
                    )
                    .check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("INSERT INTO test VALUES "
                    + "(CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)),"
                    + "(CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)),"
                    + "(CAST(? as TIMESTAMP WITH LOCAL TIME ZONE)),"
                    + "(CAST(? as TIMESTAMP WITH LOCAL TIME ZONE))")
                    .withTimeZoneId(zone)
                    .withParams(
                            "1970-01-01 14:00:00",
                            LocalDateTime.parse("1970-01-01T15:00:00"),
                            LocalDate.parse("1970-01-01"),
                            LocalTime.parse("14:00:00")
                    )
                    .check();
        }

        assertQuery("SELECT * FROM test")
                .returns(Instant.parse("1970-01-01T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T09:00:00Z"))
                .returns(Instant.parse("1969-12-31T20:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T06:00:00Z"))
                .returns(Instant.parse("1970-01-01T07:00:00Z"))
                .returns(Instant.parse("1969-12-31T16:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                .check();
    }

    @Test
    void explicitCastOfSourceTableOnInsert() {
        sql("INSERT INTO src VALUES "
                + "(1, '1970-01-01 12:00:00', timestamp '1970-01-01 13:00:00', date '1970-01-01', time '12:00:00'),"
                + "(2, '1970-01-01 14:00:00', timestamp '1970-01-01 15:00:00', date '1970-01-01', time '14:00:00')"
        );

        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("INSERT INTO test SELECT CAST(s as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT CAST(ts as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT CAST(d as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT CAST(t as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("INSERT INTO test SELECT CAST(s as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT CAST(ts as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT CAST(d as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .check();
            assertQuery("INSERT INTO test SELECT CAST(t as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .check();
        }

        assertQuery("SELECT * FROM test")
                .returns(Instant.parse("1970-01-01T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T09:00:00Z"))
                .returns(Instant.parse("1969-12-31T20:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                .returns(Instant.parse("1970-01-01T06:00:00Z"))
                .returns(Instant.parse("1970-01-01T07:00:00Z"))
                .returns(Instant.parse("1969-12-31T16:00:00Z"))
                .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                .check();
    }

    @Test
    void explicitCastOfSourceTableOnSelect() {
        sql("INSERT INTO src VALUES "
                + "(1, '1970-01-01 12:00:00', timestamp '1970-01-01 13:00:00', date '1970-01-01', time '12:00:00'),"
                + "(2, '1970-01-01 14:00:00', timestamp '1970-01-01 15:00:00', date '1970-01-01', time '14:00:00')"
        );

        {
            ZoneId zone = ZoneOffset.ofHours(4);

            assertQuery("SELECT CAST(s as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1970-01-01T08:00:00Z"))
                    .check();

            assertQuery("SELECT CAST(ts as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1970-01-01T09:00:00Z"))
                    .check();

            assertQuery("SELECT CAST(d as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1969-12-31T20:00:00Z"))
                    .check();

            assertQuery("SELECT CAST(t as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 1")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse(currentUtcDate() + "T08:00:00Z"))
                    .check();
        }

        {
            ZoneId zone = ZoneOffset.ofHours(8);

            assertQuery("SELECT CAST(s as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1970-01-01T06:00:00Z"))
                    .check();

            assertQuery("SELECT CAST(ts as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1970-01-01T07:00:00Z"))
                    .check();

            assertQuery("SELECT CAST(d as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse("1969-12-31T16:00:00Z"))
                    .check();

            assertQuery("SELECT CAST(t as TIMESTAMP WITH LOCAL TIME ZONE) FROM src WHERE id = 2")
                    .withTimeZoneId(zone)
                    .returns(Instant.parse(currentUtcDate() + "T06:00:00Z"))
                    .check();
        }
    }

    private static LocalDate currentUtcDate() {
        return LocalDate.now(ZoneId.of("UTC"));
    }
}
