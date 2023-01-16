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

import static org.apache.ignite.internal.sql.engine.util.SqlTypeUtils.toSqlType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

/** Dynamic parameters checks. */
public class ItDynamicParameterTest extends AbstractBasicIntegrationTest {
    private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

    @BeforeEach
    public void createTable() {
        sql("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val1 INTEGER NOT NULL, val2 INTEGER)");
    }

    @AfterEach
    public void dropTables() {
        sql("DROP TABLE IF EXISTS t1");
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class,
            //    https://issues.apache.org/jira/browse/IGNITE-18258
            //    https://issues.apache.org/jira/browse/IGNITE-18414
            //    https://issues.apache.org/jira/browse/IGNITE-18415
            //    https://issues.apache.org/jira/browse/IGNITE-18345
            names = {"DECIMAL", "NUMBER", "UUID", "BITMASK", "DURATION", "DATETIME", "TIMESTAMP", "DATE", "TIME", "PERIOD"},
            mode = Mode.EXCLUDE
    )
    void testMetadataTypesForDynamicParameters(ColumnType type) {
        Object param = generateValueByType(RND.nextInt(), type);

        assertQuery("SELECT typeof(?)").withParams(param).returns(toSqlType(type)).check();
        assertQuery("SELECT ?").withParams(param).returns(param).columnMetadata(new MetadataMatcher().type(type)).check();
    }

    @Test
    public void testDynamicParameters() {
        assertQuery("SELECT COALESCE(?, ?)").withParams("a", 10).returns("a").check();
        assertQuery("SELECT COALESCE(null, ?)").withParams(13).returns(13).check();
        assertQuery("SELECT LOWER(?)").withParams("ASD").returns("asd").check();
        assertQuery("SELECT ?").withParams("asd").returns("asd").check();
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2, 2, "TeSt").returns(4, "test").check();
        assertQuery("SELECT LOWER(?), ? + ? ").withParams("TeSt", 2, 2).returns("test", 4).check();

        createAndPopulateTable();
        assertQuery("SELECT name LIKE '%' || ? || '%' FROM person where name is not null").withParams("go").returns(true).returns(false)
                .returns(false).returns(false).check();

        assertQuery("SELECT id FROM person WHERE name LIKE ? ORDER BY id LIMIT ?").withParams("I%", 1).returns(0).check();
        assertQuery("SELECT id FROM person WHERE name LIKE ? ORDER BY id LIMIT ? OFFSET ?").withParams("I%", 1, 1).returns(2).check();
    }

    // After fix the mute reason need to merge the test with above testDynamicParameters
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18258")
    @Test
    public void testDynamicParameters2() {
        assertQuery("SELECT POWER(?, ?)").withParams(2, 3).returns(8).check();
        assertQuery("SELECT SQRT(?)").withParams(4d).returns(2d).check();
        assertQuery("SELECT ? % ?").withParams(11, 10).returns(BigDecimal.valueOf(1)).check();

        assertQuery("SELECT id from person where salary<? and id>?").withParams(15, 1).returns(2).check();
    }

    // After fix the mute reason need to merge the test with above testDynamicParameters
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18345")
    @Test
    public void testDynamicParameters3() {
        assertQuery("SELECT LAST_DAY(?)").withParams(Date.valueOf("2022-01-01")).returns(Date.valueOf("2022-01-31")).check();
        assertQuery("SELECT LAST_DAY(?)").withParams(LocalDate.parse("2022-01-01")).returns(Date.valueOf("2022-01-31")).check();
    }

    /** Need to test the same query with different type of parameters to cover case with check right plans cache work. **/
    @Test
    public void testWithDifferentParametersTypes() {
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2, 2, "TeSt").returns(4, "test").check();
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2.2, 2.2, "TeSt").returns(4.4, "test").check();

        assertQuery("SELECT COALESCE(?, ?)").withParams(null, null).returns(null).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(null, 13).returns(13).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams("a", 10).returns("a").check();
        assertQuery("SELECT COALESCE(?, ?)").withParams("a", "b").returns("a").check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(22, 33).returns(22).check();

        assertQuery("SELECT UPPER(TYPEOF(?))").withParams(1).returns("INTEGER").check();
        assertQuery("SELECT UPPER(TYPEOF(?))").withParams(1d).returns("DOUBLE").check();
    }

    // After fix the mute reason need to merge the test with above testWithDifferentParametersTypes
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18369")
    @Test
    public void testWithDifferentParametersTypes2() {
        assertQuery("SELECT COALESCE(?, ?)").withParams(12.2, "b").returns(12.2).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(12, "b").returns(12).check();
    }

    @Test
    public void testExplainWithDynamicParameters() {
        assertQuery("EXPLAIN PLAN FOR SELECT * FROM t1 WHERE id > ?").skipExplain(true).check();
    }

    @Test
    public void testUnspecifiedDynamicParameterInSelectList() {
        assertIllegalDynamicParameter("SELECT COALESCE(?)");
        assertIllegalDynamicParameter("SELECT * FROM (VALUES(1, 2, ?)) t1");
    }

    @Test
    public void testUnspecifiedDynamicParameterInInsert() {
        // not nullable column
        assertIllegalDynamicParameter("INSERT INTO t1 VALUES(1, ?, 3)");
        // nullable column
        assertIllegalDynamicParameter("INSERT INTO t1 VALUES(1, 2, ?)");
    }

    @Test
    public void testUnspecifiedDynamicParameterInUpdate() {
        // column value
        assertIllegalDynamicParameter("UPDATE t1 SET val1=? WHERE id = 1");
        // predicate
        assertIllegalDynamicParameter("UPDATE t1 SET val1=10 WHERE id = ?");
    }

    @Test
    public void testUnspecifiedDynamicParameterInDelete() {
        assertIllegalDynamicParameter("DELETE FROM t1 WHERE id = ? AND val1=1");
    }

    @Test
    public void testUnexpectedNumberOfParametersInSelectList() {
        assertUnexpectedNumberOfParameters("SELECT 1", 1);
        assertUnexpectedNumberOfParameters("SELECT ?", 1, 2);
    }

    @Test
    public void testUnexpectedNumberOfParametersInSelectInInsert() {
        assertUnexpectedNumberOfParameters("INSERT INTO t1 VALUES(1, 2, 3)", 1);
        assertUnexpectedNumberOfParameters("INSERT INTO t1 VALUES(1, 2, ?)", 1, 2);
    }

    @Test
    public void testUnexpectedNumberOfParametersInDelete() {
        assertUnexpectedNumberOfParameters("DELETE FROM t1 WHERE id = 1 AND val1=1", 1);
        assertUnexpectedNumberOfParameters("DELETE FROM t1 WHERE id = ? AND val1=1", 1, 2);
    }

    @Test
    public void testUnspecifiedDynamicParameterInLimitOffset() {
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT 1", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT ?", 1, 2);

        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT 1 OFFSET 1", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT 1 OFFSET ?", 1, 2);

        assertUnexpectedNumberOfParameters("SELECT * FROM t1 OFFSET 1", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 OFFSET ?", 1, 2);
    }

    private Object generateValueByType(int i, ColumnType type) {
        switch (type) {
            case BOOLEAN:
                return i % 2 == 0;
            case INT8:
                return (byte) i;
            case INT16:
                return (short) i;
            case INT32:
                return i;
            case INT64:
                return (long) i;
            case FLOAT:
                return (float) i + ((float) i / 1000);
            case DOUBLE:
                return (double) i + ((double) i / 1000);
            case STRING:
                return "str_" + i;
            case BYTE_ARRAY:
                return new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2)};
            case NULL:
                return null;
            case DECIMAL:
                return BigDecimal.valueOf((double) i + ((double) i / 1000));
            case NUMBER:
                return BigInteger.valueOf(i);
            case UUID:
                return new UUID(i, i);
            case BITMASK:
                return new byte[]{(byte) i};
            case DURATION:
                return Duration.ofNanos(i);
            case DATETIME:
                return LocalDateTime.of(
                        (LocalDate) generateValueByType(i, ColumnType.DATE),
                        (LocalTime) generateValueByType(i, ColumnType.TIME)
                );
            case TIMESTAMP:
                return Instant.from((LocalDateTime) generateValueByType(i, ColumnType.DATETIME));
            case DATE:
                return LocalDate.of(2022, 01, 01).plusDays(i);
            case TIME:
                return LocalTime.of(0, 00, 00).plusSeconds(i);
            case PERIOD:
                return Period.of(i % 2, i % 12, i % 29);
            default:
                throw new IllegalArgumentException("unsupported type " + type);
        }
    }

    private static void assertIllegalDynamicParameter(String query, Object... params) {
        CompletionException clientError = assertThrows(CompletionException.class, () -> {
            assertQuery(query).withParams(params).check();
        }, "query: " + query);

        CalciteContextException err = assertInstanceOf(CalciteContextException.class, clientError.getCause());
        assertThat("query: " + query, err.getMessage(), containsString("Illegal use of dynamic parameter"));
    }

    private static void assertUnexpectedNumberOfParameters(String query, Object... params) {
        CompletionException clientError = assertThrows(CompletionException.class, () -> {
            assertQuery(query).withParams(params).check();
        }, "query: " + query);

        CalciteContextException err = assertInstanceOf(CalciteContextException.class, clientError.getCause());
        assertThat("query: " + query, err.getMessage(), containsString("Unexpected number of query parameters"));
    }
}
