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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

/** Dynamic parameters checks. */
public class ItDynamicParameterTest extends ClusterPerClassIntegrationTest {
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
            //    https://issues.apache.org/jira/browse/IGNITE-18789
            //    https://issues.apache.org/jira/browse/IGNITE-18414
            names = {"DECIMAL", "NUMBER", "BITMASK", "DURATION", "PERIOD"},
            mode = Mode.EXCLUDE
    )
    void testMetadataTypesForDynamicParameters(ColumnType type) {
        Object param = generateValueByType(RND.nextInt(), type);
        List<List<Object>> ret = sql("SELECT typeof(?)", param);
        String type0 = (String) ret.get(0).get(0);

        assertTrue(type0.startsWith(toSqlType(type)));
        assertQuery("SELECT ?").withParams(param).returns(param).columnMetadata(new MetadataMatcher().type(type)).check();
    }

    @Test
    public void testDynamicParameters() {
        assertQuery("SELECT COALESCE(null, ?)").withParams(13).returns(13).check();
        assertQuery("SELECT LOWER(?)").withParams("ASD").returns("asd").check();
        assertQuery("SELECT POWER(?, ?)").withParams(2, 3).returns(8d).check();
        assertQuery("SELECT SQRT(?)").withParams(4d).returns(2d).check();
        assertQuery("SELECT ?").withParams("asd").returns("asd").check();
        assertQuery("SELECT ? % ?").withParams(11, 10).returns(1).check();
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2, 2, "TeSt").returns(4, "test").check();
        assertQuery("SELECT LOWER(?), ? + ? ").withParams("TeSt", 2, 2).returns("test", 4).check();

        createAndPopulateTable();
        assertQuery("SELECT name LIKE '%' || ? || '%' FROM person where name is not null").withParams("go").returns(true).returns(false)
                .returns(false).returns(false).check();

        assertQuery("SELECT id FROM person WHERE name LIKE ? ORDER BY id LIMIT ?").withParams("I%", 1).returns(0).check();
        assertQuery("SELECT id FROM person WHERE name LIKE ? ORDER BY id LIMIT ? OFFSET ?").withParams("I%", 1, 1).returns(2).check();
        assertQuery("SELECT id from person WHERE salary<? and id<?").withParams(15, 3).returns(0).check();

        IgniteTestUtils.assertThrowsWithCause(() -> sql("SELECT LAST_DAY(?)", Date.valueOf("2022-01-01")),
                SqlException.class, "Unsupported dynamic parameter defined");

        LocalDate date1 = LocalDate.parse("2022-01-01");
        LocalDate date2 = LocalDate.parse("2022-01-31");

        assertQuery("SELECT LAST_DAY(?)").withParams(date1).returns(date2).check();
    }

    /**
     * Tests a nested CASE WHEN statement using various combinations of dynamic parameter values,
     * including the {@code NULL} value for different operands.
     */
    @Test
    public void testNestedCase() {
        sql("CREATE TABLE TBL1(ID INT PRIMARY KEY, VAL VARCHAR, NUM INT)");

        assertTrue(sql(
                "select case when (_T0.VAL is not distinct from ?) then 0 else (case when (_T0.VAL > ?) then 1 else -1 end) end "
                        + "from PUBLIC.TBL1 as _T0", "abc", "abc").isEmpty());

        sql("INSERT INTO TBL1 VALUES"
                + " (0, 'abc', 0),"
                + " (1, 'abc', NULL),"
                + " (2, NULL, 0)");

        String sql = "select case when (VAL = ?) then 0 else (case when (NUM = ?) then ? else ? end) end ";
        assertQuery(sql + "from TBL1").withParams("abc", 0, 1, null).returns(0).returns(0).returns(1).check();
        assertQuery(sql + "IS NULL from TBL1").withParams("abc", 0, 1, null).returns(false).returns(false).returns(false).check();

        sql = "select case when (VAL = ?) then 0 else (case when (NUM IS NULL) then ? else ? end) end ";
        // NULL in THEN operand.
        assertQuery(sql + "from TBL1").withParams("diff", null, 1).returns(1).returns((Object) null).returns(1).check();
        assertQuery(sql + "IS NULL from TBL1").withParams("diff", null, 1).returns(false).returns(true).returns(false).check();
        // NULL in ELSE operand.
        assertQuery(sql + "from TBL1").withParams("diff", 1, null).returns((Object) null).returns(1).returns((Object) null).check();
        assertQuery(sql + "IS NULL from TBL1").withParams("diff", 1, null).returns(true).returns(false).returns(true).check();

        sql = "select case when (VAL is not distinct from ?) then '0' else (case when (NUM is not distinct from ?) then ? else ? end) end ";
        assertQuery(sql + "from TBL1").withParams(null, null, "1", null).returns((Object) null).returns("1").returns("0").check();
        assertQuery(sql + "IS NULL from TBL1").withParams(null, null, "1", null).returns(true).returns(false).returns(false).check();
    }

    /** Need to test the same query with different type of parameters to cover case with check right plans cache work. **/
    @Test
    public void testWithDifferentParametersTypes() {
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2, 2, "TeSt").returns(4, "test").check();
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2.2, 2.2, "TeSt").returns(4.4, "test").check();

        assertQuery("SELECT COALESCE(?, ?)").withParams(null, null).returns(null).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(null, 13).returns(13).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams("a", "b").returns("a").check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(22, 33).returns(22).check();

        assertQuery("SELECT UPPER(TYPEOF(?))").withParams(1).returns("INTEGER").check();
        assertQuery("SELECT UPPER(TYPEOF(?))").withParams(1d).returns("DOUBLE").check();

        assertQuery("SELECT ?::INTEGER = '8'").withParams(8).returns(true).check();
    }

    /**
     * SQL 2016, clause 9.5: Mixing types in CASE/COALESCE expressions is illegal.
     */
    @Test
    public void testWithDifferentParametersTypesMismatch() {
        assertThrows(CalciteContextException.class, () -> assertQuery("SELECT COALESCE(12.2, ?)").withParams("b").returns(12.2).check());
        assertThrows(CalciteContextException.class, () -> assertQuery("SELECT COALESCE(?, ?)").withParams(12.2, "b").returns(12.2).check());
    }

    @Test
    public void testUnspecifiedDynamicParameterInExplain() {
        assertUnexpectedNumberOfParameters("EXPLAIN PLAN FOR SELECT * FROM t1 WHERE id > ?");
    }

    @Test
    public void testDynamicParametersInExplain() {
        sql("EXPLAIN PLAN FOR SELECT * FROM t1 WHERE id > ?", 1);
    }

    @Test
    public void testUnspecifiedDynamicParameterInSelectList() {
        assertUnexpectedNumberOfParameters("SELECT COALESCE(?)");
        assertUnexpectedNumberOfParameters("SELECT * FROM (VALUES(1, 2, ?)) t1");
    }

    @Test
    public void testUnspecifiedDynamicParameterInInsert() {
        assertUnexpectedNumberOfParameters("INSERT INTO t1 VALUES(1, 2, ?)");
    }

    @Test
    public void testUnspecifiedDynamicParameterInUpdate() {
        // column value
        assertUnexpectedNumberOfParameters("UPDATE t1 SET val1=? WHERE id = 1");
        // predicate
        assertUnexpectedNumberOfParameters("UPDATE t1 SET val1=10 WHERE id = ?");
    }

    @Test
    public void testUnspecifiedDynamicParameterInDelete() {
        assertUnexpectedNumberOfParameters("DELETE FROM t1 WHERE id = ? AND val1=1");
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
                return Instant.from(ZonedDateTime.of((LocalDateTime) generateValueByType(i, ColumnType.DATETIME), ZoneId.systemDefault()));
            case DATE:
                return LocalDate.of(2022, 01, 01).plusDays(i % 30);
            case TIME:
                return LocalTime.of(0, 00, 00).plusSeconds(i % 1000);
            case PERIOD:
                return Period.of(i % 2, i % 12, i % 29);
            default:
                throw new IllegalArgumentException("unsupported type " + type);
        }
    }

    private static String toSqlType(ColumnType columnType) {
        switch (columnType) {
            case BOOLEAN:
                return SqlTypeName.BOOLEAN.getName();
            case INT8:
                return SqlTypeName.TINYINT.getName();
            case INT16:
                return SqlTypeName.SMALLINT.getName();
            case INT32:
                return SqlTypeName.INTEGER.getName();
            case INT64:
                return SqlTypeName.BIGINT.getName();
            case FLOAT:
                return SqlTypeName.REAL.getName();
            case DOUBLE:
                return SqlTypeName.DOUBLE.getName();
            case DECIMAL:
                return SqlTypeName.DECIMAL.getName();
            case DATE:
                return SqlTypeName.DATE.getName();
            case TIME:
                return SqlTypeName.TIME.getName();
            case DATETIME:
                return SqlTypeName.TIMESTAMP.getName();
            case TIMESTAMP:
                return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getName();
            case UUID:
                return UuidType.NAME;
            case STRING:
                return SqlTypeName.VARCHAR.getName();
            case BYTE_ARRAY:
                return SqlTypeName.VARBINARY.getName();
            case NUMBER:
                return SqlTypeName.INTEGER.getName();
            case NULL:
                return SqlTypeName.NULL.getName();
            default:
                throw new IllegalArgumentException("Unsupported type " + columnType);
        }
    }

    private static void assertUnexpectedNumberOfParameters(String query, Object... params) {
        SqlException err = assertThrows(SqlException.class, () -> {
            assertQuery(query).withParams(params).check();
        }, "query: " + query);

        assertThat("query: " + query, err.getMessage(), containsString("Unexpected number of query parameters"));
    }
}
