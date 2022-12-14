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

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.sql.SqlColumnType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Dynamic parameters checks. */
public class ItDynamicParameterTest extends AbstractBasicIntegrationTest {
    private static final Object UNSUPPORTED_SIGN = new Object();

    @Test
    void testMetadataTypesForDynamicParameters() {
        int i = 1;
        for (SqlColumnType type : SqlColumnType.values()) {
            Object param = generateValueByType(i++, type);
            if (param == UNSUPPORTED_SIGN) {
                continue;
            }

            assertQuery("SELECT typeof(?)").withParams(param).returns(toSqlType(type)).check();
            ;
            assertQuery("SELECT ?").withParams(param).returns(param).columnMetadata(new MetadataMatcher().type(type)).check();
        }
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
    }

    // After fix the mute reason need to merge the test with above testWithDifferentParametersTypes
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18369")
    @Test
    public void testWithDifferentParametersTypes2() {
        assertQuery("SELECT COALESCE(?, ?)").withParams(12.2, "b").returns(12.2).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(12, "b").returns(12).check();
    }

    private Object generateValueByType(int i, SqlColumnType type) {
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

            // https://issues.apache.org/jira/browse/IGNITE-18258
            //            case DECIMAL:
            //                return BigDecimal.valueOf((double) i + ((double) i / 1000));

            // https://issues.apache.org/jira/browse/IGNITE-18414
            //            case NUMBER:
            //                return BigInteger.valueOf(i);
            //https://issues.apache.org/jira/browse/IGNITE-18415
            //            case UUID:
            //                return new UUID(i, i);
            //
            //            case BITMASK:
            //                return new byte[]{(byte) i};

            // https://issues.apache.org/jira/browse/IGNITE-18345
            //            case DURATION:
            //                return Duration.ofNanos(i);
            //            case DATETIME:
            //                return LocalDateTime.of(
            //                        (LocalDate) generateValueByType(i, SqlColumnType.DATE),
            //                        (LocalTime) generateValueByType(i, SqlColumnType.TIME)
            //                );
            //            case TIMESTAMP:
            //                return Instant.from((LocalDateTime) generateValueByType(i, SqlColumnType.DATETIME));
            //            case DATE:
            //                return LocalDate.of(2022, 01, 01).plusDays(i);
            //            case TIME:
            //                return LocalTime.of(0, 00, 00).plusSeconds(i);
            //            case PERIOD:
            //                return Period.of(i%2, i%12, i%29);

            default:
                return UNSUPPORTED_SIGN;
        }

    }
}
