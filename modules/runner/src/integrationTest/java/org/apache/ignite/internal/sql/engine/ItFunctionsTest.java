/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.function.LongFunction;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test Ignite SQL functions.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItFunctionsTest extends AbstractBasicIntegrationTest {
    private static final Object[] NULL_RESULT = new Object[] { null };

    @Test
    public void testLength() {
        assertQuery("SELECT LENGTH('TEST')").returns(4).check();
        assertQuery("SELECT LENGTH(NULL)").returns(NULL_RESULT).check();
    }

    @Test
    public void testCurrentDateTimeTimeStamp() {
        checkDateTimeQuery("SELECT CURRENT_DATE", Date::new);
        checkDateTimeQuery("SELECT CURRENT_TIME", Time::new);
        checkDateTimeQuery("SELECT CURRENT_TIMESTAMP", Timestamp::new);
        checkDateTimeQuery("SELECT LOCALTIME", Time::new);
        checkDateTimeQuery("SELECT LOCALTIMESTAMP", Timestamp::new);
        checkDateTimeQuery("SELECT {fn CURDATE()}", Date::new);
        checkDateTimeQuery("SELECT {fn CURTIME()}", Time::new);
        checkDateTimeQuery("SELECT {fn NOW()}", Timestamp::new);
    }

    private static <T> void checkDateTimeQuery(String sql, LongFunction<T> func) {
        while (true) {
            long tsBeg = System.currentTimeMillis();

            List<List<?>> res = sql(sql);

            long tsEnd = System.currentTimeMillis();

            assertEquals(1, res.size());
            assertEquals(1, res.get(0).size());

            String strBeg = func.apply(tsBeg).toString();
            String strEnd = func.apply(tsEnd).toString();

            // Date changed, time comparison may return wrong result.
            if (strBeg.compareTo(strEnd) > 0) {
                continue;
            }

            String strRes = res.get(0).get(0).toString();

            assertTrue(strBeg.compareTo(strRes) <= 0);
            assertTrue(strEnd.compareTo(strRes) >= 0);

            return;
        }
    }

    @Test
    public void testRange() {
        assertQuery("SELECT * FROM table(system_range(1, 4))")
                .returns(1L)
                .returns(2L)
                .returns(3L)
                .returns(4L)
                .check();

        assertQuery("SELECT * FROM table(system_range(1, 4, 2))")
                .returns(1L)
                .returns(3L)
                .check();

        assertQuery("SELECT * FROM table(system_range(4, 1, -1))")
                .returns(4L)
                .returns(3L)
                .returns(2L)
                .returns(1L)
                .check();

        assertQuery("SELECT * FROM table(system_range(4, 1, -2))")
                .returns(4L)
                .returns(2L)
                .check();

        assertEquals(0, sql("SELECT * FROM table(system_range(4, 1))").size());

        assertEquals(0, sql("SELECT * FROM table(system_range(null, 1))").size());

        IgniteException ex = assertThrows(IgniteException.class,
                () -> sql("SELECT * FROM table(system_range(1, 1, 0))"), "Increment can't be 0");

        assertTrue(
                ex.getCause() instanceof IllegalArgumentException,
                IgniteStringFormatter.format(
                        "Expected cause is {}, but was {}",
                        IllegalArgumentException.class.getSimpleName(),
                        ex.getCause() == null ? null : ex.getCause().getClass().getSimpleName()
                )
        );

        assertEquals("Increment can't be 0", ex.getCause().getMessage());
    }

    @Test
    public void testRangeWithCache() {
        TableDefinition tblDef = SchemaBuilders.tableBuilder("PUBLIC", "TEST")
                .columns(
                        SchemaBuilders.column("ID", ColumnType.INT32).build(),
                        SchemaBuilders.column("VAL", ColumnType.INT32).build()
                )
                .withPrimaryKey("ID")
                .build();

        String tblName = tblDef.canonicalName();

        RecordView<Tuple> tbl = CLUSTER_NODES.get(0).tables().createTable(tblDef.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(tblDef, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        ).recordView();

        try {

            for (int i = 0; i < 100; i++) {
                tbl.insert(null, Tuple.create().set("ID", i).set("VAL", i));
            }

            // Correlated INNER join.
            assertQuery("SELECT t.val FROM test t WHERE t.val < 5 AND "
                    + "t.id in (SELECT x FROM table(system_range(t.val, t.val))) ")
                    .returns(0)
                    .returns(1)
                    .returns(2)
                    .returns(3)
                    .returns(4)
                    .check();

            // Correlated LEFT joins.
            assertQuery("SELECT t.val FROM test t WHERE t.val < 5 AND "
                    + "EXISTS (SELECT x FROM table(system_range(t.val, t.val)) WHERE mod(x, 2) = 0) ")
                    .returns(0)
                    .returns(2)
                    .returns(4)
                    .check();

            assertQuery("SELECT t.val FROM test t WHERE t.val < 5 AND "
                    + "NOT EXISTS (SELECT x FROM table(system_range(t.val, t.val)) WHERE mod(x, 2) = 0) ")
                    .returns(1)
                    .returns(3)
                    .check();

            assertQuery("SELECT t.val FROM test t WHERE "
                    + "EXISTS (SELECT x FROM table(system_range(t.val, null))) ")
                    .check();

            // Non-correlated join.
            assertQuery("SELECT t.val FROM test t JOIN table(system_range(1, 50)) as r ON t.id = r.x "
                    + "WHERE mod(r.x, 10) = 0")
                    .returns(10)
                    .returns(20)
                    .returns(30)
                    .returns(40)
                    .returns(50)
                    .check();
        } finally {
            CLUSTER_NODES.get(0).tables().dropTable(tblName);
        }
    }

    @Test
    public void testPercentRemainder() {
        assertQuery("SELECT 3 % 2").returns(1).check();
        assertQuery("SELECT 4 % 2").returns(0).check();
        assertQuery("SELECT NULL % 2").returns(NULL_RESULT).check();
        assertQuery("SELECT 3 % NULL::int").returns(NULL_RESULT).check();
        assertQuery("SELECT 3 % NULL").returns(NULL_RESULT).check();
    }

    @Test
    public void testNullFunctionArguments() {
        // Don't infer result data type from arguments (result is always INTEGER_NULLABLE).
        assertQuery("SELECT ASCII(NULL)").returns(NULL_RESULT).check();
        // Inferring result data type from first STRING argument.
        assertQuery("SELECT REPLACE(NULL, '1', '2')").returns(NULL_RESULT).check();
        // Inferring result data type from both arguments.
        assertQuery("SELECT MOD(1, null)").returns(NULL_RESULT).check();
        // Inferring result data type from first NUMERIC argument.
        assertQuery("SELECT TRUNCATE(NULL, 0)").returns(NULL_RESULT).check();
        // Inferring arguments data types and then inferring result data type from all arguments.
        assertQuery("SELECT FALSE AND NULL").returns(false).check();
    }

    @Test
    public void testReplace() {
        assertQuery("SELECT REPLACE('12341234', '1', '55')").returns("5523455234").check();
        assertQuery("SELECT REPLACE(NULL, '1', '5')").returns(NULL_RESULT).check();
        assertQuery("SELECT REPLACE('1', NULL, '5')").returns(NULL_RESULT).check();
        assertQuery("SELECT REPLACE('11', '1', NULL)").returns(NULL_RESULT).check();
        assertQuery("SELECT REPLACE('11', '1', '')").returns("").check();
    }

    @Test
    public void testMonthnameDayname() {
        assertQuery("SELECT MONTHNAME(DATE '2021-01-01')").returns("January").check();
        assertQuery("SELECT DAYNAME(DATE '2021-01-01')").returns("Friday").check();
    }

    @Test
    public void testRegex() {
        assertQuery("SELECT 'abcd' ~ 'ab[cd]'").returns(true).check();
        assertQuery("SELECT 'abcd' ~ 'ab[cd]$'").returns(false).check();
        assertQuery("SELECT 'abcd' ~ 'ab[CD]'").returns(false).check();
        assertQuery("SELECT 'abcd' ~* 'ab[cd]'").returns(true).check();
        assertQuery("SELECT 'abcd' ~* 'ab[cd]$'").returns(false).check();
        assertQuery("SELECT 'abcd' ~* 'ab[CD]'").returns(true).check();
        assertQuery("SELECT 'abcd' !~ 'ab[cd]'").returns(false).check();
        assertQuery("SELECT 'abcd' !~ 'ab[cd]$'").returns(true).check();
        assertQuery("SELECT 'abcd' !~ 'ab[CD]'").returns(true).check();
        assertQuery("SELECT 'abcd' !~* 'ab[cd]'").returns(false).check();
        assertQuery("SELECT 'abcd' !~* 'ab[cd]$'").returns(true).check();
        assertQuery("SELECT 'abcd' !~* 'ab[CD]'").returns(false).check();
        assertQuery("SELECT null ~ 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' ~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null ~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null ~* 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' ~* null").returns(NULL_RESULT).check();
        assertQuery("SELECT null ~* null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~ 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' !~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~* 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' !~* null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~* null").returns(NULL_RESULT).check();
        assertThrows(IgniteException.class, () -> sql("SELECT 'abcd' ~ '[a-z'"));
    }

    @Test
    public void testTypeOf() {
        assertQuery("SELECT TYPEOF(1)").returns("INTEGER").check();
        assertQuery("SELECT TYPEOF(1.1::DOUBLE)").returns("DOUBLE").check();
        assertQuery("SELECT TYPEOF(1.1::DECIMAL(3, 2))").returns("DECIMAL(3, 2)").check();
        assertQuery("SELECT TYPEOF('a')").returns("CHAR(1)").check();
        assertQuery("SELECT TYPEOF('a'::varchar(1))").returns("VARCHAR(1)").check();
        assertQuery("SELECT TYPEOF(NULL)").returns("NULL").check();
        assertQuery("SELECT TYPEOF(NULL::VARCHAR(100))").returns("VARCHAR(100)").check();
        try {
            sql("SELECT TYPEOF()");
        } catch (Throwable e) {
            assertTrue(IgniteTestUtils.hasCause(e, SqlValidatorException.class, "Invalid number of arguments"));
        }

        try {
            sql("SELECT TYPEOF(1, 2)");
        } catch (Throwable e) {
            assertTrue(IgniteTestUtils.hasCause(e, SqlValidatorException.class, "Invalid number of arguments"));
        }
    }
}
