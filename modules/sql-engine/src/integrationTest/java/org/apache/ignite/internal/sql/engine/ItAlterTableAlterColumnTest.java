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
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.lang.ErrorGroups.Sql.CONSTRAINT_VIOLATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration test for ALTER TABLE ALTER COLUMN command.
 *
 * <p>SQL F381 feature. Extended schema manipulation.
 */
public class ItAlterTableAlterColumnTest extends BaseSqlIntegrationTest {
    @AfterEach
    public void dropTables() {
        dropAllTables();
    }

    @ParameterizedTest
    @MethodSource("supportedTypesTransitions")
    public void testSupportedColumnTypeChange(String initType, Object initVal, ColumnType alterColType, String alterType, Object alterVal) {
        sql(format("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY, val {})", initType));
        sql(format("INSERT INTO t VALUES(1, {})", initVal));
        List<List<Object>> res = sql("SELECT val from t WHERE id=1");
        checkTypeAwareness(initVal, res);

        sql(format("ALTER TABLE t ALTER COLUMN val SET DATA TYPE {}", initType));
        sql(format("UPDATE t SET val={} WHERE id=1", initVal));

        sql(format("ALTER TABLE t ALTER COLUMN val SET DATA TYPE {}", alterType));
        sql(format("INSERT INTO t VALUES(2, {})", alterVal));
        res = sql("SELECT val from t WHERE id=2");
        checkTypeAwareness(alterVal, res);

        sql(format("UPDATE t SET val={} WHERE id=1", alterVal));
        res = sql("SELECT val from t WHERE id=1");
        checkTypeAwareness(alterVal, res);

        assertQuery("select val from t").columnMetadata(
                new MetadataMatcher().name("VAL").type(alterColType)
        ).check();
    }

    private static void checkTypeAwareness(Object val, List<List<Object>> res) {
        if (val instanceof String) {
            if (((String) val).startsWith("x'")) {
                String initVal0 = ((String) val).substring(2, ((String) val).length() - 1);
                byte[] bytes = StringUtils.fromHexString(initVal0);
                assertArrayEquals(bytes, (byte[]) res.get(0).get(0));
            } else {
                String initVal0 = ((String) val).substring(1, ((String) val).length() - 1);
                assertEquals(initVal0, res.get(0).get(0));
            }
        } else {
            assertEquals(val, res.get(0).get(0));
        }
    }

    private static Stream<Arguments> supportedTypesTransitions() {
        List<Arguments> arguments = new ArrayList<>();

        arguments.add(Arguments.of("TINYINT", Byte.MAX_VALUE, ColumnType.INT16, "SMALLINT", Short.MAX_VALUE));
        arguments.add(Arguments.of("SMALLINT", Short.MAX_VALUE, ColumnType.INT32, "INT", Integer.MAX_VALUE));
        arguments.add(Arguments.of("INT", Integer.MAX_VALUE, ColumnType.INT64, "BIGINT", Long.MAX_VALUE));
        arguments.add(Arguments.of("FLOAT", Float.MAX_VALUE, ColumnType.DOUBLE, "DOUBLE", Double.MAX_VALUE));
        arguments.add(Arguments.of("FLOAT", Float.MIN_VALUE, ColumnType.DOUBLE, "DOUBLE", Double.MIN_VALUE));
        arguments.add(Arguments.of("VARCHAR(10)", "'" + "c".repeat(10) + "'", ColumnType.STRING,
                "VARCHAR(20)", "'" + "c".repeat(20) + "'"));
        arguments.add(Arguments.of("VARBINARY(1)", "x'01'", ColumnType.BYTE_ARRAY,
                "VARBINARY(2)", "x'0102'"));

        return arguments.stream();
    }

    @Test
    public void testDecimalIncreasePrecision() {
        sql("CREATE TABLE t (id INT PRIMARY KEY, val DECIMAL(6, 5))");
        sql(format("INSERT INTO t VALUES(1, {})", 1));
        sql("ALTER TABLE t ALTER COLUMN val SET DATA TYPE DECIMAL(16, 5)");

        sql(format("INSERT INTO t VALUES(2, {})", Integer.MAX_VALUE));
        List<List<Object>> res = sql("SELECT val from t WHERE id=2");
        assertEquals(0, new BigDecimal(Integer.MAX_VALUE).compareTo((BigDecimal) res.get(0).get(0)));

        sql(format("UPDATE t SET val={} WHERE id=1", Integer.MAX_VALUE));
        res = sql("SELECT val from t WHERE id=1");
        assertEquals(0, new BigDecimal(Integer.MAX_VALUE).compareTo((BigDecimal) res.get(0).get(0)));
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testDecimalDecreasePrecision() {
        sql("CREATE TABLE t1 (ID INT PRIMARY KEY, DECIMAL_C2 DECIMAL(2) NOT NULL)");

        // Increase precision.
        sql("ALTER TABLE t1 ALTER COLUMN DECIMAL_C2 SET DATA TYPE DECIMAL(3)");

        sql("INSERT INTO t1 VALUES (1, 123)");
        assertThrowsSqlException(Sql.RUNTIME_ERR, "Numeric field overflow",
                () -> sql("INSERT INTO t1 VALUES (2, 1234)"));

        // Increase precision and drop NOT NULL.
        sql("ALTER TABLE t1 ALTER COLUMN DECIMAL_C2 SET DATA TYPE DECIMAL NULL");
        sql("INSERT INTO t1 VALUES (2, 1234), (3, NULL)");

        assertThrowsSqlException(STMT_VALIDATION_ERR, "Decreasing the precision",
                () -> sql("ALTER TABLE t1 ALTER COLUMN DECIMAL_C2 SET DATA TYPE DECIMAL(3)"));
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testDecimalChangeScale() {
        sql("CREATE TABLE t (id INT PRIMARY KEY, val DECIMAL(6, 5))");

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Changing the scale for column of type",
                () -> sql("ALTER TABLE t ALTER COLUMN val SET DATA TYPE DECIMAL(6, 6)"));
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testChangeNullability() {
        sql("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR(10) NOT NULL, val2 VARCHAR(10) NOT NULL)");
        sql("ALTER TABLE t ALTER COLUMN val SET DATA TYPE VARCHAR(10)");

        assertThrowsSqlException(CONSTRAINT_VIOLATION_ERR, "does not allow NULLs",
                () -> sql("INSERT INTO t VALUES(1, NULL, NULL)"));

        // NOT NULL -> NOT NULL is allowed.
        sql("ALTER TABLE t ALTER COLUMN val SET DATA TYPE VARCHAR(11) NOT NULL");
        // NOT NULL -> NULL is allowed.
        sql("ALTER TABLE t ALTER COLUMN val2 SET DATA TYPE VARCHAR(11) NULL");

        // NOT NULL -> NULL is allowed.
        sql("ALTER TABLE t ALTER COLUMN val DROP NOT NULL");
        // NULL -> NULL is allowed.
        sql("ALTER TABLE t ALTER COLUMN val2 DROP NOT NULL");

        sql("INSERT INTO t VALUES(1, NULL, NULL)");

        // NULL -> NOT NULL is forbidden.
        assertThrowsSqlException(STMT_VALIDATION_ERR, "Adding NOT NULL constraint is not allowed",
                () -> sql("ALTER TABLE t ALTER COLUMN val SET NOT NULL"));
        assertThrowsSqlException(STMT_VALIDATION_ERR, "Adding NOT NULL constraint is not allowed",
                () -> sql("ALTER TABLE t ALTER COLUMN val2 SET NOT NULL"));
        assertThrowsSqlException(STMT_VALIDATION_ERR, "Adding NOT NULL constraint is not allowed",
                () -> sql("ALTER TABLE t ALTER COLUMN val SET DATA TYPE VARCHAR(100) NOT NULL"));
        assertThrowsSqlException(STMT_VALIDATION_ERR, "Adding NOT NULL constraint is not allowed",
                () -> sql("ALTER TABLE t ALTER COLUMN val2 SET DATA TYPE VARCHAR(100) NOT NULL"));
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testChangeColumnDefault() {
        sql("CREATE TABLE test("
                + "id BIGINT PRIMARY KEY, "
                + "valint INTEGER, "
                + "valdate DATE,"
                + "valtime TIME(3),"
                + "valts TIMESTAMP(3),"
                + "valstr VARCHAR,"
                + "valbin VARBINARY"
                + ")");

        sql("ALTER TABLE test ALTER COLUMN valint SET DEFAULT 1");
        sql("ALTER TABLE test ALTER COLUMN valdate SET DEFAULT DATE '2001-12-21'");
        sql("ALTER TABLE test ALTER COLUMN valtime SET DEFAULT TIME '11:22:33.444555'");
        sql("ALTER TABLE test ALTER COLUMN valts SET DEFAULT TIMESTAMP '2001-12-21 11:22:33.444555'");
        sql("ALTER TABLE test ALTER COLUMN valstr SET DEFAULT 'string'");
        sql("ALTER TABLE test ALTER COLUMN valbin SET DEFAULT x'ff'");

        sql("INSERT INTO test (id) VALUES (0)");

        assertQuery("SELECT * FROM test")
                .returns(0L,
                        1,
                        LocalDate.of(2001, Month.DECEMBER, 21),
                        LocalTime.of(11, 22, 33, 444000000),
                        LocalDateTime.of(2001, Month.DECEMBER, 21, 11, 22, 33, 444000000),
                        "string",
                        new byte[]{(byte) 0xff}
                )
                .check();
    }

    @Test
    public void setDefault() {
        sql("CREATE TABLE t (id int PRIMARY KEY, val int)");

        for (String col : Arrays.asList("id", "val")) {
            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Non-constant default cannot be assigned after table creation.",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DEFAULT rand_uuid", col))
            );

            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Non-constant default cannot be assigned after table creation.",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DEFAULT rand_uuid()", col))
            );

            // Compound id
            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Unsupported default expression: A.B.C",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DEFAULT a.b.c", col))
            );

            // Expression
            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Unsupported default expression: 1 / 0",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DEFAULT (1/0)", col))
            );

            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Unsupported default expression: 1 / 0",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DEFAULT 1/0", col))
            );

            // SELECT

            assertThrowsSqlException(
                    STMT_PARSE_ERR,
                    "Query expression encountered in illegal context",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DEFAULT (SELECT 1000)", col))
            );

            assertThrowsSqlException(
                    STMT_PARSE_ERR,
                    "Query expression encountered in illegal context",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DEFAULT (SELECT count(*) FROM xyz)", col))
            );
        }
    }

    @Test
    public void dropDefaultNonNullableColumn() {
        sql("CREATE TABLE t (id INT PRIMARY KEY)");
        sql("INSERT INTO t VALUES (0)");
        sql("ALTER TABLE t ADD COLUMN C1 VARCHAR NOT NULL DEFAULT 'a'");
        sql("ALTER TABLE t ADD COLUMN C2 VARCHAR NOT NULL DEFAULT 'b'");

        sql("ALTER TABLE t ALTER COLUMN C1 DROP DEFAULT");
        sql("ALTER TABLE t ALTER COLUMN C2 SET DEFAULT NULL");

        assertThrowsSqlException(Sql.CONSTRAINT_VIOLATION_ERR, "Column 'C1' does not allow NULL",
                () -> sql("INSERT INTO t (id, c2) VALUES (1, 'c')"));
        assertThrowsSqlException(Sql.CONSTRAINT_VIOLATION_ERR, "Column 'C2' does not allow NULL",
                () -> sql("INSERT INTO t (id, c1) VALUES (1, 'c')"));

        sql("INSERT INTO t (id, c1, c2) VALUES (1, 'a1', 'b1')");

        sql("ALTER TABLE t ALTER COLUMN C1 SET DEFAULT 'a2'");
        sql("ALTER TABLE t ALTER COLUMN C2 SET DEFAULT 'b2'");

        sql("INSERT INTO t (id) VALUES (2)");

        assertQuery("SELECT id, c1, c2 FROM t ORDER BY id")
                .returns(0, "a", "b")
                .returns(1, "a1", "b1")
                .returns(2, "a2", "b2")
                .check();
    }

    @Test
    public void dropDefaultNullableColumn() {
        sql("CREATE TABLE t (id INT PRIMARY KEY)");
        sql("INSERT INTO t VALUES (0)");
        sql("ALTER TABLE t ADD COLUMN C1 VARCHAR DEFAULT 'a'");
        sql("ALTER TABLE t ADD COLUMN C2 VARCHAR DEFAULT 'b'");

        sql("ALTER TABLE t ALTER COLUMN C1 DROP DEFAULT");
        sql("ALTER TABLE t ALTER COLUMN C2 SET DEFAULT NULL");

        sql("INSERT INTO t (id) VALUES (1)");

        sql("ALTER TABLE t ALTER COLUMN C1 SET DEFAULT 'a2'");
        sql("ALTER TABLE t ALTER COLUMN C2 SET DEFAULT 'b2'");

        sql("INSERT INTO t (id) VALUES (2)");

        assertQuery("SELECT id, c1, c2 FROM t ORDER BY id")
                .returns(0, "a", "b")
                .returns(1, null, null)
                .returns(2, "a2", "b2")
                .check();
    }

    @Test
    public void setDataTypeSetDefault() {
        sql("CREATE TABLE t (id int PRIMARY KEY, val int)");

        for (String col : Arrays.asList("id", "val")) {
            // Compound id
            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Unsupported default expression: A.B.C",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DATA TYPE BIGINT DEFAULT a.b.c", col))
            );

            // Expression
            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Unsupported default expression: 1 / 0",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DATA TYPE BIGINT DEFAULT (1/0)", col))
            );

            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Unsupported default expression: 1 / 0",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DATA TYPE BIGINT DEFAULT 1/0", col))
            );

            // SELECT

            assertThrowsSqlException(
                    STMT_PARSE_ERR,
                    "Query expression encountered in illegal context",
                    () -> sql(format("ALTER TABLE t ALTER COLUMN {} SET DATA TYPE BIGINT DEFAULT (SELECT 1000)", col))
            );
        }
    }

    @Override
    protected int initialNodes() {
        return 1;
    }
}
