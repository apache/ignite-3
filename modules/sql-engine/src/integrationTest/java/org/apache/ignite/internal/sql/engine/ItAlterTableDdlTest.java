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
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for ALTER TABLE DDL statements.
 *
 * <p>SQL F031-04 feature. ALTER TABLE statement: ADD COLUMN clause
 * SQL F033 feature. ALTER TABLE statement: DROP COLUMN clause
 */
public class ItAlterTableDdlTest extends BaseSqlIntegrationTest {
    @AfterEach
    public void dropTables() {
        dropAllTables();
        dropAllZonesExceptDefaultOne();
    }

    /** Test correct mapping schema after alter columns. */
    @Test
    public void testDropAndAddColumns() {
        sql("CREATE TABLE my (c1 INT PRIMARY KEY, c2 INT, c3 VARCHAR)");

        sql("INSERT INTO my VALUES (11, 2, '3')");
        assertQuery("SELECT * FROM my")
                .returns(11, 2, "3")
                .check();

        // Drop column. Table columns: c1, c3
        sql("ALTER TABLE my DROP COLUMN c2");
        assertQuery("SELECT * FROM my")
                .returns(11, "3")
                .check();

        // Add columns. Table columns: c1, c3, c2, c4
        sql("ALTER TABLE my ADD COLUMN (c2 INT DEFAULT -1, c4 VARCHAR)");

        sql("INSERT INTO my VALUES (12, '2', 3, '4')");
        assertQuery("SELECT * FROM my")
                .returns(11, "3", -1, null)
                .returns(12, "2", 3, "4")
                .check();

        // Re-create column with the same name. Table columns: c1, c3, c4, c2
        sql("ALTER TABLE my DROP COLUMN c2");
        sql("ALTER TABLE my ADD COLUMN (c2 VARCHAR)");

        sql("INSERT INTO my VALUES (13, '2', '3', '4')");
        assertQuery("SELECT * FROM my")
                .returns(11, "3", null, null)
                .returns(12, "2", "4", null)
                .returns(13, "2", "3", "4")
                .check();

        // Checking the correctness of reading a row created on a different version of the schema.
        sql("ALTER TABLE my ADD COLUMN (c5 INT, c6 BOOLEAN)");
        sql("ALTER TABLE my DROP COLUMN c4");
        // Table columns: c1, c3, c2, c5, c6
        assertQuery("SELECT * FROM my")
                .returns(11, "3", null, null, null)
                .returns(12, "2", null, null, null)
                .returns(13, "2", "4", null, null)
                .check();
    }

    /** Test add/drop column short syntax. */
    @Test
    public void testDropAndAddColumnShortSyntax() {
        sql("CREATE TABLE my (c1 INT PRIMARY KEY, c2 INT)");

        sql("ALTER TABLE my ADD (c3 VARCHAR)");
        sql("ALTER TABLE my ADD (c4 INT DEFAULT -1, c5 INT)");

        sql("INSERT INTO my (c1, c2, c3) VALUES (1, 2, '3')");

        // Table columns: c1, c2, c3, c4, c5
        assertQuery("SELECT * FROM my")
                .returns(1, 2, "3", -1, null)
                .check();

        sql("ALTER TABLE my DROP c2");

        // Table columns: c1, c3, c4, c5
        assertQuery("SELECT * FROM my")
                .returns(1, "3", -1, null)
                .check();

        sql("ALTER TABLE my DROP (c3, c5)");

        // Table columns: c1, c4
        assertQuery("SELECT * FROM my")
                .returns(1, -1)
                .check();
    }

    /** Test that adding nullable column via ALTER TABLE ADD name type NULL works. */
    @Test
    public void testNullableColumn() {
        sql("CREATE TABLE my (c1 INT PRIMARY KEY, c2 INT)");
        sql("INSERT INTO my VALUES (1, 1)");
        sql("ALTER TABLE my ADD COLUMN c3 INT NULL");
        sql("INSERT INTO my VALUES (2, 2, NULL)");

        assertQuery("SELECT * FROM my ORDER by c1 ASC")
                .returns(1, 1, null)
                .returns(2, 2, null)
                .check();
    }

    /**
     * Adds columns of all supported types and checks that the row
     * created on the old schema version is read correctly.
     */
    @Test
    public void testDropAndAddColumnsAllTypes() {
        List<NativeType> allTypes = SchemaTestUtils.ALL_TYPES;

        // List of columns for 'ADD COLUMN' statement.
        IgniteStringBuilder addColumnsList = new IgniteStringBuilder();
        // List of columns for 'DROP COLUMN' statement.
        IgniteStringBuilder dropColumnsList = new IgniteStringBuilder();

        for (int i = 0; i < allTypes.size(); i++) {
            NativeType type = allTypes.get(i);

            RelDataType relDataType = TypeUtils.native2relationalType(Commons.typeFactory(), type);

            if (addColumnsList.length() > 0) {
                addColumnsList.app(',');
                dropColumnsList.app(',');
            }

            addColumnsList.app("c").app(i).app(' ').app(relDataType.getSqlTypeName().getSpaceName());
            dropColumnsList.app("c").app(i);
        }

        sql("CREATE TABLE test (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO test VALUES (0, 1)");
        sql(format("ALTER TABLE test ADD COLUMN ({})", addColumnsList.toString()));

        List<List<Object>> res = sql("SELECT * FROM test");
        assertThat(res.size(), is(1));
        assertThat(res.get(0).size(), is(allTypes.size() + /* initial columns */ 2));

        sql(format("ALTER TABLE test DROP COLUMN ({})", dropColumnsList.toString()));
        assertQuery("SELECT * FROM test")
                .returns(0, 1)
                .check();
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-19162 Fix test to ensure time/timestamp columns created with desired precision.
    @Test
    public void addColumnWithConstantDefault() {
        // // SQL Standard 2016 feature E141-07 - Basic integrity constraints. Column defaults
        sql("CREATE TABLE test(id BIGINT DEFAULT 1 PRIMARY KEY)");

        sql("ALTER TABLE test ADD COLUMN valint INTEGER DEFAULT 1");
        sql("ALTER TABLE test ADD COLUMN valdate DATE DEFAULT DATE '2001-12-21'");
        sql("ALTER TABLE test ADD COLUMN valtime TIME(3) DEFAULT TIME '11:22:33.444555'");
        sql("ALTER TABLE test ADD COLUMN valts TIMESTAMP(3) DEFAULT TIMESTAMP '2001-12-21 11:22:33.444555'");
        sql("ALTER TABLE test ADD COLUMN valstr VARCHAR DEFAULT 'string'");
        sql("ALTER TABLE test ADD COLUMN valbin VARBINARY DEFAULT x'ff'");
        sql("ALTER TABLE test ADD COLUMN valuuid UUID DEFAULT '00000000-0000-0000-0000-000000000000'");

        sql("INSERT INTO test (id) VALUES (0)");

        assertQuery("SELECT * FROM test")
                .returns(0L,
                        1,
                        LocalDate.of(2001, Month.DECEMBER, 21),
                        LocalTime.of(11, 22, 33, 444000000),
                        LocalDateTime.of(2001, Month.DECEMBER, 21, 11, 22, 33, 444000000),
                        "string",
                        new byte[]{(byte) 0xff},
                        UUID.fromString("00000000-0000-0000-0000-000000000000")
                )
                .check();
    }

    @Test
    public void doNotAllowFunctionsInNonPkColumns() {
        // SQL Standard 2016 feature E141-07 - Basic integrity constraints. Column defaults
        sql("CREATE TABLE t (id VARCHAR PRIMARY KEY, val VARCHAR)");

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Functional defaults are not supported for non-primary key columns",
                () -> sql("ALTER TABLE t ADD COLUMN val2 VARCHAR DEFAULT rand_uuid")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Functional defaults are not supported for non-primary key columns",
                () -> sql("ALTER TABLE t ADD COLUMN val2 VARCHAR DEFAULT rand_uuid()")
        );
    }

    @Test
    public void uuidDefault() {
        UUID defaultUuid = UUID.randomUUID();
        sql("CREATE TABLE test(id INT PRIMARY KEY)");

        RecordView<Tuple> recView = CLUSTER.aliveNode().tables().table("test").recordView();
        KeyValueView<Tuple, Tuple> kvBinaryView = CLUSTER.aliveNode().tables().table("test").keyValueView();
        KeyValueView<Integer, UUID> kvView = CLUSTER.aliveNode().tables().table("test").keyValueView(Integer.class, UUID.class);

        // Ensure that invalid UUIDs are rejected.
        {
            //noinspection ThrowableNotThrown
            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Invalid default value for column 'VAL'",
                    () -> sql("ALTER TABLE test ADD COLUMN (val UUID DEFAULT '00000000-0000-0000-0000-')")
            );

            //noinspection ThrowableNotThrown
            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Invalid default value for column 'VAL'",
                    () -> sql("ALTER TABLE test ADD COLUMN (val UUID DEFAULT 911)")
            );
        }

        // Add column with UUID default value.
        sql(format("ALTER TABLE test ADD COLUMN (val UUID DEFAULT '{}')", defaultUuid));

        // Put some data.
        sql("INSERT INTO test VALUES (1, DEFAULT), (2, DEFAULT)");
        sql("INSERT INTO test (id) VALUES (3)");
        recView.upsert(null, Tuple.create().set("id", 4));
        kvBinaryView.put(null, Tuple.create().set("id", 5), Tuple.create());
        sql("INSERT INTO test VALUES (6, NULL)");
        kvView.put(null, 7, null);

        // Verify UUID value using SQL.
        {
            assertQuery("SELECT id, val FROM test ORDER BY id")
                    .returns(1, defaultUuid)
                    .returns(2, defaultUuid)
                    .returns(3, defaultUuid)
                    .returns(4, defaultUuid)
                    .returns(5, defaultUuid)
                    .returns(6, null)
                    .returns(7, null)
                    .check();
        }

        // Verify UUID values using record and key-value view.
        {
            List<Integer> ids = List.of(1, 2, 3, 4, 5);

            ids.forEach(id -> {
                Tuple record = recView.get(null, Tuple.create().set("id", id));
                assertThat("id=" + id, record.uuidValue("val"), equalTo(defaultUuid));

                Tuple row = kvBinaryView.get(null, Tuple.create().set("id", id));
                assertNotNull(row);
                assertThat("id=" + id, row.uuidValue("val"), equalTo(defaultUuid));

                UUID val = kvView.get(null, id);
                assertThat("id=" + id, val, equalTo(defaultUuid));
            });

            List<Integer> nullIds = List.of(6, 7);

            nullIds.forEach(id -> {
                Tuple record = recView.get(null, Tuple.create().set("id", id));
                assertNull(record.uuidValue("val"), "id=" + id);

                Tuple row = kvBinaryView.get(null, Tuple.create().set("id", id));
                assertNotNull(row);
                assertNull(row.uuidValue("val"), "id=" + id);

                NullableValue<UUID> val = kvView.getNullable(null, id);
                assertNotNull(val, "id=" + id);
                assertNull(val.get());
            });
        }

        // Verify NULL as default value.
        {
            sql("ALTER TABLE test ALTER COLUMN val SET DEFAULT NULL");
            sql("INSERT INTO test VALUES (8, DEFAULT)");
            sql("INSERT INTO test (id) VALUES (9)");
            assertQuery("SELECT id, val FROM test ORDER BY id")
                    .returns(1, defaultUuid)
                    .returns(2, defaultUuid)
                    .returns(3, defaultUuid)
                    .returns(4, defaultUuid)
                    .returns(5, defaultUuid)
                    .returns(6, null)
                    .returns(7, null)
                    .returns(8, null)
                    .returns(9, null)
                    .check();
        }
    }

    @Test
    public void testAddColumnWithIncorrectType() {
        sql("CREATE TABLE test(id INTEGER PRIMARY KEY, val INTEGER)");

        // Char

        assertThrowsSqlException(
                STMT_PARSE_ERR,
                "Literal '2147483648' can not be parsed to type",
                () -> sql("ALTER TABLE test ADD COLUMN val2 VARCHAR(2147483648)")
        );

        assertThrowsSqlException(
                STMT_PARSE_ERR,
                "Literal '2147483648' can not be parsed to type",
                () -> sql("ALTER TABLE test ADD COLUMN val2 VARCHAR(2147483648) ")
        );

        // Binary

        assertThrowsSqlException(
                STMT_PARSE_ERR,
                "Literal '2147483648' can not be parsed to type",
                () -> sql("ALTER TABLE test ADD COLUMN val2 VARBINARY(2147483648)")
        );

        // Decimal

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "DECIMAL precision 10000000 must be between 1 and 32767. [column=VAL2]",
                () -> sql("ALTER TABLE test ADD COLUMN val2 DECIMAL(10000000)")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "DECIMAL scale 10000000 must be between 0 and 32767. [column=VAL2]",
                () -> sql("ALTER TABLE test ADD COLUMN val2 DECIMAL(100, 10000000)")
        );

        // Time

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "TIME precision 10000000 must be between 0 and 9. [column=VAL2]",
                () -> sql("ALTER TABLE test ADD COLUMN val2 TIME(10000000)")
        );

        // Timestamp

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "TIMESTAMP precision 10000000 must be between 0 and 9. [column=VAL2]",
                () -> sql("ALTER TABLE test ADD COLUMN val2 TIMESTAMP(10000000)")
        );
    }

    @Test
    public void testAddColumnWithNotFittingDefaultValues() {
        // Char

        String longString = "1".repeat(101);
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Invalid default value for column 'VAL2'",
                () -> sql("ALTER TABLE test ADD COLUMN val2 VARCHAR(100) DEFAULT x'" + longString + "'")
        );

        // Binary

        String longByteString = "01".repeat(101);

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Invalid default value for column 'VAL2'",
                () -> sql("ALTER TABLE test ADD COLUMN val2 VARBINARY(100) DEFAULT x'" + longByteString + "'")
        );

        // Decimal

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Invalid default value for column 'VAL2'",
                () -> sql("ALTER TABLE test ADD COLUMN val2 DECIMAL(5) DEFAULT 1000000")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Invalid default value for column 'VAL2'",
                () -> sql("ALTER TABLE test ADD COLUMN val2 DECIMAL(3, 2) DEFAULT 333.123")
        );

        // Time

        sql("CREATE TABLE test_time (id INT PRIMARY KEY, val INT)");
        sql("ALTER TABLE test_time ADD COLUMN val2 TIME(2) DEFAULT '00:00:00.1234'");
        sql("INSERT INTO test_time VALUES (1, 1, DEFAULT)");
        assertQuery("SELECT val2 FROM test_time")
                .returns(LocalTime.of(0, 0, 0, 120_000_000))
                .check();

        // Timestamp

        sql("CREATE TABLE test_ts (id INT PRIMARY KEY, val INT)");
        sql("ALTER TABLE test_ts ADD COLUMN val2 TIMESTAMP(2) DEFAULT '2000-01-01 00:00:00.1234'");
        sql("INSERT INTO test_ts VALUES (1, 1, DEFAULT)");
        assertQuery("SELECT val2 FROM test_ts")
                .returns(LocalDateTime.of(2000, 1, 1, 0, 0, 0, 120_000_000))
                .check();
    }

    @Test
    public void testRejectNotSupportedDefaults() {
        sql("CREATE TABLE t (id int, val int, PRIMARY KEY (id))");

        // Compound id
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Unsupported default expression: A.B.C",
                () -> sql("ALTER TABLE t ADD COLUMN col INT DEFAULT a.b.c")
        );

        // Expression
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Unsupported default expression: 1 / 0",
                () -> sql("ALTER TABLE t ADD COLUMN col INT DEFAULT (1/0)")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Unsupported default expression: 1 / 0",
                () -> sql("ALTER TABLE t ADD COLUMN col INT DEFAULT 1/0")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Functional defaults are not supported for non-primary key columns",
                () -> sql("ALTER TABLE t ADD COLUMN col INT DEFAULT rand_uuid")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Functional defaults are not supported for non-primary key columns",
                () -> sql("ALTER TABLE t ADD COLUMN col INT DEFAULT rand_uuid()")
        );

        // SELECT

        assertThrowsSqlException(
                STMT_PARSE_ERR,
                "Query expression encountered in illegal context",
                () -> sql("ALTER TABLE t ADD COLUMN col INT DEFAULT (SELECT count(*) from xyz)")
        );

        assertThrowsSqlException(
                STMT_PARSE_ERR,
                "Query expression encountered in illegal context",
                () -> sql("ALTER TABLE t ADD COLUMN col INT DEFAULT (SELECT count(*) FROM xyz)")
        );
    }
}
