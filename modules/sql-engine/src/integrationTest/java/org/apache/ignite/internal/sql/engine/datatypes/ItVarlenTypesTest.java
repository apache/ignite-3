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

package org.apache.ignite.internal.sql.engine.datatypes;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;

import java.util.Arrays;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for large values for varlen types.
 */
public class ItVarlenTypesTest extends BaseSqlIntegrationTest {

    @BeforeEach
    public void before() {
        dropAllTables();
    }

    @Test
    public void testLargeBinary() {
        // Longer than default
        sql("CREATE TABLE t (id INT PRIMARY KEY, val VARBINARY(65537))");
        assertQuery("SELECT val FROM t")
                .columnMetadata(new MetadataMatcher().type(ColumnType.BYTE_ARRAY).precision(65537))
                .check();

        // insert
        {
            Object val1 = SqlTestUtils.generateValueByType(ColumnType.BYTE_ARRAY, 65537, 0);
            sql("INSERT INTO t VALUES (1, ?)", val1);

            Object val2 = SqlTestUtils.generateValueByType(ColumnType.BYTE_ARRAY, 65537, 0);
            sql(format("INSERT INTO t VALUES (2, x'{}')", StringUtils.toHexString((byte[]) val2)));

            assertQuery("SELECT val FROM t ORDER BY id")
                    .returns(val1)
                    .returns(val2)
                    .check();
        }

        // cast
        {
            Object val1 = SqlTestUtils.generateValueByType(ColumnType.BYTE_ARRAY, 700000 + 1, 0);
            byte[] shorten1 = Arrays.copyOf((byte[]) val1, 700000);

            assertQuery("SELECT CAST(? AS VARBINARY(700000))")
                    .withParams(val1)
                    .columnMetadata(new MetadataMatcher().type(ColumnType.BYTE_ARRAY).precision(700000))
                    .returns(shorten1)
                    .check();

            Object val2 = SqlTestUtils.generateValueByType(ColumnType.BYTE_ARRAY, 17000 + 1, 0);
            assertThrowsSqlException(Common.INTERNAL_ERR,
                    "Expression compiler error.",
                    () -> sql(format("SELECT CAST(x'{}' AS VARBINARY(17000))", StringUtils.toHexString((byte[]) val2))));
        }
    }

    @Test
    public void testMaxLengthVarBinary() {
        sql(format("CREATE TABLE t (id INT PRIMARY KEY, val VARBINARY({}))", Integer.MAX_VALUE));
        assertQuery("SELECT val FROM t")
                .columnMetadata(new MetadataMatcher().type(ColumnType.BYTE_ARRAY).precision(Integer.MAX_VALUE))
                .check();
    }

    @Test
    public void testLargeVarchar() {
        // Longer than default
        sql("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR(65537))");
        assertQuery("SELECT val FROM t")
                .columnMetadata(new MetadataMatcher().type(ColumnType.STRING).precision(65537))
                .check();

        // insert
        {
            Object val1 = "a".repeat(65537);
            assert val1 != null;
            sql("INSERT INTO t VALUES (1, ?)", val1);

            Object val2 = "b".repeat(65537);
            assert val2 != null;
            sql(format("INSERT INTO t VALUES (2, '{}')", val2));

            assertQuery("SELECT val FROM t ORDER BY id")
                    .returns(val1)
                    .returns(val2)
                    .check();
        }

        // cast
        {
            String val1 = "a".repeat(700000 + 1);
            assertQuery("SELECT CAST(? AS VARCHAR(700000))")
                    .withParams(val1)
                    .returns(val1.substring(0, 700000))
                    .check();

            String val2 = "b".repeat(700000 + 1);
            assertQuery(format("SELECT CAST('{}' AS VARCHAR(700000))", val2))
                    .returns(val2.substring(0, 700000))
                    .check();
        }
    }

    @Test
    public void testMaxLengthVarchar() {
        sql(format("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR({}))", Integer.MAX_VALUE));
        assertQuery("SELECT val FROM t")
                .columnMetadata(new MetadataMatcher().type(ColumnType.STRING).precision(Integer.MAX_VALUE))
                .check();
    }
}
