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

import java.math.BigDecimal;
import java.util.Random;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Test cases for the division operator involving decimal operand(s). */
public class ItDivisionDecimalTest extends BaseSqlIntegrationTest {

    @BeforeAll
    public static void createTable() {
        sql("CREATE TABLE t (id INT PRIMARY KEY, "
                + " ti TINYINT, si SMALLINT, i INT, bi BIGINT, "
                + " r REAL, d DOUBLE, "
                + " d3_1 DECIMAL(3, 1), d5_2 DECIMAL(5, 2)"
                + ")");
    }

    /**
     * Drops all created tables.
     */
    @AfterAll
    public void dropTables() {
        dropAllTables();
    }

    @AfterEach
    void clearTable() {
        sql("DELETE FROM t");
    }

    @ParameterizedTest
    @CsvSource({
            "5, 1, 5, 1",
            "5, 2, 5, 2",
            "5, 3, 5, 3",
            "5, 4, 5, 4",

            "10, 5, 20, 5",
            "10, 6, 20, 6",
            "10, 7, 20, 7",
            "10, 8, 20, 8",
            "10, 9, 20, 9",
    })
    public void decimalDivide(int p1, int s1, int p2, int s2) {
        IgniteTypeFactory tf = Commons.typeFactory();

        RelDataType t1 = tf.createSqlType(SqlTypeName.DECIMAL, p1, s1);
        RelDataType t2 = tf.createSqlType(SqlTypeName.DECIMAL, p2, s2);
        RelDataType rt = IgniteTypeSystem.INSTANCE.deriveDecimalDivideType(Commons.typeFactory(), t1, t2);

        long seed = System.nanoTime();
        log.info("Seed: {}", seed);

        Random rnd = new Random();
        rnd.setSeed(seed);

        BigDecimal a1 = IgniteTestUtils.randomBigDecimal(rnd, t1.getPrecision(), t1.getScale());
        BigDecimal a2 = IgniteTestUtils.randomBigDecimal(rnd, t2.getPrecision(), t2.getScale());
        BigDecimal rs = a1.divide(a2, rt.getScale(), IgniteTypeSystem.INSTANCE.roundingMode());

        String type1 = format("DECIMAL({}, {})", p1, s1);
        String type2 = format("DECIMAL({}, {})", p2, s2);

        // Literals no casts
        assertQuery("SELECT " + a1 + "/" + a2)
                .returns(rs)
                .columnMetadata(new MetadataMatcher()
                        .type(ColumnType.DECIMAL)
                        .precision(rt.getPrecision())
                        .scale(rt.getScale()))
                .check();

        // Literals w/ casts
        assertQuery("SELECT " + a1  + "::" + type1 + "/" + a2 + "::" + type2)
                .returns(rs)
                .columnMetadata(new MetadataMatcher()
                        .type(ColumnType.DECIMAL)
                        .precision(rt.getPrecision())
                        .scale(rt.getScale()))
                .check();

        // Dynamic params
        assertQuery("SELECT ?::" + type1 + "/?::" + type2)
                .withParams(a1, a2)
                .returns(rs)
                .columnMetadata(new MetadataMatcher()
                        .type(ColumnType.DECIMAL)
                        .precision(rt.getPrecision())
                        .scale(rt.getScale()))
                .check();
    }

    @Test
    public void decimalTinyint() {
        sql("INSERT INTO t (id, ti, d3_1) VALUES (1, 10, 3.1)");

        assertQuery("SELECT ti/d3_1 FROM t")
                .returns(new BigDecimal("3.225806"))
                .columnMetadata(new MetadataMatcher().type(ColumnType.DECIMAL).precision(10).scale(6))
                .check();

        assertQuery("SELECT d3_1/ti FROM t")
                .returns(new BigDecimal("0.310000"))
                .columnMetadata(new MetadataMatcher().type(ColumnType.DECIMAL).precision(8).scale(6))
                .check();
    }

    @Test
    public void decimalSmallint() {
        sql("INSERT INTO t (id, si, d3_1) VALUES (1, 10, 3.1)");

        assertQuery("SELECT si/d3_1 FROM t")
                .returns(new BigDecimal("3.225806"))
                .columnMetadata(new MetadataMatcher().type(ColumnType.DECIMAL).precision(12).scale(6))
                .check();

        assertQuery("SELECT d3_1/si FROM t")
                .returns(new BigDecimal("0.3100000"))
                .columnMetadata(new MetadataMatcher().type(ColumnType.DECIMAL).precision(9).scale(7))
                .check();
    }

    @Test
    public void decimalInt() {
        sql("INSERT INTO t (id, i, d3_1) VALUES (1, 10, 3.1)");

        assertQuery("SELECT i/d3_1 FROM t")
                .returns(new BigDecimal("3.225806"))
                .columnMetadata(new MetadataMatcher().type(ColumnType.DECIMAL).precision(17).scale(6))
                .check();

        assertQuery("SELECT d3_1/i FROM t")
                .returns(new BigDecimal("0.310000000000"))
                .columnMetadata(new MetadataMatcher().type(ColumnType.DECIMAL).precision(14).scale(12))
                .check();
    }

    @Test
    public void decimalBigInt() {
        sql("INSERT INTO t (id, bi, d3_1) VALUES (1, 10, 3.1)");

        assertQuery("SELECT bi/d3_1 FROM t")
                .returns(new BigDecimal("3.225806"))
                .columnMetadata(new MetadataMatcher().type(ColumnType.DECIMAL).precision(26).scale(6))
                .check();

        assertQuery("SELECT d3_1/bi FROM t")
                .returns(new BigDecimal("0.310000000000000000000"))
                .columnMetadata(new MetadataMatcher().type(ColumnType.DECIMAL).precision(23).scale(21))
                .check();
    }

    @Test
    public void decimalReal() {
        sql("INSERT INTO t (id, r, d3_1) VALUES (1, 10, 2.0)");

        assertQuery("SELECT r/d3_1 FROM t")
                .returns(5.0d)
                .columnMetadata(new MetadataMatcher().type(ColumnType.DOUBLE))
                .check();

        assertQuery("SELECT d3_1/r FROM t")
                .returns(0.2d)
                .columnMetadata(new MetadataMatcher().type(ColumnType.DOUBLE))
                .check();
    }

    @Test
    public void decimalDouble() {
        sql("INSERT INTO t (id, d, d3_1) VALUES (1, 10, 2.0)");

        assertQuery("SELECT d/d3_1 FROM t")
                .returns(5.0d)
                .columnMetadata(new MetadataMatcher().type(ColumnType.DOUBLE))
                .check();

        assertQuery("SELECT d3_1/d FROM t")
                .returns(0.2d)
                .columnMetadata(new MetadataMatcher().type(ColumnType.DOUBLE))
                .check();
    }

    @Test
    public void decimalDecimal() {
        sql("INSERT INTO t (id, d5_2, d3_1) VALUES (1, 10.00, 3.1)");

        assertQuery("SELECT d5_2/d3_1 FROM t")
                .returns(new BigDecimal("3.225806"))
                .columnMetadata(new MetadataMatcher().type(ColumnType.DECIMAL).precision(10).scale(6))
                .check();

        assertQuery("SELECT d3_1/d5_2 FROM t")
                .returns(new BigDecimal("0.3100000"))
                .columnMetadata(new MetadataMatcher().type(ColumnType.DECIMAL).precision(11).scale(7))
                .check();
    }
}
