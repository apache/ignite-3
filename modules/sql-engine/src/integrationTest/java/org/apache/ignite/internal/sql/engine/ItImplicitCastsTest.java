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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Implicit casts are added where it is necessary to do so.
 */
public class ItImplicitCastsTest extends BaseSqlIntegrationTest {

    @BeforeEach
    @AfterEach
    public void dropTables() {
        sql("DROP TABLE IF EXISTS t1");
        sql("DROP TABLE IF EXISTS t2");
        sql("DROP TABLE IF EXISTS t11");
        sql("DROP TABLE IF EXISTS t12");
    }

    @ParameterizedTest
    @MethodSource("columnPairs")
    public void testFilter(ColumnPair columnPair) {
        sql(format("CREATE TABLE T11 (c1 int primary key, c2 {})", columnPair.lhs));
        sql(format("CREATE TABLE T12 (c1 int primary key, c2 {})", columnPair.rhs));

        initData(columnPair);

        String value = columnPair.lhsLiteral(0);
        // Implicit casts are added to the left hand side of the expression.
        String query = format("SELECT T11.c2 FROM T11 WHERE T11.c2 > CAST({} AS {})", value, columnPair.rhs);

        assertQuery(query).check();
    }

    @ParameterizedTest
    @MethodSource("columnPairs")
    public void testMergeSort(ColumnPair columnPair) {
        sql(format("CREATE TABLE T11 (c1 int primary key, c2 {})", columnPair.lhs));
        sql(format("CREATE TABLE T12 (c1 int primary key, c2 {})", columnPair.rhs));

        initData(columnPair);

        assertQuery("SELECT T11.c2, T12.c2 FROM T11, T12 WHERE T11.c2 = T12.c2").check();
        assertQuery("SELECT T11.c2, T12.c2 FROM T11, T12 WHERE T11.c2 IS NOT DISTINCT FROM T12.c2").check();
    }

    @ParameterizedTest
    @MethodSource("columnPairs")
    public void testNestedLoopJoin(ColumnPair columnPair) {
        sql(format("CREATE TABLE T11 (c1 int primary key, c2 {})", columnPair.lhs));
        sql(format("CREATE TABLE T12 (c1 int primary key, c2 {})", columnPair.rhs));

        initData(columnPair);

        assertQuery("SELECT T11.c2, T12.c2 FROM T11, T12 WHERE T11.c2 != T12.c2").check();
        assertQuery("SELECT T11.c2, T12.c2 FROM T11, T12 WHERE T11.c2 IS DISTINCT FROM T12.c2").check();
    }

    /** MERGE with type coercion on insert. */
    @ParameterizedTest
    @CsvSource({
            // UPDATE SET c1, INSERT src.c2
            "src.c2 + 2",
            "CAST((src.c2 + 2) AS BIGINT)",
    })
    public void testMergeInsert(String insertC2) {
        sql("CREATE TABLE T1 (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER)");
        sql("CREATE TABLE T2 (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER)");

        sql("INSERT INTO t1 VALUES(1, 10, 20)");
        sql("INSERT INTO t1 VALUES(2, 20, 30)");
        sql("INSERT INTO t2 VALUES(1, 10, 12)");

        String sql = "MERGE INTO T2 dst USING t1 src ON dst.id = src.id "
                + "WHEN NOT MATCHED THEN INSERT (id, c1, c2) VALUES (src.id, src.c1 + 1, " + insertC2 + ")";

        sql(sql);

        assertQuery("SELECT * FROM t2 ORDER BY id")
                .returns(1, 10, 12)
                .returns(2, 21, 32)
                .check();
    }

    /** MERGE with type coercion on insert/update. */
    @ParameterizedTest
    @CsvSource({
            // UPDATE SET c1, INSERT src.c2\
            "100, src.c2 + 2",
            "'100', src.c2 + 2",
            "100, CAST((src.c2 + 2) AS BIGINT)",
            "'100', CAST((src.c2 + 2) AS BIGINT)",
    })
    public void testMergeUpdateInsert(String updateC1, String insertC2) {
        sql("CREATE TABLE T1 (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER)");
        sql("CREATE TABLE T2 (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER)");

        sql("INSERT INTO t1 VALUES(1, 10, 20)");
        sql("INSERT INTO t1 VALUES(2, 20, 30)");
        sql("INSERT INTO t2 VALUES(1, 10, 12)");

        String sql = "MERGE INTO T2 dst USING t1 src ON dst.id = src.id "
                + "WHEN MATCHED THEN UPDATE SET c1 = " + updateC1
                + "WHEN NOT MATCHED THEN INSERT (id, c1, c2) VALUES (src.id, src.c1 + 1, " + insertC2 + ")";

        sql(sql);

        assertQuery("SELECT * FROM t2 ORDER BY id")
                .returns(1, 100, 12)
                .returns(2, 21, 32)
                .check();
    }

    /**
     * SQL 2016, clause 9.5: Mixing types in CASE/COALESCE expressions is illegal.
     */
    @Test
    public void expressionWithMixedParametersIsIllegal() {
        assertThrows(IgniteException.class, () -> assertQuery("SELECT COALESCE(12.2, 'b')").check());
    }

    private static Stream<ColumnPair> columnPairs() {
        IgniteTypeFactory typeFactory = Commons.typeFactory();
        List<ColumnPair> columnPairs = new ArrayList<>();

        columnPairs.add(new ColumnPair(typeFactory.createSqlType(SqlTypeName.INTEGER), typeFactory.createSqlType(SqlTypeName.FLOAT)));
        columnPairs.add(new ColumnPair(typeFactory.createSqlType(SqlTypeName.DOUBLE), typeFactory.createSqlType(SqlTypeName.BIGINT)));

        List<ColumnPair> result = new ArrayList<>(columnPairs);
        Collections.reverse(columnPairs);

        columnPairs.stream().map(p -> new ColumnPair(p.rhs, p.lhs)).forEach(result::add);

        return result.stream();
    }

    private static void initData(ColumnPair columnPair) {
        Transaction tx = CLUSTER.aliveNode().transactions().begin();
        sql(tx, format("INSERT INTO T11 VALUES(1, CAST({} AS {}))", columnPair.lhsLiteral(1), columnPair.lhs));
        sql(tx, format("INSERT INTO T11 VALUES(2, CAST({} AS {}))", columnPair.lhsLiteral(3), columnPair.lhs));
        sql(tx, format("INSERT INTO T12 VALUES(1, CAST({} AS {}))", columnPair.lhsLiteral(2), columnPair.rhs));
        sql(tx, format("INSERT INTO T12 VALUES(2, CAST({} AS {}))", columnPair.lhsLiteral(4), columnPair.rhs));
        tx.commit();
    }

    private static final class ColumnPair {

        private final RelDataType lhs;

        private final RelDataType rhs;

        ColumnPair(RelDataType lhs, RelDataType rhs) {
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public String toString() {
            return lhs + " " + rhs;
        }

        String lhsLiteral(int idx) {
            return generateValue(lhs, idx);
        }

        static String generateValue(RelDataType type, int i) {
            if (SqlTypeUtil.isNumeric(type)) {
                return Integer.toString(i);
            } else if (type.getSqlTypeName() == SqlTypeName.UUID) {
                return SqlTestUtils.makeLiteral(new UUID(i, i), NativeTypes.UUID);
            } else if (type.getSqlTypeName() == SqlTypeName.CHAR
                    || type.getSqlTypeName() == SqlTypeName.VARCHAR) {
                return SqlTestUtils.makeLiteral(new UUID(i, i), NativeTypes.STRING);
            } else {
                throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
    }
}
