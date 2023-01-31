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

import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Implicit casts are added where it is necessary to do so.
 */
public class ItImplicitCastsTest extends AbstractBasicIntegrationTest {

    @AfterEach
    public void dropTables() {
        sql("DROP TABLE IF EXISTS t11");
        sql("DROP TABLE IF EXISTS t12");
    }

    @ParameterizedTest
    @MethodSource("columnPairs")
    public void testFilter(ColumnPair columnPair) {
        prepareTables(columnPair);

        assertQuery("SELECT T11.c2 FROM T11 WHERE T11.c2 > 1.0").check();
    }

    @ParameterizedTest
    @MethodSource("columnPairs")
    public void testMergeSort(ColumnPair columnPair) {
        prepareTables(columnPair);

        assertQuery("SELECT T11.c2, T12.c2 FROM T11, T12 WHERE T11.c2 = T12.c2").check();
        assertQuery("SELECT T11.c2, T12.c2 FROM T11, T12 WHERE T11.c2 IS NOT DISTINCT FROM T12.c2").check();
    }

    @ParameterizedTest
    @MethodSource("columnPairs")
    public void testNestedLoopJoin(ColumnPair columnPair) {
        prepareTables(columnPair);

        assertQuery("SELECT T11.c2, T12.c2 FROM T11, T12 WHERE T11.c2 != T12.c2").check();
        assertQuery("SELECT T11.c2, T12.c2 FROM T11, T12 WHERE T11.c2 IS DISTINCT FROM T12.c2").check();
    }

    private static Stream<ColumnPair> columnPairs() {
        IgniteTypeFactory typeFactory = new IgniteTypeFactory();

        return Stream.of(
                new ColumnPair(typeFactory.createSqlType(SqlTypeName.INTEGER), typeFactory.createSqlType(SqlTypeName.FLOAT)),
                new ColumnPair(typeFactory.createSqlType(SqlTypeName.DOUBLE), typeFactory.createSqlType(SqlTypeName.BIGINT))
        );
    }

    private static void prepareTables(ColumnPair columnPair) {
        sql(String.format("CREATE TABLE T11 (c1 int primary key, c2 %s)", columnPair.lhs));
        sql(String.format("CREATE TABLE T12 (c1 decimal primary key, c2 %s)", columnPair.rhs));

        Transaction tx = CLUSTER_NODES.get(0).transactions().begin();
        sql(tx, "INSERT INTO T11 VALUES(1, 2)");
        sql(tx, "INSERT INTO T11 VALUES(2, 3)");
        sql(tx, "INSERT INTO T12 VALUES(1, 2)");
        sql(tx, "INSERT INTO T12 VALUES(2, 4)");
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
    }
}
