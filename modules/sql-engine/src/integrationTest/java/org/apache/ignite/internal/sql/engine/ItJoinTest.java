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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Check JOIN on basic cases.
 */
@SuppressWarnings("ConcatenationWithEmptyString")
public class ItJoinTest extends BaseSqlIntegrationTest {
    @BeforeAll
    public static void beforeTestsStarted() {
        sql("CREATE TABLE t1 (id INT PRIMARY KEY, c1 INT NOT NULL, c2 INT, c3 INT)");
        sql("CREATE TABLE t2 (id INT PRIMARY KEY, c1 INT NOT NULL, c2 INT, c3 INT)");
        sql("CREATE TABLE t3 (id INT PRIMARY KEY, c1 INTEGER)");
        sql("CREATE TABLE checkNulls1 (id INT PRIMARY KEY, c1 INTEGER, c2 INTEGER)");
        sql("CREATE TABLE checkNulls2 (id INT PRIMARY KEY, c1 INTEGER, c2 INTEGER)");

        sql("create index t1_idx on t1 (c3, c2, c1)");
        sql("create index t2_idx on t2 (c3, c2, c1)");

        insertData("t1", List.of("ID", "C1", "C2", "C3"),
                new Object[] {0, 1, 1, 1},
                new Object[] {1, 2, null, 2},
                new Object[] {2, 2, 2, 2},
                new Object[] {3, 3, 3, null},
                new Object[] {4, 3, 3, 3},
                new Object[] {5, 4, 4, 4}
        );

        insertData("t2", List.of("ID", "C1", "C2", "C3"),
                new Object[] {0, 1, 1, 1},
                new Object[] {1, 2, 2, null},
                new Object[] {2, 2, 2, 2},
                new Object[] {3, 3, null, 3},
                new Object[] {4, 3, 3, 3},
                new Object[] {5, 4, 4, 4}
        );

        insertData("t3", List.of("ID", "C1"),
                new Object[] {0, 1},
                new Object[] {1, 2},
                new Object[] {2, 3},
                new Object[] {3, null},
                new Object[] {7, 7},
                new Object[] {8, 8}
        );

        insertData("checkNulls1", List.of("ID", "C1", "C2"),
                new Object[] {0, null, 1}
        );

        insertData("checkNulls2", List.of("ID", "C1", "C2"),
                new Object[] {0, 1, null}
        );
    }

    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testCheckNullsNullFirstLeftJoin(JoinType joinType) {
        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls1 t1 left join checkNulls1 t2 on t1.c1 = t2.c1;",
                joinType
        )
                .returns(null, 1, null, null)
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls1 t1 left join checkNulls1 t2 on t1.c1 = t2.c1 and t1.c2 = t2.c2;",
                joinType
        )
                .returns(null, 1, null, null)
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls1 t1 left join checkNulls1 t2 on t1.c2 = t2.c2;",
                joinType
        )
                .returns(null, 1, null, 1)
                .check();
    }

    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testCheckNullsNullFirstRightJoin(JoinType joinType) {
        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls1 t1 right join checkNulls1 t2 on t1.c1 = t2.c1;",
                joinType
        )
                .returns(null, null, null, 1)
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls1 t1 right join checkNulls1 t2 on t1.c1 = t2.c1 and t1.c2 = t2.c2;",
                joinType
        )
                .returns(null, null, null, 1)
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls1 t1 right join checkNulls1 t2 on t1.c2 = t2.c2;",
                joinType
        )
                .returns(null, 1, null, 1)
                .check();
    }

    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testCheckNullsNullFirstInnerJoin(JoinType joinType) {
        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls1 t1 inner join checkNulls1 t2 on t1.c1 = t2.c1;",
                joinType
        )
                .returnNothing()
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls1 t1 inner join checkNulls1 t2 on t1.c1 = t2.c1 and t1.c2 = t2.c2;",
                joinType
        )
                .returnNothing()
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls1 t1 inner join checkNulls1 t2 on t1.c2 = t2.c2;",
                joinType
        )
                .returns(null, 1, null, 1)
                .check();
    }

    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testCheckNullsNullSecondLeftJoin(JoinType joinType) {
        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls2 t1 left join checkNulls2 t2 on t1.c1 = t2.c1;",
                joinType
        )
                .returns(1, null, 1, null)
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls2 t1 left join checkNulls2 t2 on t1.c1 = t2.c1 and t1.c2 = t2.c2;",
                joinType
        )
                .returns(1, null, null, null)
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls2 t1 left join checkNulls2 t2 on t1.c2 = t2.c2;",
                joinType
        )
                .returns(1, null, null, null)
                .check();
    }

    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testCheckNullsNullSecondRightJoin(JoinType joinType) {
        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls2 t1 right join checkNulls2 t2 on t1.c1 = t2.c1;",
                joinType
        )
                .returns(1, null, 1, null)
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls2 t1 right join checkNulls2 t2 on t1.c1 = t2.c1 and t1.c2 = t2.c2;",
                joinType
        )
                .returns(null, null, 1, null)
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls2 t1 right join checkNulls2 t2 on t1.c2 = t2.c2;",
                joinType
        )
                .returns(null, null, 1, null)
                .check();
    }

    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testCheckNullsNullSecondInnerJoin(JoinType joinType) {
        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls2 t1 inner join checkNulls2 t2 on t1.c1 = t2.c1;",
                joinType
        )
                .returns(1, null, 1, null)
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls2 t1 inner join checkNulls2 t2 on t1.c1 = t2.c1 and t1.c2 = t2.c2;",
                joinType
        )
                .returnNothing()
                .check();

        assertQuery("select t1.c1, t1.c2, t2.c1, t2.c2 "
                        + "from checkNulls2 t1 inner join checkNulls2 t2 on t1.c2 = t2.c2;",
                joinType
        )
                .returnNothing()
                .check();
    }

    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testFullOuterJoin(JoinType joinType) {
        assertQuery(""
                        + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t3.id c21, t3.c1 c22 "
                        + "  from t1 "
                        + "  full outer join t3 "
                        + "    on t1.c1 = t3.id "
                        + "   and t1.c2 = t3.c1 "
                        + " order by t1.c1, t1.c2, t1.c3, t3.id",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, null, null)
                .returns(2, 2, 2, null, null)
                .returns(2, null, 2, null, null)
                .returns(3, 3, 3, null, null)
                .returns(3, 3, null, null, null)
                .returns(4, 4, 4, null, null)
                .returns(null, null, null, 0, 1)
                .returns(null, null, null, 1, 2)
                .returns(null, null, null, 2, 3)
                .returns(null, null, null, 3, null)
                .returns(null, null, null, 7, 7)
                .returns(null, null, null, 8, 8)
                .check();
    }

    /**
     * Test verifies result of inner join with different ordering.
     */
    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testInnerJoin(JoinType joinType) {
        assertQuery("SELECT c1 FROM t3 WHERE c1 = ANY(SELECT c1 FROM t3) ORDER BY c1", joinType)
                .ordered()
                .returns(1)
                .returns(2)
                .returns(3)
                .returns(7)
                .returns(8)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1, t1.c2, t1.c3",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, null, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1, t1.c2, t1.c3 nulls first",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, null, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1, t1.c2, t1.c3 nulls last",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, null, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1 desc, t1.c2, t1.c3",
                joinType
        )
                .ordered()
                .returns(4, 4, 4, 4, 4)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, null, 3, 3)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(1, 1, 1, 1, 1)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1 desc, t1.c2, t1.c3 nulls first",
                joinType
        )
                .ordered()
                .returns(4, 4, 4, 4, 4)
                .returns(3, 3, null, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(1, 1, 1, 1, 1)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1 desc, t1.c2, t1.c3 nulls last",
                joinType
        )
                .ordered()
                .returns(4, 4, 4, 4, 4)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, null, 3, 3)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(1, 1, 1, 1, 1)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t2.c3 c23, t2.c2 c22 "
                + "  from t1 "
                + "  join t2 "
                + "    on t1.c3 = t2.c3 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c3, t1.c2",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1)
                .returns(2, 2, 2, 2)
                .returns(3, 3, 3, 3)
                .returns(4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  join t2 "
                + "    on t1.c3 = t2.c3 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c3 nulls first, t1.c2 nulls first, t1.c1 nulls first",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c3 nulls first, t1.c2 nulls first, t1.c1 nulls first",
                joinType
        )
                .ordered()
                .returns(null, 3, 3, 3, 3)
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c3 nulls last, t1.c2 nulls last, t1.c1 nulls last",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .returns(null, 3, 3, 3, 3)
                .check();
    }

    /**
     * Test verifies result of left join with different ordering.
     */
    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = {"CORRELATED"})
    public void testLeftJoin(JoinType joinType) {
        assertQuery("select t31.c1 from t3 t31 left join t3 t32 on t31.c1 = t32.c1 ORDER BY t31.c1;", joinType)
                .ordered()
                .returns(1)
                .returns(2)
                .returns(3)
                .returns(7)
                .returns(8)
                .returns(null)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c1 = t2.c1"
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1, t1.c2, t1.c3",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(2, null, 2, null, null)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, null, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1, t1.c2 nulls first, t1.c3 nulls first",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, null, 2, null, null)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, null, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1, t1.c2 nulls last, t1.c3 nulls last",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(2, null, 2, null, null)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, null, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1 desc, t1.c2, t1.c3",
                joinType
        )
                .ordered()
                .returns(4, 4, 4, 4, 4)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, null, 3, 3)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(2, null, 2, null, null)
                .returns(1, 1, 1, 1, 1)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1 desc, t1.c2, t1.c3 nulls first",
                joinType
        )
                .ordered()
                .returns(4, 4, 4, 4, 4)
                .returns(3, 3, null, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(2, null, 2, null, null)
                .returns(1, 1, 1, 1, 1)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c1 desc, t1.c2, t1.c3 nulls last",
                joinType
        )
                .ordered()
                .returns(4, 4, 4, 4, 4)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, null, 3, 3)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(2, null, 2, null, null)
                .returns(1, 1, 1, 1, 1)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t2.c3 c23, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c3 = t2.c3 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c3, t1.c2",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1)
                .returns(2, 2, 2, 2)
                .returns(2, null, null, null)
                .returns(3, 3, 3, 3)
                .returns(4, 4, 4, 4)
                .returns(null, 3, null, null)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c3 = t2.c3 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c3 nulls first, t1.c2 nulls first, t1.c1 nulls first",
                joinType
        )
                .ordered()
                .returns(null, 3, 3, null, null)
                .returns(1, 1, 1, 1, 1)
                .returns(2, null, 2, null, null)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c3 = t2.c3 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c3 nulls last, t1.c2 nulls last, t1.c1 nulls last",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, null, 2, null, null)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .returns(null, 3, 3, null, null)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c3 nulls first, t1.c2 nulls first, t1.c1 nulls first",
                joinType
        )
                .ordered()
                .returns(null, 3, 3, 3, 3)
                .returns(1, 1, 1, 1, 1)
                .returns(2, null, 2, null, null)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 "
                + "  from t1 "
                + "  left join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t1.c3 nulls last, t1.c2 nulls last, t1.c1 nulls last",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 2)
                .returns(2, null, 2, null, null)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .returns(null, 3, 3, 3, 3)
                .check();
    }

    /**
     * Test verifies result of right join with different ordering.
     */
    @ParameterizedTest
    // right join is not supported by CNLJ
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testRightJoin(JoinType joinType) {
        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + " right join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t2.c1, t2.c2, t2.c3",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, null)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(null, null, 3, null, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + " right join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t2.c1, t2.c2 nulls first, t2.c3 nulls first",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, null)
                .returns(2, 2, 2, 2, 2)
                .returns(null, null, 3, null, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + " right join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t2.c1, t2.c2 nulls last, t2.c3 nulls last",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, null)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(null, null, 3, null, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + " right join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t2.c1 desc, t2.c2, t2.c3",
                joinType
        )
                .ordered()
                .returns(4, 4, 4, 4, 4)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(null, null, 3, null, 3)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, null)
                .returns(1, 1, 1, 1, 1)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + " right join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t2.c1 desc, t2.c2, t2.c3 nulls first",
                joinType
        )
                .ordered()
                .returns(4, 4, 4, 4, 4)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(null, null, 3, null, 3)
                .returns(2, 2, 2, 2, null)
                .returns(2, 2, 2, 2, 2)
                .returns(1, 1, 1, 1, 1)
                .check();

        assertQuery(""
                + "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + " right join t2 "
                + "    on t1.c1 = t2.c1 "
                + "   and t1.c2 = t2.c2 "
                + " order by t2.c1 desc, t2.c2, t2.c3 nulls last",
                joinType
        )
                .ordered()
                .returns(4, 4, 4, 4, 4)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(null, null, 3, null, 3)
                .returns(2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, null)
                .returns(1, 1, 1, 1, 1)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t2.c3 c23, t2.c2 c22 "
                + "  from t1 "
                + " right join t2 "
                + "    on t1.c3 = t2.c3 "
                + "   and t1.c2 = t2.c2 "
                + " order by t2.c3, t2.c2",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1)
                .returns(2, 2, 2, 2)
                .returns(3, 3, 3, 3)
                .returns(null, null, 3, null)
                .returns(4, 4, 4, 4)
                .returns(null, null, null, 2)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + " right join t2 "
                + "    on t1.c3 = t2.c3 "
                + "   and t1.c2 = t2.c2 "
                + " order by t2.c3 nulls first, t2.c2 nulls first, t2.c1 nulls first",
                joinType
        )
                .ordered()
                .returns(null, null, 2, 2, null)
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(null, null, 3, null, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + "  right join t2 "
                + "    on t1.c3 = t2.c3 "
                + "    and t1.c2 = t2.c2 "
                + " order by t2.c3 nulls last, t2.c2 nulls last, t2.c1 nulls last",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, 3, 3, 3)
                .returns(null, null, 3, null, 3)
                .returns(4, 4, 4, 4, 4)
                .returns(null, null, 2, 2, null)
                .check();

        assertQuery(""
                + "select t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + "  right join t2 "
                + "    on t1.c1 = t2.c1 "
                + "    and t1.c2 = t2.c2 "
                + " order by t2.c3 nulls first, t2.c2 nulls first, t2.c1 nulls first",
                joinType
        )
                .ordered()
                .returns(2, 2, 2, 2, null)
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(null, null, 3, null, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + "  right join t2 "
                + "    on t1.c1 = t2.c1 "
                + "    and t1.c2 = t2.c2 "
                + " order by t2.c3 nulls last, t2.c2 nulls last, t2.c1 nulls last",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2)
                .returns(3, 3, 3, 3, 3)
                .returns(3, 3, 3, 3, 3)
                .returns(null, null, 3, null, 3)
                .returns(4, 4, 4, 4, 4)
                .returns(2, 2, 2, 2, null)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + "  right join t2 "
                + "    on t1.c1 = t2.c1 "
                + "    and t1.c2 = t2.c2 "
                + "   and t1.c3 = t2.c3 "
                + " order by t2.c3 nulls first, t2.c2 nulls first, t2.c1 nulls first",
                joinType
        )
                .ordered()
                .returns(null, null, null, 2, 2, null)
                .returns(1, 1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2, 2)
                .returns(null, null, null, 3, null, 3)
                .returns(3, 3, 3, 3, 3, 3)
                .returns(4, 4, 4, 4, 4, 4)
                .check();

        assertQuery(""
                + "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22, t2.c3 c23 "
                + "  from t1 "
                + "  right join t2 "
                + "    on t1.c1 = t2.c1 "
                + "    and t1.c2 = t2.c2 "
                + "    and t1.c3 = t2.c3 "
                + " order by t2.c3 nulls last, t2.c2 nulls last, t2.c1 nulls last",
                joinType
        )
                .ordered()
                .returns(1, 1, 1, 1, 1, 1)
                .returns(2, 2, 2, 2, 2, 2)
                .returns(3, 3, 3, 3, 3, 3)
                .returns(null, null, null, 3, null, 3)
                .returns(4, 4, 4, 4, 4, 4)
                .returns(null, null, null, 2, 2, null)
                .check();
    }

    /**
     * Tests JOIN with USING clause.
     */
    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testJoinWithUsing(JoinType joinType) {
        // Select all join columns.
        assertQuery("SELECT * FROM t1 JOIN t2 USING (c1, c2)", joinType)
                .returns(1, 1, 0, 1, 0, 1)
                .returns(2, 2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 1, null)
                .returns(3, 3, 3, null, 4, 3)
                .returns(3, 3, 4, 3, 4, 3)
                .returns(4, 4, 5, 4, 5, 4)
                .check();

        // Select all table columns explicitly.
        assertQuery("SELECT t1.*, t2.* FROM t1 JOIN t2 USING (c1, c2)")
                .returns(0, 1, 1, 1, 0, 1, 1, 1)
                .returns(2, 2, 2, 2, 2, 2, 2, 2)
                .returns(2, 2, 2, 2, 1, 2, 2, null)
                .returns(3, 3, 3, null, 4, 3, 3, 3)
                .returns(4, 3, 3, 3, 4, 3, 3, 3)
                .returns(5, 4, 4, 4, 5, 4, 4, 4)
                .check();

        // Select explicit columns. Columns from using - not ambiguous.
        assertQuery("SELECT c1, c2, t1.c3, t2.c3 FROM t1 JOIN t2 USING (c1, c2) ORDER BY c1, c2")
                .returns(1, 1, 1, 1)
                .returns(2, 2, 2, null)
                .returns(2, 2, 2, 2)
                .returns(3, 3, null, 3)
                .returns(3, 3, 3, 3)
                .returns(4, 4, 4, 4)
                .check();
    }

    /**
     * Tests NATURAL JOIN.
     */
    @ParameterizedTest
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove exclude
    @EnumSource(mode = Mode.EXCLUDE, names = "CORRELATED")
    public void testNatural(JoinType joinType) {
        // Select all join columns.
        assertQuery("SELECT * FROM t1 NATURAL JOIN t2", joinType)
                .returns(0, 1, 1, 1)
                .returns(2, 2, 2, 2)
                .returns(4, 3, 3, 3)
                .returns(5, 4, 4, 4)
                .check();

        // Select all tables columns explicitly.
        assertQuery("SELECT t1.*, t2.* FROM t1 NATURAL JOIN t2", joinType)
                .returns(0, 1, 1, 1, 0, 1, 1, 1)
                .returns(2, 2, 2, 2, 2, 2, 2, 2)
                .returns(4, 3, 3, 3, 4, 3, 3, 3)
                .returns(5, 4, 4, 4, 5, 4, 4, 4)
                .check();

        // Select explicit columns.
        assertQuery("SELECT t1.c1, t2.c2, t1.c3, t2.c3 FROM t1 NATURAL JOIN t2", joinType)
                .returns(1, 1, 1, 1)
                .returns(2, 2, 2, 2)
                .returns(3, 3, 3, 3)
                .returns(4, 4, 4, 4)
                .check();

        // Columns - not ambiguous.
        // TODO https://issues.apache.org/jira/browse/CALCITE-4915
        // assertQuery("SELECT c1, c2, c3 FROM t1 NATURAL JOIN t2 ORDER BY c1, c2, c3")
        //    .returns(1, 1, 1)
        //    .returns(2, 2, 2)
        //    .returns(3, 3, 3)
        //    .returns(4, 4, 4)
        //    .check();
    }

    @Test
    public void testNonColocatedNaturalJoin() {
        sqlScript("CREATE TABLE tbl1 (a INTEGER, b INTEGER,  PRIMARY KEY(b));"
                + "CREATE TABLE tbl2 (a INTEGER, c INTEGER,  PRIMARY KEY(c));"
                + "CREATE TABLE tbl3 (a INTEGER, b INTEGER, c INTEGER, PRIMARY KEY(a))");

        sql("INSERT INTO tbl1 VALUES (1, 2)");
        sql("INSERT INTO tbl2 VALUES (1, 3), (2, 4)");
        sql("INSERT INTO tbl3 VALUES (1, 2, 3)");

        gatherStatistics();

        assertQuery("SELECT * FROM tbl1 NATURAL JOIN tbl2 NATURAL JOIN tbl3")
                .returns(1, 2, 3)
                .check();
    }

    /** Check IS NOT DISTINCT execution correctness and IndexSpool presence. */
    @ParameterizedTest(name = "join algo : {0}, index present: {1}")
    @MethodSource("joinTypes")
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void testIsNotDistinctFrom(JoinType joinType, boolean indexScan) {
        try {
            sql("CREATE TABLE t11(i1 INTEGER, i2 INTEGER)");

            if (indexScan) {
                sql("CREATE INDEX t11_idx ON t11(i1)");
            }

            sql("INSERT INTO t11 VALUES (1, null), (2, 2), (null, 3), (3, null), (5, null)");

            sql("CREATE TABLE t22(i3 INTEGER, i4 INTEGER)");

            if (indexScan) {
                sql("CREATE INDEX t22_idx ON t22(i3)");
            }

            sql("INSERT INTO t22 VALUES (1, 1), (2, 2), (null, 3), (4, null), (5, null)");

            String sql = "SELECT i1, i4 FROM t11 JOIN t22 ON i1 IS NOT DISTINCT FROM i3";

            assertQuery(sql, joinType, indexScan ? "LogicalTableScanConverterRule" : null)
                    .matches(QueryChecker.matches("(?i).*IS NOT DISTINCT.*"))
                    .matches(indexScan ? QueryChecker.containsIndexScanIgnoreBounds("PUBLIC", "T11") :
                            QueryChecker.containsTableScan("PUBLIC", "T11"))
                    .returns(1, 1)
                    .returns(2, 2)
                    .returns(5, null)
                    .returns(null, 3)
                    .check();

            sql = "SELECT i1, i4 FROM t11 JOIN t22 ON i1 IS NOT DISTINCT FROM i3 AND i2 = i4";

            assertQuery(sql, joinType, indexScan ? "LogicalTableScanConverterRule" : null)
                    .matches(QueryChecker.matches("(?i).*IS NOT DISTINCT.*"))
                    .matches(indexScan ? QueryChecker.containsIndexScanIgnoreBounds("PUBLIC", "T11") :
                            QueryChecker.containsTableScan("PUBLIC", "T11"))
                    .returns(2, 2)
                    .returns(null, 3)
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS t11");
            sql("DROP TABLE IF EXISTS t22");
        }
    }

    @ParameterizedTest
    @EnumSource(value = JoinType.class, names = {"NESTED_LOOP", "HASH"}, mode = Mode.INCLUDE)
    void partiallyEquiJoin(JoinType type) {
        assertQuery(""
                + "SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM"
                + "  (SELECT x::integer AS c1, x % 2 AS c2 FROM system_range(1, 10)) AS t1"
                + " JOIN"
                + "  (SELECT x::integer AS c1, x % 3 AS c2 FROM system_range(1, 10)) t2"
                + "   ON t1.c1 = t2.c1 AND t1.c2 < t2.c2", type
        )
                .returns(2, 0, 2, 2)
                .returns(4, 0, 4, 1)
                .returns(5, 1, 5, 2)
                .returns(8, 0, 8, 2)
                .returns(10, 0, 10, 1)
                .check();

        assertQuery(""
                + "SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM"
                + "  (SELECT x::integer AS c1, x % 2 AS c2 FROM system_range(1, 10)) AS t1"
                + " LEFT JOIN"
                + "  (SELECT x::integer AS c1, x % 3 AS c2 FROM system_range(1, 10)) t2"
                + "   ON t1.c1 = t2.c1 AND t1.c2 < t2.c2", type
        )
                .returns(1, 1, null, null)
                .returns(2, 0, 2, 2)
                .returns(3, 1, null, null)
                .returns(4, 0, 4, 1)
                .returns(5, 1, 5, 2)
                .returns(6, 0, null, null)
                .returns(7, 1, null, null)
                .returns(8, 0, 8, 2)
                .returns(9, 1, null, null)
                .returns(10, 0, 10, 1)
                .check();
    }

    private static Stream<Arguments> joinTypes() {
        Stream<Arguments> types = Arrays.stream(JoinType.values())
                // TODO: https://issues.apache.org/jira/browse/IGNITE-21286 remove filter below
                .filter(type -> type != JoinType.CORRELATED)
                // TODO: https://issues.apache.org/jira/browse/IGNITE-22074 hash join to make a deal with "is not distinct" expression
                .filter(type -> type != JoinType.HASH)
                .flatMap(v -> Stream.of(Arguments.of(v, false), Arguments.of(v, true)));

        return types;
    }

    @Test
    // TODO this test case can be removed after https://issues.apache.org/jira/browse/IGNITE-22295
    public void testNaturalJoinTypeMismatch() {
        try {
            sql("CREATE TABLE t1_ij (i INTEGER PRIMARY KEY, j INTEGER);");
            sql("CREATE TABLE t2_ij (i INTEGER PRIMARY KEY, j BIGINT);");

            var expectedMessage = "Column N#1 matched using NATURAL keyword or USING clause "
                    + "has incompatible types in this context: 'INTEGER' to 'BIGINT'";

            assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, expectedMessage, () -> sql("SELECT * FROM t1_ij NATURAL JOIN t2_ij"));
        } finally {
            sql("DROP TABLE t1_ij");
            sql("DROP TABLE t2_ij");
        }
    }

    @Test
    // TODO this test case can be removed after https://issues.apache.org/jira/browse/IGNITE-22295
    public void testUsingJoinTypeMismatch() {
        try {
            sql("CREATE TABLE t1_ij (i INTEGER PRIMARY KEY, j INTEGER);");
            sql("CREATE TABLE t2_ij (i INTEGER PRIMARY KEY, j BIGINT);");

            var expectedMessage = "Column N#1 matched using NATURAL keyword or USING clause "
                    + "has incompatible types in this context: 'INTEGER' to 'BIGINT'";

            assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, expectedMessage, () -> sql("SELECT * FROM t1_ij JOIN t2_ij USING (i)"));
        } finally {
            sql("DROP TABLE t1_ij");
            sql("DROP TABLE t2_ij");
        }
    }

    @Test
    public void testUnsupportedAsofJoin() {
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Unsupported join type: ASOF", () -> sql("SELECT *\n"
                + " FROM (VALUES (NULL, 0)) AS t1(k, t)\n"
                + " ASOF JOIN (VALUES (1, NULL)) AS t2(k, t)\n"
                + " MATCH_CONDITION t2.t < t1.t\n"
                + " ON t1.k = t2.k"));

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Unsupported join type: LEFT ASOF", () -> sql("SELECT *\n"
                + " FROM (VALUES (NULL, 0)) AS t1(k, t)\n"
                + " LEFT ASOF JOIN (VALUES (1, NULL)) AS t2(k, t)\n"
                + " MATCH_CONDITION t2.t < t1.t\n"
                + " ON t1.k = t2.k"));
    }
}
