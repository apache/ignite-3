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
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsAnyProject;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsAnyScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsSubPlan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsTableScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsUnion;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.not;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Basic index tests.
 */
public class ItSecondaryIndexTest extends BaseSqlIntegrationTest {
    private static final String DEPID_IDX = "DEPID_IDX";

    private static final String NAME_CITY_IDX = "NAME_CITY_IDX";

    private static final String NAME_DEPID_CITY_IDX = "NAME_DEPID_CITY_IDX";

    private static final String NAME_DATE_IDX = "NAME_DATE_IDX";

    private static final AtomicInteger TABLE_IDX = new AtomicInteger();

    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        sql("CREATE TABLE developer (id INT, name VARCHAR, depid INT, city VARCHAR, age INT, PRIMARY KEY USING SORTED (id))");
        sql("CREATE INDEX " + DEPID_IDX + " ON developer (depid)");
        sql("CREATE INDEX " + NAME_CITY_IDX + " ON developer (name DESC, city DESC)");
        sql("CREATE INDEX " + NAME_DEPID_CITY_IDX + " ON developer (name DESC, depid DESC, city DESC)");

        sql("CREATE TABLE birthday (id INT, name VARCHAR, birthday DATE, PRIMARY KEY USING SORTED (id))");
        sql("CREATE INDEX " + NAME_DATE_IDX + " ON birthday (name, birthday)");

        insertData("BIRTHDAY", List.of("ID", "NAME", "BIRTHDAY"), new Object[][]{
                {1, "Mozart", LocalDate.parse("1756-01-27")},
                {2, "Beethoven", LocalDate.parse("1756-01-27")},
                {3, "Bach", LocalDate.parse("1756-01-27")},
                {4, "Strauss", LocalDate.parse("1756-01-27")},
                {5, "Vagner", LocalDate.parse("1756-01-27")},
                {6, "Chaikovsky", LocalDate.parse("1756-01-27")},
                {7, "Verdy", LocalDate.parse("1756-01-27")},
                {8, null, null},
        });

        insertData("DEVELOPER", List.of("ID", "NAME", "DEPID", "CITY", "AGE"), new Object[][]{
                {1, "Mozart", 3, "Vienna", 33},
                {2, "Beethoven", 2, "Vienna", 44},
                {3, "Bach", 1, "Leipzig", 55},
                {4, "Strauss", 2, "Munich", 66},
                {5, "Vagner", 4, "Leipzig", 70},
                {6, "Chaikovsky", 5, "Votkinsk", 53},
                {7, "Verdy", 6, "Rankola", 88},
                {8, "Stravinsky", 7, "Spt", 89},
                {9, "Rahmaninov", 8, "Starorussky ud", 70},
                {10, "Shubert", 9, "Vienna", 31},
                {11, "Glinka", 10, "Smolenskaya gb", 53},
                {12, "Einaudi", 11, "", -1},
                {13, "Glass", 12, "", -1},
                {14, "Rihter", 13, "", -1},
                {15, "Marradi", 14, "", -1},
                {16, "Zimmer", 15, "", -1},
                {17, "Hasaishi", 16, "", -1},
                {18, "Arnalds", 17, "", -1},
                {19, "Yiruma", 18, "", -1},
                {20, "O'Halloran", 19, "", -1},
                {21, "Cacciapaglia", 20, "", -1},
                {22, "Prokofiev", 21, "", -1},
                {23, "Musorgskii", 22, "", -1}
        });

        sql("CREATE TABLE assignments(developer_id INT, project_id INT, primary key(developer_id, project_id))");

        insertData("ASSIGNMENTS", List.of("DEVELOPER_ID", "PROJECT_ID"), new Object[][]{
                {1, 1}, {1, 2}, {2, 3}, {4, 1}, {4, 2}, {5, 6},
        });

        sql("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)");
        sql("CREATE INDEX t1_idx on t1(val DESC)");

        insertData("T1", List.of("ID", "VAL"), new Object[][]{
                {1, null},
                {2, null},
                {3, 3},
                {4, 4},
                {5, 5},
                {6, 6},
                {7, null}
        });
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21286")
    public void testIndexLoopJoin() {
        assertQuery("SELECT /*+ DISABLE_RULE('HashJoinConverter', 'MergeJoinConverter', 'NestedLoopJoinConverter') */ d1.name, d2.name "
                + "FROM Developer d1, Developer d2 WHERE d1.id = d2.id")
                .matches(containsSubPlan("CorrelatedNestedLoopJoin"))
                .returns("Bach", "Bach")
                .returns("Beethoven", "Beethoven")
                .returns("Mozart", "Mozart")
                .returns("Strauss", "Strauss")
                .returns("Vagner", "Vagner")
                .returns("Chaikovsky", "Chaikovsky")
                .returns("Verdy", "Verdy")
                .returns("Stravinsky", "Stravinsky")
                .returns("Rahmaninov", "Rahmaninov")
                .returns("Shubert", "Shubert")
                .returns("Glinka", "Glinka")
                .returns("Arnalds", "Arnalds")
                .returns("Glass", "Glass")
                .returns("O'Halloran", "O'Halloran")
                .returns("Prokofiev", "Prokofiev")
                .returns("Yiruma", "Yiruma")
                .returns("Cacciapaglia", "Cacciapaglia")
                .returns("Einaudi", "Einaudi")
                .returns("Hasaishi", "Hasaishi")
                .returns("Marradi", "Marradi")
                .returns("Musorgskii", "Musorgskii")
                .returns("Rihter", "Rihter")
                .returns("Zimmer", "Zimmer")
                .check();
    }

    // ===== No filter =====

    @Test
    public void testNoFilter() {
        assertQuery("SELECT * FROM Developer")
                .matches(containsTableScan("PUBLIC", "DEVELOPER"))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(4, "Strauss", 2, "Munich", 66)
                .returns(5, "Vagner", 4, "Leipzig", 70)
                .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
                .returns(7, "Verdy", 6, "Rankola", 88)
                .returns(8, "Stravinsky", 7, "Spt", 89)
                .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
                .returns(10, "Shubert", 9, "Vienna", 31)
                .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
                .returns(12, "Einaudi", 11, "", -1)
                .returns(13, "Glass", 12, "", -1)
                .returns(14, "Rihter", 13, "", -1)
                .returns(15, "Marradi", 14, "", -1)
                .returns(16, "Zimmer", 15, "", -1)
                .returns(17, "Hasaishi", 16, "", -1)
                .returns(18, "Arnalds", 17, "", -1)
                .returns(19, "Yiruma", 18, "", -1)
                .returns(20, "O'Halloran", 19, "", -1)
                .returns(21, "Cacciapaglia", 20, "", -1)
                .returns(22, "Prokofiev", 21, "", -1)
                .returns(23, "Musorgskii", 22, "", -1)
                .check();
    }

    // ===== id filter =====

    @Test
    public void testKeyEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id=2")
                .matches(containsSubPlan("KeyValueGet(table=[[PUBLIC, DEVELOPER]]"))
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .check();
    }

    @Test
    public void testKeyGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE id>? and id<?")
                .withParams(3, 12)
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", "DEVELOPER_PK"))
                .returns(4, "Strauss", 2, "Munich", 66)
                .returns(5, "Vagner", 4, "Leipzig", 70)
                .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
                .returns(7, "Verdy", 6, "Rankola", 88)
                .returns(8, "Stravinsky", 7, "Spt", 89)
                .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
                .returns(10, "Shubert", 9, "Vienna", 31)
                .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
                .check();
    }

    @Test
    public void testKeyGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id>=3 and id<12")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", "DEVELOPER_PK"))
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(4, "Strauss", 2, "Munich", 66)
                .returns(5, "Vagner", 4, "Leipzig", 70)
                .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
                .returns(7, "Verdy", 6, "Rankola", 88)
                .returns(8, "Stravinsky", 7, "Spt", 89)
                .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
                .returns(10, "Shubert", 9, "Vienna", 31)
                .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
                .check();
    }

    @Test
    public void testKeyLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE id<3")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", "DEVELOPER_PK"))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .check();
    }

    @Test
    public void testKeyLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id<=2")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", "DEVELOPER_PK"))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .check();
    }

    // ===== indexed field filter =====

    @Test
    public void testIndexedFieldEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId=2")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(4, "Strauss", 2, "Munich", 66)
                .check();
    }

    @Test
    public void testIndexedFieldGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId>21")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(23, "Musorgskii", 22, "", -1)
                .check();
    }

    @Test
    public void testIndexedFieldGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId>=21")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(22, "Prokofiev", 21, "", -1)
                .returns(23, "Musorgskii", 22, "", -1)
                .check();
    }

    @Test
    public void testIndexedFieldLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId<?")
                .withParams(3)
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(4, "Strauss", 2, "Munich", 66)
                .check();
    }

    @Test
    public void testIndexedFieldLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId<=?")
                .withParams(2)
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(4, "Strauss", 2, "Munich", 66)
                .check();
    }

    // ===== non-indexed field filter =====

    @Test
    public void testNonIndexedFieldEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age=?")
                .withParams(44)
                .matches(containsTableScan("PUBLIC", "DEVELOPER"))
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .check();
    }

    @Test
    public void testNonIndexedFieldGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE age>?")
                .withParams(50)
                .matches(containsTableScan("PUBLIC", "DEVELOPER"))
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(4, "Strauss", 2, "Munich", 66)
                .returns(5, "Vagner", 4, "Leipzig", 70)
                .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
                .returns(7, "Verdy", 6, "Rankola", 88)
                .returns(8, "Stravinsky", 7, "Spt", 89)
                .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
                .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
                .check();
    }

    @Test
    public void testNonIndexedFieldGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age>=?")
                .withParams(34)
                .matches(containsTableScan("PUBLIC", "DEVELOPER"))
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(4, "Strauss", 2, "Munich", 66)
                .returns(5, "Vagner", 4, "Leipzig", 70)
                .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
                .returns(7, "Verdy", 6, "Rankola", 88)
                .returns(8, "Stravinsky", 7, "Spt", 89)
                .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
                .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
                .check();
    }

    @Test
    public void testNonIndexedFieldLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE age<?")
                .withParams(56)
                .matches(containsTableScan("PUBLIC", "DEVELOPER"))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
                .returns(10, "Shubert", 9, "Vienna", 31)
                .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
                .returns(12, "Einaudi", 11, "", -1)
                .returns(13, "Glass", 12, "", -1)
                .returns(14, "Rihter", 13, "", -1)
                .returns(15, "Marradi", 14, "", -1)
                .returns(16, "Zimmer", 15, "", -1)
                .returns(17, "Hasaishi", 16, "", -1)
                .returns(18, "Arnalds", 17, "", -1)
                .returns(19, "Yiruma", 18, "", -1)
                .returns(20, "O'Halloran", 19, "", -1)
                .returns(21, "Cacciapaglia", 20, "", -1)
                .returns(22, "Prokofiev", 21, "", -1)
                .returns(23, "Musorgskii", 22, "", -1)
                .check();
    }

    @Test
    public void testNonIndexedFieldLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age<=?")
                .withParams(55)
                .matches(containsTableScan("PUBLIC", "DEVELOPER"))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
                .returns(10, "Shubert", 9, "Vienna", 31)
                .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
                .returns(12, "Einaudi", 11, "", -1)
                .returns(13, "Glass", 12, "", -1)
                .returns(14, "Rihter", 13, "", -1)
                .returns(15, "Marradi", 14, "", -1)
                .returns(16, "Zimmer", 15, "", -1)
                .returns(17, "Hasaishi", 16, "", -1)
                .returns(18, "Arnalds", 17, "", -1)
                .returns(19, "Yiruma", 18, "", -1)
                .returns(20, "O'Halloran", 19, "", -1)
                .returns(21, "Cacciapaglia", 20, "", -1)
                .returns(22, "Prokofiev", 21, "", -1)
                .returns(23, "Musorgskii", 22, "", -1)
                .check();
    }

    // ===== various complex conditions =====

    @Test
    public void testComplexIndexCondition1() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition2() {
        assertQuery("SELECT * FROM Developer WHERE depId=? AND name=?")
                .withParams(3, "Mozart")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition3() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Vienna'")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition4() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Leipzig'")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
                .check();
    }

    @Test
    public void testComplexIndexCondition5() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND city='Vienna'")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition6() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition7() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=2")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition8() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=2 AND age>20")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition9() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId>=2 AND city>='Vienna'")
                .matches(containsAnyScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX, NAME_DEPID_CITY_IDX, DEPID_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(10, "Shubert", 9, "Vienna", 31)
                .check();
    }

    @Test
    public void testComplexIndexCondition10() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND city>='Vienna'")
                .matches(containsAnyScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX, NAME_DEPID_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(10, "Shubert", 9, "Vienna", 31)
                .check();
    }

    @Test
    public void testComplexIndexCondition11() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3 AND city>='Vienna'")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition12() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Vienna'")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition13() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=3 AND city='Vienna'")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition14() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3 AND city>='Vienna'")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition15() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND city='Vienna'")
                .matches(containsTableScan("PUBLIC", "DEVELOPER"))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testComplexIndexCondition16() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND (city='Vienna' AND depId=3)")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testEmptyResult() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND city='Leipzig'")
                .matches(containsTableScan("PUBLIC", "DEVELOPER"))
                .check();
    }

    @Test
    public void testOrCondition1() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' OR age=55")
                .matches(containsTableScan("PUBLIC", "DEVELOPER"))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .check();
    }

    @Test
    public void testOrCondition2() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND (depId=1 OR depId=3)")
                .matches(not(containsUnion()))
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testOrCondition3() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND (age > 22 AND (depId=1 OR depId=3))")
                .matches(not(containsUnion()))
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .check();
    }

    @Test
    public void testOrCondition4() {
        assertQuery("SELECT * FROM Developer WHERE depId=1 OR (name='Mozart' AND depId=3)")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .check();
    }

    @Test
    public void testOrCondition5() {
        assertQuery("SELECT * FROM Developer WHERE depId=1 OR name='Mozart'")
                .matches(containsUnion(true))
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .check();
    }

    // ===== various complex conditions =====

    @Test
    public void testOrderByKey() {
        assertQuery("SELECT * FROM Developer WHERE id<=4 ORDER BY id")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", "DEVELOPER_PK"))
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(4, "Strauss", 2, "Munich", 66)
                .ordered()
                .check();
    }

    @Test
    public void testOrderByDepId() {
        assertQuery("SELECT depid FROM Developer ORDER BY depId")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .matches(not(containsSubPlan("Sort")))
                .returns(1) // Bach
                .returns(2) // Beethoven or Strauss
                .returns(2) // Strauss or Beethoven
                .returns(3) // Mozart
                .returns(4) // Vagner
                .returns(5) // Chaikovsky
                .returns(6) // Verdy
                .returns(7) // Stravinsky
                .returns(8) // Rahmaninov
                .returns(9) // Shubert
                .returns(10) // Glinka

                .returns(11) // Einaudi
                .returns(12) // Glass
                .returns(13) // Rihter
                .returns(14) // Marradi
                .returns(15) // Zimmer
                .returns(16) // Hasaishi
                .returns(17) // Arnalds
                .returns(18) // Yiruma
                .returns(19) // O'Halloran
                .returns(20) // Cacciapaglia
                .returns(21) // Prokofiev
                .returns(22) // Musorgskii

                .ordered()
                .check();
    }

    @Test
    public void testOrderByNameCityAsc() {
        assertQuery("SELECT * FROM Developer ORDER BY name, city")
                .matches(containsAnyScan("PUBLIC", "DEVELOPER"))
                .matches(containsAnyScan("PUBLIC", "DEVELOPER"))
                .matches(containsSubPlan("Sort"))
                .returns(18, "Arnalds", 17, "", -1)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(21, "Cacciapaglia", 20, "", -1)
                .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
                .returns(12, "Einaudi", 11, "", -1)
                .returns(13, "Glass", 12, "", -1)
                .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
                .returns(17, "Hasaishi", 16, "", -1)
                .returns(15, "Marradi", 14, "", -1)
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(23, "Musorgskii", 22, "", -1)
                .returns(20, "O'Halloran", 19, "", -1)
                .returns(22, "Prokofiev", 21, "", -1)
                .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
                .returns(14, "Rihter", 13, "", -1)
                .returns(10, "Shubert", 9, "Vienna", 31)
                .returns(4, "Strauss", 2, "Munich", 66)
                .returns(8, "Stravinsky", 7, "Spt", 89)
                .returns(5, "Vagner", 4, "Leipzig", 70)
                .returns(7, "Verdy", 6, "Rankola", 88)
                .returns(19, "Yiruma", 18, "", -1)
                .returns(16, "Zimmer", 15, "", -1)
                .ordered()
                .check();
    }

    @Test
    public void testOrderByNameCityDesc() {
        assertQuery("SELECT ID, NAME, DEPID, CITY, AGE FROM Developer ORDER BY name DESC, city DESC")
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX))
                .matches(not(containsSubPlan("Sort")))
                .returns(16, "Zimmer", 15, "", -1)
                .returns(19, "Yiruma", 18, "", -1)
                .returns(7, "Verdy", 6, "Rankola", 88)
                .returns(5, "Vagner", 4, "Leipzig", 70)
                .returns(8, "Stravinsky", 7, "Spt", 89)
                .returns(4, "Strauss", 2, "Munich", 66)
                .returns(10, "Shubert", 9, "Vienna", 31)
                .returns(14, "Rihter", 13, "", -1)
                .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
                .returns(22, "Prokofiev", 21, "", -1)
                .returns(20, "O'Halloran", 19, "", -1)
                .returns(23, "Musorgskii", 22, "", -1)
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(15, "Marradi", 14, "", -1)
                .returns(17, "Hasaishi", 16, "", -1)
                .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
                .returns(13, "Glass", 12, "", -1)
                .returns(12, "Einaudi", 11, "", -1)
                .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
                .returns(21, "Cacciapaglia", 20, "", -1)
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(18, "Arnalds", 17, "", -1)
                .ordered()
                .check();
    }

    @Test
    public void testOrderByNoIndexedColumn() {
        assertQuery("SELECT * FROM Developer ORDER BY age DESC, depid ASC")
                .matches(containsAnyProject("PUBLIC", "DEVELOPER"))
                .matches(containsSubPlan("Sort"))
                .returns(8, "Stravinsky", 7, "Spt", 89)
                .returns(7, "Verdy", 6, "Rankola", 88)
                .returns(5, "Vagner", 4, "Leipzig", 70)
                .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
                .returns(4, "Strauss", 2, "Munich", 66)
                .returns(3, "Bach", 1, "Leipzig", 55)
                .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
                .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
                .returns(2, "Beethoven", 2, "Vienna", 44)
                .returns(1, "Mozart", 3, "Vienna", 33)
                .returns(10, "Shubert", 9, "Vienna", 31)
                .returns(12, "Einaudi", 11, "", -1)
                .returns(13, "Glass", 12, "", -1)
                .returns(14, "Rihter", 13, "", -1)
                .returns(15, "Marradi", 14, "", -1)
                .returns(16, "Zimmer", 15, "", -1)
                .returns(17, "Hasaishi", 16, "", -1)
                .returns(18, "Arnalds", 17, "", -1)
                .returns(19, "Yiruma", 18, "", -1)
                .returns(20, "O'Halloran", 19, "", -1)
                .returns(21, "Cacciapaglia", 20, "", -1)
                .returns(22, "Prokofiev", 21, "", -1)
                .returns(23, "Musorgskii", 22, "", -1)
                .ordered()
                .check();
    }

    /**
     * Test verifies that ranges would be serialized and deserialized without any errors.
     */
    @Test
    public void testSelectWithRanges() {
        String sql = "select depId from Developer "
                + "where depId in (1,2,3,5,6,7,9,10,13,14,15,18,19,20,21,22,23,24,25,26,27,28,30,31,32,33) "
                + "   or depId between 7 and 8 order by depId limit 5";

        assertQuery(sql)
                .returns(1)
                .returns(2)
                .returns(2)
                .returns(3)
                .returns(5)
                .check();
    }

    /**
     * Test scan correctly handle 'nulls' when range condition is used.
     */
    @Test
    public void testIndexedNullableFieldGreaterThanFilter() {
        assertQuery("SELECT * FROM T1 WHERE val > 4")
                .matches(containsIndexScan("PUBLIC", "T1", "T1_IDX"))
                .returns(5, 5)
                .returns(6, 6)
                .check();
    }

    /**
     * Test index search bounds merge.
     */
    @Test
    public void testIndexBoundsMerge() {
        assertQuery("SELECT id FROM Developer WHERE depId < 2 AND depId < ?")
                .withParams(3)
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .matches(containsString("searchBounds=[[RangeBounds [lowerBound=null, upperBound=$LEAST2(2, ?0)"))
                .returns(3)
                .check();

        assertQuery("SELECT id FROM Developer WHERE depId > 19 AND depId > ?")
                .withParams(20)
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .matches(containsString("searchBounds=[[RangeBounds [lowerBound=$GREATEST2(19, ?0), upperBound=null:INTEGER"))
                .returns(22)
                .returns(23)
                .check();

        assertQuery("SELECT id FROM Developer WHERE depId > 20 AND depId > ?")
                .withParams(19)
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .matches(containsString("searchBounds=[[RangeBounds [lowerBound=$GREATEST2(20, ?0), upperBound=null:INTEGER"))
                .returns(22)
                .returns(23)
                .check();

        assertQuery("SELECT id FROM Developer WHERE depId >= 20 AND depId > ?")
                .withParams(19)
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .matches(containsString("searchBounds=[[RangeBounds [lowerBound=$GREATEST2(20, ?0), upperBound=null:INTEGER"))
                .returns(21)
                .returns(22)
                .returns(23)
                .check();

        assertQuery("SELECT id FROM Developer WHERE depId BETWEEN ? AND ? AND depId > 19")
                .withParams(19, 21)
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .matches(containsString("searchBounds=[[RangeBounds [lowerBound=$GREATEST2(?0, 19), upperBound=?1"))
                .returns(21)
                .returns(22)
                .check();

        // Index with DESC ordering.
        assertQuery("SELECT id FROM Birthday WHERE name BETWEEN 'B' AND 'D' AND name > ?")
                .withParams("Bach")
                .matches(containsIndexScan("PUBLIC", "BIRTHDAY", NAME_DATE_IDX))
                .matches(containsString("searchBounds=[[RangeBounds [lowerBound=$GREATEST2(_UTF-8'B':VARCHAR(65536) "
                        + "CHARACTER SET \"UTF-8\", ?0), upperBound=_UTF-8'D':VARCHAR(65536) CHARACTER SET \"UTF-8\""))
                .returns(2)
                .returns(6)
                .check();
    }

    /**
     * Test scan correctly handle 'nulls' when range condition is used.
     */
    @Test
    public void testIndexedNullableFieldLessThanFilter() {
        assertQuery("SELECT * FROM T1 WHERE val <= 5")
                .matches(containsIndexScan("PUBLIC", "T1", "T1_IDX"))
                .returns(3, 3)
                .returns(4, 4)
                .returns(5, 5)
                .check();
    }

    @Test
    public void testNotNullCondition() {
        // IS NOT NULL predicate has low selectivity, thus, given the cost of the index scan,
        // it's considered cheaper to scan the whole table instead. Let's force planner to use
        // index of interest
        assertQuery("SELECT /*+ FORCE_INDEX(t1_idx) */ t1.* FROM T1 WHERE val is not null")
                .matches(containsIndexScan("PUBLIC", "T1", "T1_IDX"))
                .matches(not(containsUnion()))
                .returns(3, 3)
                .returns(4, 4)
                .returns(5, 5)
                .returns(6, 6)
                .check();

        // Not nullable column, filter is always - false.
        assertQuery("SELECT * FROM T1 WHERE id IS NULL")
                .matches(QueryChecker.matches(".*tuples=\\[\\[\\]\\].*"))
                .check();
    }

    /**
     * Test index search bounds on complex index expression.
     */
    @Test
    public void testComplexIndexExpression() {
        assertQuery("SELECT id FROM Developer WHERE depId BETWEEN ? - 1 AND ? + 1")
                .withParams(20, 20)
                .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
                .returns(20)
                .returns(21)
                .returns(22)
                .check();

        assertQuery("SELECT id FROM Birthday WHERE name = SUBSTRING(?::VARCHAR, 1, 4)")
                .withParams("BachBach")
                .matches(containsIndexScan("PUBLIC", "BIRTHDAY", NAME_DATE_IDX))
                .returns(3)
                .check();

        assertQuery("SELECT id FROM Birthday WHERE name = SUBSTRING(name, 1, 4)")
                .matches(containsTableScan("PUBLIC", "BIRTHDAY"))
                .returns(3)
                .check();
    }

    @Test
    public void testNullCondition1() {
        assertQuery("SELECT * FROM T1 WHERE val is null")
                .matches(containsIndexScan("PUBLIC", "T1", "T1_IDX"))
                .matches(not(containsUnion()))
                .returns(1, null)
                .returns(2, null)
                .returns(7, null)
                .check();
    }

    @Test
    public void testNullCondition2() {
        assertQuery("SELECT * FROM T1 WHERE (val <= 5) or (val is null)")
                .matches(containsIndexScan("PUBLIC", "T1", "T1_IDX"))
                .matches(not(containsUnion()))
                .returns(1, null)
                .returns(2, null)
                .returns(3, 3)
                .returns(4, 4)
                .returns(5, 5)
                .returns(7, null)
                .check();
    }

    @Test
    public void testNullCondition3() {
        assertQuery("SELECT * FROM T1 WHERE (val >= 5) or (val is null)")
                .matches(containsIndexScan("PUBLIC", "T1", "T1_IDX"))
                .matches(not(containsUnion()))
                .returns(1, null)
                .returns(2, null)
                .returns(5, 5)
                .returns(6, 6)
                .returns(7, null)
                .check();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21286")
    public void testNullsInCorrNestedLoopJoinSearchRow() {
        try {
            sql("CREATE TABLE t(i0 INTEGER PRIMARY KEY, i1 INTEGER, i2 INTEGER)");
            sql("CREATE INDEX t_idx ON t(i1)");
            sql("INSERT INTO t VALUES (1, 0, null), (2, 1, null), (3, 2, 2), (4, 3, null), (5, 4, null), (6, null, 5)");

            String sql = "SELECT t1.i1, t2.i1 FROM t t1 LEFT JOIN t t2 ON t1.i2 = t2.i1";

            assertQuery(sql)
                    .disableRules("NestedLoopJoinConverter", "MergeJoinConverter", "HashJoinConverter")
                    .matches(containsSubPlan("CorrelatedNestedLoopJoin"))
                    .matches(containsIndexScan("PUBLIC", "T", "T_IDX"))
                    .returns(0, null)
                    .returns(1, null)
                    .returns(2, 2)
                    .returns(3, null)
                    .returns(4, null)
                    .returns(null, null)
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS t");
        }
    }

    @Test
    public void testNullsInSearchRow() {
        try {
            sql("CREATE TABLE t(i0 INTEGER PRIMARY KEY, i1 INTEGER, i2 INTEGER)");
            sql("CREATE INDEX t_idx ON t(i1, i2)");
            sql("INSERT INTO t VALUES (1, null, 0), (2, 1, null), (3, 2, 2), (4, 3, null)");

            assertQuery("SELECT * FROM t WHERE i1 = ?")
                    .withParams(null)
                    .matches(containsIndexScan("PUBLIC", "T", "T_IDX"))
                    .check();

            assertQuery("SELECT * FROM t WHERE i1 = 1 AND i2 = ?")
                    .withParams(new Object[] { null })
                    .matches(containsIndexScan("PUBLIC", "T", "T_IDX"))
                    .check();

            // Multi ranges.
            assertQuery("SELECT * FROM t WHERE i1 IN (1, 2, 3) AND i2 = ?")
                    .withParams(new Object[] { null })
                    .matches(containsIndexScan("PUBLIC", "T", "T_IDX"))
                    .check();

            assertQuery("SELECT i1, i2 FROM t WHERE i1 IN (1, 2) AND i2 IS NULL")
                    .matches(containsIndexScan("PUBLIC", "T", "T_IDX"))
                    .returns(1, null)
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS t");
        }
    }

    /**
     * Saturated value are placed in search bounds of a sorted index.
     */
    @Test
    public void testSaturatedBoundsSortedIndex() {
        sql("CREATE TABLE t100 (ID INTEGER PRIMARY KEY, VAL TINYINT)");
        sql("CREATE INDEX t100_idx ON t100 (VAL)");

        sql("INSERT INTO t100 VALUES (1, 127)");

        assertQuery("SELECT * FROM t100 WHERE val = 1024").returnNothing().check();
    }

    /**
     * Saturated value are placed in search bounds of a hash index.
     */
    @Test
    public void testSaturatedBoundsHashIndex() {
        sql("CREATE TABLE t200 (ID INTEGER PRIMARY KEY, VAL TINYINT)");
        sql("CREATE INDEX t200_idx ON t200 USING HASH (VAL)");

        sql("INSERT INTO t200 VALUES (1, 127)");

        assertQuery("SELECT * FROM t200 WHERE val = 1024").returnNothing().check();
    }

    @Test
    public void testScanBooleanField() {
        try {
            sql("CREATE TABLE t(i INTEGER PRIMARY KEY, b BOOLEAN)");
            sql("CREATE INDEX t_idx ON t(b)");
            sql("INSERT INTO t VALUES (0, TRUE), (1, TRUE), (2, FALSE), (3, FALSE), (4, null)");

            assertQuery("SELECT i FROM t WHERE b = TRUE")
                    .matches(containsIndexScan("PUBLIC", "T", "T_IDX"))
                    .returns(0)
                    .returns(1)
                    .check();

            assertQuery("SELECT i FROM t WHERE b = FALSE")
                    .matches(containsIndexScan("PUBLIC", "T", "T_IDX"))
                    .returns(2)
                    .returns(3)
                    .check();

            assertQuery("SELECT i FROM t WHERE b IS TRUE")
                    .matches(containsIndexScan("PUBLIC", "T", "T_IDX"))
                    .returns(0)
                    .returns(1)
                    .check();

            assertQuery("SELECT i FROM t WHERE b IS FALSE")
                    .matches(containsIndexScan("PUBLIC", "T", "T_IDX"))
                    .returns(2)
                    .returns(3)
                    .check();

            assertQuery("SELECT i FROM t WHERE b IS NULL")
                    .matches(containsIndexScan("PUBLIC", "T", "T_IDX"))
                    .returns(4)
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS t");
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19964")
    public void testScanBooleanFieldMostlyPopulatedWithTrueValues() {
        try {
            sql("CREATE TABLE t_true(i INTEGER PRIMARY KEY, b BOOLEAN)");
            sql("INSERT INTO t_true VALUES (0, TRUE), (1, TRUE), (2, TRUE), (3, TRUE), (4, FALSE)");
            sql("CREATE INDEX t_true_idx ON t_true(b)");

            assertQuery("SELECT i FROM t_true WHERE b IS NOT TRUE")
                    .matches(containsIndexScan("PUBLIC", "T_TRUE", "T_TRUE_IDX"))
                    .returns(4)
                    .check();

            assertQuery("SELECT i FROM t_true WHERE b = FALSE or b is NULL")
                    .matches(containsIndexScan("PUBLIC", "T_TRUE", "T_TRUE_IDX"))
                    .returns(4)
                    .check();

            assertQuery("SELECT i FROM t_true WHERE b IS NOT FALSE")
                    .matches(containsTableScan("PUBLIC", "T_TRUE"))
                    .returns(0)
                    .returns(1)
                    .returns(2)
                    .returns(3)
                    .check();

            assertQuery("SELECT i FROM t_true WHERE b = TRUE or b is NULL")
                    .matches(containsTableScan("PUBLIC", "T_TRUE"))
                    .returns(0)
                    .returns(1)
                    .returns(2)
                    .returns(3)
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS t_true");
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19964")
    public void testScanBooleanFieldMostlyPopulatedWithFalseValues() {
        try {
            sql("CREATE TABLE t_false(i INTEGER PRIMARY KEY, b BOOLEAN)");
            sql("INSERT INTO t_false VALUES (0, FALSE), (1, FALSE), (2, FALSE), (3, FALSE), (4, TRUE)");
            sql("CREATE INDEX t_false_idx ON t_false(b)");

            assertQuery("SELECT i FROM t_false WHERE b IS NOT FALSE")
                    .matches(containsIndexScan("PUBLIC", "T_FALSE", "T_FALSE_IDX"))
                    .returns(4)
                    .check();

            assertQuery("SELECT i FROM t_false WHERE b = TRUE or b is NULL")
                    .matches(containsIndexScan("PUBLIC", "T_FALSE", "T_FALSE_IDX"))
                    .returns(4)
                    .check();

            assertQuery("SELECT i FROM t_false WHERE b IS NOT TRUE")
                    .matches(containsTableScan("PUBLIC", "T_FALSE"))
                    .returns(0)
                    .returns(1)
                    .returns(2)
                    .returns(3)
                    .check();

            assertQuery("SELECT i FROM t_false WHERE b = FALSE or b is NULL")
                    .matches(containsTableScan("PUBLIC", "T_FALSE"))
                    .returns(0)
                    .returns(1)
                    .returns(2)
                    .returns(3)
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS t_false");
        }
    }

    /**
     * Regression test to ensure predicate by part of the primary key and
     * trimming of all other fields won't cause optimizer to choose
     * {@link IgniteKeyValueGet Key Value Lookup node}.
     */
    @Test
    void lookupByPartialKey() {
        assertQuery("SELECT developer_id FROM ASSIGNMENTS WHERE developer_id = 1")
                .matches(containsTableScan("PUBLIC", "ASSIGNMENTS"))
                .returns(1)
                .returns(1)
                .check();
    }

    @ParameterizedTest
    @CsvSource(value = {
            // type, literal
            "TINYINT;50",
            "TINYINT;50::BIGINT",
            "TINYINT;50::DECIMAL(10)",
            "TINYINT;50::REAL",

            "REAL;50.00",
            "REAL;50.00::REAL",
            "REAL;50.00::DOUBLE",
            "REAL;50",

            "DOUBLE;50.00",
            "DOUBLE;50.00::REAL",
            "DOUBLE;50.00::DECIMAL(10,2)",
            "DOUBLE;50",

            "DECIMAL(10, 2);50.00", // DECIMAL(10,2)
            "DECIMAL(10, 2);50.00::REAL",
            "DECIMAL(10, 2);50.00::DOUBLE",
            "DECIMAL(10, 2);50",
    }, delimiter = ';')
    public void testTypeCastsIndexBounds(String type, String val) {
        int id = TABLE_IDX.getAndIncrement();

        sql(format("create table tt_{}(id INTEGER PRIMARY KEY, field_1 {})", id, type, val));

        sql(format("SELECT * FROM tt_{} WHERE field_1 = {}", id, val));

        sql(format("CREATE INDEX tt_idx_{} ON tt_{} (field_1)", id, id));

        sql(format("SELECT * FROM tt_{} WHERE field_1 = {}", id, val));
    }
}
