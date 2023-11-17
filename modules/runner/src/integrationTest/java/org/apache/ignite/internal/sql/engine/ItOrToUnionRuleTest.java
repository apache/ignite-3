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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsTableScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsUnion;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;

import java.util.List;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test OR -> UnionAll rewrite rule.
 *
 * <p>Example: SELECT * FROM products WHERE category = 'Photo' OR subcategory ='Camera Media';
 *
 * <p>A query above will be rewritten to next (or equivalient similar query)
 *
 * <p>SELECT * FROM products WHERE category = 'Photo' UNION ALL SELECT * FROM products WHERE subcategory ='Camera Media' AND LNNVL(category,
 * 'Photo');
 */
public class ItOrToUnionRuleTest extends BaseSqlIntegrationTest {
    public static final String IDX_SUBCAT_ID = "IDX_SUBCAT_ID";

    public static final String IDX_SUBCATEGORY = "IDX_SUBCATEGORY";

    public static final String IDX_CATEGORY = "IDX_CATEGORY";

    public static final String IDX_CAT_ID = "IDX_CAT_ID";

    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        sql("CREATE TABLE products (id INT PRIMARY KEY, category VARCHAR, cat_id INT NOT NULL,"
                + " subcategory VARCHAR, subcat_id INT NOT NULL, name VARCHAR)");

        sql("CREATE INDEX " + IDX_CATEGORY + " ON products (category)");
        sql("CREATE INDEX " + IDX_CAT_ID + " ON products (cat_id)");
        sql("CREATE INDEX " + IDX_SUBCATEGORY + " ON products (subcategory)");
        sql("CREATE INDEX " + IDX_SUBCAT_ID + " ON products (subcat_id)");

        insertData("products", List.of("ID", "CATEGORY", "CAT_ID", "SUBCATEGORY", "SUBCAT_ID", "NAME"), new Object[][]{
                {1, "Photo", 1, "Camera Media", 11, "Media 1"},
                {2, "Photo", 1, "Camera Media", 11, "Media 2"},
                {3, "Photo", 1, "Camera Lens", 12, "Lens 1"},
                {4, "Photo", 1, "Other", 12, "Charger 1"},
                {5, "Video", 2, "Camera Media", 21, "Media 3"},
                {6, "Video", 2, "Camera Lens", 22, "Lens 3"},
                {7, "Video", 1, null, 0, "Canon"},
                {8, null, 0, "Camera Lens", 11, "Zeiss"},
                {9, null, 0, null, 0, null},
                {10, null, 0, null, 30, null},
                {11, null, 0, null, 30, null},
                {12, null, 0, null, 31, null},
                {13, null, 0, null, 31, null},
                {14, null, 0, null, 32, null},
                {15, null, 0, null, 33, null},
                {16, null, 0, null, 34, null},
                {17, null, 0, null, 35, null},
                {18, null, 0, null, 36, null},
                {19, null, 0, null, 37, null},
                {20, null, 0, null, 38, null},
                {21, null, 0, null, 39, null},
                {22, null, 0, null, 40, null},
                {23, null, 0, null, 41, null},
        });
    }

    /**
     * Check 'OR -> UNION' rule is applied for equality conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEqualityOrToUnionAllRewrite() {
        assertQuery("SELECT * "
                + "FROM products "
                + "WHERE category = 'Video' "
                + "OR subcategory ='Camera Lens'")
                .matches(containsUnion(true))
                .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"))
                .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"))
                .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
                .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
                .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
                .returns(7, "Video", 1, null, 0, "Canon")
                .returns(8, null, 0, "Camera Lens", 11, "Zeiss")
                .check();
    }

    /**
     * Check 'OR -> UNION' rule is applied for mixed conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedOrToUnionAllRewrite() {
        assertQuery("SELECT * "
                + "FROM products "
                + "WHERE category = 'Photo' "
                + "OR (subcat_id > 12 AND subcat_id < 22)")
                .matches(containsUnion(true))
                .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"))
                .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCAT_ID"))
                .returns(1, "Photo", 1, "Camera Media", 11, "Media 1")
                .returns(2, "Photo", 1, "Camera Media", 11, "Media 2")
                .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
                .returns(4, "Photo", 1, "Other", 12, "Charger 1")
                .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
                .check();
    }

    /*--- "Not contains union" section. ---*/

    /**
     * Check 'OR -> UNION' rule is NOT applied for equality conditions on the same indexed column.
     */
    @Test
    public void testNonDistinctOrToUnionAllRewrite() {
        assertQuery("SELECT * "
                + "FROM products "
                + "WHERE subcategory = 'Camera Lens' "
                + "OR subcategory = 'Other'")
                .matches(not(containsUnion(true)))
                .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"))
                .matches(containsString("searchBounds=[[MultiBounds"))
                .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
                .returns(4, "Photo", 1, "Other", 12, "Charger 1")
                .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
                .returns(8, null, 0, "Camera Lens", 11, "Zeiss")
                .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied for range conditions on indexed columns.
     */
    @Test
    public void testRangeOrToUnionAllRewrite() {
        assertQuery("SELECT * "
                + "FROM products "
                + "WHERE cat_id > 1 "
                + "OR subcat_id < 10")
                .matches(not(containsUnion(true)))
                .matches(containsTableScan("PUBLIC", "PRODUCTS"))
                .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
                .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
                .returns(7, "Video", 1, null, 0, "Canon")
                .returns(9, null, 0, null, 0, null)
                .check();
    }

    /**
     * Check 'OR -> UNION' rule is NOT applied if no acceptable index was found.
     */
    @Test
    public void testUnionRuleNotApplicable() {
        assertQuery("SELECT * FROM products WHERE name = 'Canon' OR subcat_id = 22")
                .matches(not(containsUnion(true)))
                .matches(containsTableScan("PUBLIC", "PRODUCTS"))
                .returns(7, "Video", 1, null, 0, "Canon")
                .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
                .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied if (at least) one of column is not indexed.
     */
    @Test
    public void testNonIndexedOrToUnionAllRewrite() {
        assertQuery("SELECT * "
                + "FROM products "
                + "WHERE name = 'Canon' "
                + "OR category = 'Video'")
                .matches(not(containsUnion(true)))
                .matches(containsTableScan("PUBLIC", "PRODUCTS"))
                .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
                .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
                .returns(7, "Video", 1, null, 0, "Canon")
                .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied if all columns are not indexed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAllNonIndexedOrToUnionAllRewrite() {
        assertQuery("SELECT * "
                + "FROM products "
                + "WHERE name = 'Canon' "
                + "OR name = 'Sony'")
                .matches(not(containsUnion(true)))
                .matches(containsTableScan("PUBLIC", "PRODUCTS"))
                .returns(7, "Video", 1, null, 0, "Canon")
                .check();
    }
}
