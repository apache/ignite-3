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

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type.HASH;
import static org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type.SORTED;

import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/** End-to-end tests to verify indexes system view. */
public class ItIndexesSystemViewTest extends BaseSqlIntegrationTest {
    private static final int TABLE_ID = 7;

    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String COLUMN_NAME = "ID";

    private static final int SCHEMA_ID = 0;

    private static final String SCHEMA_NAME = "PUBLIC";

    private static final int LAST_INDEX_ID = 7;

    @Override
    @BeforeAll
    protected void beforeAll(TestInfo testInfo) {
        super.beforeAll(testInfo);

        IgniteTestUtils.await(systemViewManager().completeRegistration());
    }

    @Test
    public void multipleIndexes() {
        sql(String.format("CREATE TABLE %S (%s INT PRIMARY KEY);", TABLE_NAME, COLUMN_NAME));

        String hashIndexName = "TEST_INDEX_HASH";
        String sortedIndexName = "TEST_INDEX_SORTED";
        String primaryKeyIndexName = TABLE_NAME + "_PK";

        sql(createIndexSql("TEST_INDEX_HASH", TABLE_NAME, HASH.name(), COLUMN_NAME));
        sql(createIndexSql("TEST_INDEX_SORTED", TABLE_NAME, "TREE", COLUMN_NAME + " DESC"));

        assertQuery("SELECT * FROM SYSTEM.INDEXES").returnRowCount(3).check();

        assertQuery(selectFromIndexesSystemView(primaryKeyIndexName)).returns(
                LAST_INDEX_ID + 1,
                primaryKeyIndexName,
                HASH.name(),
                TABLE_ID,
                TABLE_NAME,
                SCHEMA_ID,
                SCHEMA_NAME,
                true,
                COLUMN_NAME,
                AVAILABLE.name()
        ).check();

        assertQuery(selectFromIndexesSystemView("TEST_INDEX_HASH")).returns(
                LAST_INDEX_ID + 2,
                hashIndexName,
                HASH.name(),
                TABLE_ID,
                TABLE_NAME,
                SCHEMA_ID,
                SCHEMA_NAME,
                false,
                COLUMN_NAME,
                AVAILABLE.name()
        ).check();

        assertQuery(selectFromIndexesSystemView("TEST_INDEX_SORTED")).returns(
                LAST_INDEX_ID + 3,
                sortedIndexName,
                SORTED.name(),
                TABLE_ID,
                TABLE_NAME,
                SCHEMA_ID,
                SCHEMA_NAME,
                false,
                COLUMN_NAME + " " + Collation.DESC_NULLS_FIRST,
                AVAILABLE.name()
        ).check();
    }

    private static String createIndexSql(String name, String tableName, String type, String columnString) {
        return String.format("CREATE INDEX %s ON %s USING %S (%s);", name, tableName, type, columnString);
    }

    private static String selectFromIndexesSystemView(String indexName) {
        String sqlFormat = "SELECT INDEX_ID, INDEX_NAME, TYPE, TABLE_ID, TABLE_NAME, SCHEMA_ID, SCHEMA_NAME, \"UNIQUE\", COLUMNS, STATUS "
                + " FROM SYSTEM.INDEXES WHERE INDEX_NAME = '%s'";

        return String.format(sqlFormat, indexName);
    }
}
