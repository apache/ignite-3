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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;

import org.apache.ignite.internal.catalog.IndexExistsValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for DDL statements that affect indexes.
 */
public class ItIndexDdlTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String INDEX_NAME = "TEST_INDEX";

    @BeforeEach
    void setUp() {
        sql(String.format("CREATE TABLE IF NOT EXISTS %s (key BIGINT PRIMARY KEY, valInt INT, valStr VARCHAR)", TABLE_NAME));
    }

    @AfterEach
    void tearDown() {
        sql(String.format("DROP TABLE IF EXISTS %S", TABLE_NAME));
    }

    @Test
    public void testAddIndex() {
        tryToCreateIndex(TABLE_NAME, INDEX_NAME, true);

        assertThrowsWithCause(
                () -> tryToCreateIndex(TABLE_NAME, INDEX_NAME, true),
                IndexExistsValidationException.class,
                String.format("Index with name '%s.%s' already exists", DEFAULT_SCHEMA_NAME, INDEX_NAME)
        );

        tryToCreateIndex(TABLE_NAME, INDEX_NAME, false);
    }

    @Test
    void testDropIndex() {
        tryToCreateIndex(TABLE_NAME, INDEX_NAME, true);

        // Let's check the drop on an existing index.
        tryToDropIndex(INDEX_NAME, true);

        // Let's check the drop on a non-existent index.
        assertThrowsWithCause(
                () -> tryToDropIndex(INDEX_NAME, true),
                IndexNotFoundValidationException.class,
                String.format("Index with name '%s.%s' not found", DEFAULT_SCHEMA_NAME, INDEX_NAME)
        );

        tryToCreateIndex(TABLE_NAME, INDEX_NAME, false);
    }

    /**
     * Tries to create the index.
     *
     * @param tableName Table name.
     * @param indexName Index name.
     * @param failIfExist Throw an exception if the index exist.
     */
    private static void tryToCreateIndex(String tableName, String indexName, boolean failIfExist) {
        sql(String.format(
                "CREATE INDEX %s ON %s (valInt, valStr)",
                failIfExist ? indexName : "IF NOT EXISTS " + indexName, tableName
        ));
    }

    /**
     * Tries to destroy the index.
     *
     * @param indexName Index name.
     * @param failIfNotExist Throw an exception if the index does not exist.
     */
    private static void tryToDropIndex(String indexName, boolean failIfNotExist) {
        sql(String.format("DROP INDEX %s", failIfNotExist ? indexName : "IF EXISTS " + indexName));
    }
}
