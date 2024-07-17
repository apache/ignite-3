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
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;

import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests making sure an index is available for SQL use as soon as its creation future completes.
 */
class ItIndexAvailabilityTest extends BaseSqlIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String INDEX_NAME = "TEST_INDEX";

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(TestIgnitionManager.PRODUCTION_CLUSTER_CONFIG_STRING);
    }

    @BeforeEach
    void setUp() {
        sql(String.format("CREATE TABLE IF NOT EXISTS %s (key BIGINT PRIMARY KEY, val VARCHAR)", TABLE_NAME));
    }

    @AfterEach
    void tearDown() {
        sql(String.format("DROP TABLE IF EXISTS %S", TABLE_NAME));
    }

    @Test
    void indexIsQueriableRightAfterCreationFutureCompletes() {
        sql(String.format("CREATE INDEX %s ON %s (val)", INDEX_NAME, TABLE_NAME));

        assertQuery(format("SELECT * FROM {} WHERE val = 'test'", TABLE_NAME))
                .matches(containsIndexScan(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, INDEX_NAME))
                .check();
    }
}
