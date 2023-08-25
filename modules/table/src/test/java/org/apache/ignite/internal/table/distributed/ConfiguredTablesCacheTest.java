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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class ConfiguredTablesCacheTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private TablesConfiguration tablesConfig;

    private ConfiguredTablesCache cache;

    @BeforeEach
    void createCache() {
        cache = new ConfiguredTablesCache(tablesConfig, false);
    }

    @Test
    void tableIdCheckIsCoherentWithConfigGenerationUpdate() {
        createTable(1, "t1");

        assertTrue(cache.isTableConfigured(1));
        assertFalse(cache.isTableConfigured(2));

        createTable(2, "t2");

        assertTrue(cache.isTableConfigured(2));
    }

    private void createTable(int tableId, String tableName) {
        CompletableFuture<Void> future = tablesConfig.change(tablesChange -> {
            tablesChange.changeTables(ch -> ch.create(tableName, tableChange -> {
                tableChange.changeId(tableId);
            }));

            tablesChange.changeTablesGeneration(tablesChange.tablesGeneration() + 1);
        });

        assertThat(future, willCompleteSuccessfully());
    }

    @Test
    void tableIdsIsCoherentWithConfigGenerationUpdate() {
        assertThat(cache.configuredTableIds(), is(empty()));

        createTable(1, "t1");

        assertThat(cache.configuredTableIds(), contains(1));

        createTable(2, "t2");

        assertThat(cache.configuredTableIds(), contains(1, 2));
    }
}
