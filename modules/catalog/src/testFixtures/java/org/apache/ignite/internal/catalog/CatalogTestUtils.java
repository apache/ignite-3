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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnCommand;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ColumnType.PrecisionScale;

/**
 * Utilities for working with the catalog in tests.
 */
public class CatalogTestUtils {
    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, HybridClock clock) {
        var vault = new VaultManager(new InMemoryVaultService());

        StandaloneMetaStorageManager metastore = StandaloneMetaStorageManager.create(vault, new SimpleInMemoryKeyValueStorage(nodeName));

        var clockWaiter = new ClockWaiter(nodeName, clock);

        return new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter) {
            @Override
            public void start() {
                vault.start();
                metastore.start();
                clockWaiter.start();

                super.start();

                assertThat(metastore.deployWatches(), willCompleteSuccessfully());
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
                metastore.beforeNodeStop();
                vault.beforeNodeStop();
            }

            @Override
            public void stop() throws Exception {
                super.stop();

                clockWaiter.stop();
                metastore.stop();
                vault.stop();
            }
        };
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     * @param metastore Meta storage manager.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, HybridClock clock, MetaStorageManager metastore) {
        var clockWaiter = new ClockWaiter(nodeName, clock);

        return new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter) {
            @Override
            public void start() {
                clockWaiter.start();

                super.start();
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
            }

            @Override
            public void stop() throws Exception {
                super.stop();

                clockWaiter.stop();
            }
        };
    }

    /** Default nullable behavior. */
    public static final boolean DEFAULT_NULLABLE = false;

    /** Append precision\scale according to type requirement. */
    public static Builder initializeColumnWithDefaults(ColumnType type, Builder colBuilder) {
        if (type.precScale() != PrecisionScale.NO_NO) {
            colBuilder.precision(11);
        }

        if (type.precScale() == PrecisionScale.YES_YES) {
            colBuilder.scale(0);
        }

        if (type.specifiedLength()) {
            colBuilder.length(1 << 5);
        }

        return colBuilder;
    }

    /** Append scale according to type requirement. */
    public static void applyNecessaryPrecision(ColumnType type, Builder colBuilder) {
        if (type.precScale() != PrecisionScale.NO_NO) {
            colBuilder.precision(11);
        }
    }

    /** Append scale according to type requirement. */
    public static void applyNecessaryLength(ColumnType type, Builder colBuilder) {
        if (type.specifiedLength()) {
            colBuilder.length(1 << 5);
        }
    }

    static ColumnParams columnParams(String name, ColumnType type) {
        return columnParams(name, type, DEFAULT_NULLABLE);
    }

    static ColumnParams columnParams(String name, ColumnType type, boolean nullable) {
        return columnParamsBuilder(name, type, nullable).build();
    }

    static ColumnParams columnParams(String name, ColumnType type, boolean nullable, int precision) {
        return columnParamsBuilder(name, type, nullable, precision).build();
    }

    static ColumnParams columnParams(String name, ColumnType type, boolean nullable, int precision, int scale) {
        return columnParamsBuilder(name, type, nullable, precision, scale).build();
    }

    static ColumnParams columnParams(String name, ColumnType type, int length, boolean nullable) {
        return columnParamsBuilder(name, type, length, nullable).build();
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type) {
        return columnParamsBuilder(name, type, DEFAULT_NULLABLE);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, boolean nullable) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, boolean nullable, int precision) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type).precision(precision);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, boolean nullable, int precision, int scale) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type).precision(precision).scale(scale);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, int length, boolean nullable) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type).length(length);
    }

    static CatalogCommand dropTableCommand(String tableName) {
        return DropTableCommand.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(tableName).build();
    }

    static CatalogCommand dropColumnParams(String tableName, String... columns) {
        return AlterTableDropColumnCommand.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(tableName).columns(Set.of(columns)).build();
    }

    static CatalogCommand addColumnParams(String tableName, ColumnParams... columns) {
        return AlterTableAddColumnCommand.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(tableName).columns(List.of(columns)).build();
    }
}
