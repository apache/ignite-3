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

import static java.lang.Thread.currentThread;
import static org.apache.ignite.internal.PublicApiThreadingTests.anIgniteThread;
import static org.apache.ignite.internal.PublicApiThreadingTests.asyncContinuationPool;
import static org.apache.ignite.internal.PublicApiThreadingTests.tryToSwitchFromUserThreadWithDelayedSchemaSync;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.catalog.ItCatalogDslTest.POJO_RECORD_TABLE_NAME;
import static org.apache.ignite.internal.catalog.ItCatalogDslTest.ZONE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.catalog.sql.IgniteCatalogSqlImpl;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ItCatalogApiThreadingTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    void clearDatabase() {
        dropAllTables();
        dropAllZonesExceptDefaultOne();
    }

    @ParameterizedTest
    @EnumSource(CatalogAsyncOperation.class)
    void catalogFuturesCompleteInContinuationsPool(CatalogAsyncOperation operation) {
        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(catalogForPublicUse())
                        .thenApply(unused -> currentThread())
        );

        assertThat(completerFuture, willBe(asyncContinuationPool()));
    }

    private static <T> T forcingSwitchFromUserThread(Supplier<? extends T> action) {
        return tryToSwitchFromUserThreadWithDelayedSchemaSync(CLUSTER.aliveNode(), action);
    }

    private static IgniteCatalog catalogForPublicUse() {
        return CLUSTER.aliveNode().catalog();
    }

    @ParameterizedTest
    @EnumSource(CatalogAsyncOperation.class)
    @Disabled("IGNITE-22687 or IGNITE-24204")
    // TODO: enable this after IGNITE-22687 is fixed or after IGNITE-24204 (which will give possibility to distinguish the common FJP from
    // the user-supplied async continuation executor).
    void catalogFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(CatalogAsyncOperation operation) {
        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(catalogForInternalUse())
                        .thenApply(unused -> currentThread())
        );

        assertThat(completerFuture, willBe(anIgniteThread()));
    }

    private static IgniteCatalog catalogForInternalUse() {
        return Wrappers.unwrap(CLUSTER.aliveNode().catalog(), IgniteCatalogSqlImpl.class);
    }

    @ParameterizedTest
    @EnumSource(CatalogOperationProvidingTable.class)
    void tableFuturesFromCatalogAreProtectedFromThreadHijacking(CatalogOperationProvidingTable operation) throws Exception {
        Table table = operation.executeOn(catalogForPublicUse());

        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> table.keyValueView().queryAsync(null, null)
                        .thenApply(unused -> currentThread())
        );

        assertThat(completerFuture, willBe(asyncContinuationPool()));
    }

    private static ZoneDefinition testZoneDefinition() {
        return ZoneDefinition.builder(ZONE_NAME)
                .distributionAlgorithm("rendezvous")
                .filter(DEFAULT_FILTER)
                .storageProfiles(DEFAULT_AIPERSIST_PROFILE_NAME)
                .build();
    }

    private static TableDefinition testTableDefinition() {
        return TableDefinition.builder(POJO_RECORD_TABLE_NAME)
                .record(Pojo.class)
                .build();
    }

    private enum CatalogAsyncOperation {
        CREATE_TABLE_FOR_RECORD(catalog -> catalog.createTableAsync(Pojo.class)),
        CREATE_TABLE_FOR_KEY_AND_VALUE(catalog -> catalog.createTableAsync(PojoKey.class, PojoValue.class)),
        CREATE_TABLE_BY_DEFINITION(catalog -> catalog.createTableAsync(testTableDefinition())),
        CREATE_ZONE(catalog -> catalog.createZoneAsync(testZoneDefinition())),
        DROP_TABLE_BY_DEFINITION(catalog -> catalog.dropTableAsync(testTableDefinition())),
        DROP_TABLE_BY_NAME(catalog -> catalog.dropTableAsync(testTableDefinition().tableName())),
        DROP_ZONE_BY_DEFINITION(catalog -> catalog.dropZoneAsync(testZoneDefinition())),
        DROP_ZONE_BY_NAME(catalog -> catalog.dropZoneAsync(testZoneDefinition().zoneName()));

        private final Function<IgniteCatalog, CompletableFuture<?>> action;

        CatalogAsyncOperation(Function<IgniteCatalog, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(IgniteCatalog catalog) {
            return action.apply(catalog);
        }
    }

    private enum CatalogOperationProvidingTable {
        CREATE_TABLE_FOR_RECORD(catalog -> catalog.createTable(Pojo.class)),
        CREATE_TABLE_FOR_KEY_AND_VALUE(catalog -> catalog.createTable(PojoKey.class, PojoValue.class)),
        CREATE_TABLE_BY_DEFINITION(catalog -> catalog.createTable(testTableDefinition())),
        CREATE_TABLE_FOR_RECORD_ASYNC(catalog -> catalog.createTableAsync(Pojo.class).get(10, TimeUnit.SECONDS)),
        CREATE_TABLE_FOR_KEY_AND_VALUE_ASYNC(catalog -> catalog.createTableAsync(PojoKey.class, PojoValue.class).get(10, TimeUnit.SECONDS)),
        CREATE_TABLE_BY_DEFINITION_ASYNC(catalog -> catalog.createTableAsync(testTableDefinition()).get(10, TimeUnit.SECONDS));

        private final ThrowingFunction<IgniteCatalog, Table> action;

        CatalogOperationProvidingTable(ThrowingFunction<IgniteCatalog, Table> action) {
            this.action = action;
        }

        Table executeOn(IgniteCatalog catalog) throws Exception {
            return action.apply(catalog);
        }
    }

    /**
     * Function that can throw an {@link Exception}.
     */
    @FunctionalInterface
    private interface ThrowingFunction<T, R> {
        /**
         * Applies the function the a given argument.
         *
         * @param t Argument.
         * @return Application result.
         * @throws Exception If something goes wrong.
         */
        R apply(T t) throws Exception;
    }
}
