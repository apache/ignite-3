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

package org.apache.ignite.internal.schemasync;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.table.QualifiedName.DEFAULT_SCHEMA_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ItSchemaSyncMetastorageDependencyTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private IgniteImpl ignite;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void prepare() {
        ignite = igniteImpl(0);

        ignite.sql().executeScript("CREATE TABLE "
                + TABLE_NAME
                + " (ID INT PRIMARY KEY, VAL VARCHAR)");

        // Do a put to make sure that replicas are started before we start blocking the metastorage progress.
        assertThat(doPutAsync(ignite), willCompleteSuccessfully());
    }

    @Test
    void longWatchHandlingForNonCatalogEntryDoesNotAffectSchemaSync() throws Exception {
        ByteArray entryKey = new ByteArray(ItSchemaSyncMetastorageDependencyTest.class.getName());
        ignite.metaStorageManager().registerExactWatch(entryKey, event -> neverCompletingFuture());

        assertThat(ignite.metaStorageManager().put(entryKey, new byte[0]), willCompleteSuccessfully());

        waitOutDelayDuration();

        assertThat(doPutAsync(ignite), willCompleteSuccessfully());
    }

    private static <T> CompletableFuture<T> neverCompletingFuture() {
        return new CompletableFuture<>();
    }

    private static void waitOutDelayDuration() throws InterruptedException {
        Thread.sleep(TestIgnitionManager.DEFAULT_DELAY_DURATION_MS * 2);
    }

    private static CompletableFuture<Void> doPutAsync(IgniteImpl ignite) {
        return ignite.tables().tableAsync(TABLE_NAME)
                .thenCompose(table -> {
                    KeyValueView<Integer, String> view = table.keyValueView(Integer.class, String.class);
                    return view.putAsync(null, 1, "one");
                });
    }

    @Test
    void longWatchHandlingForCatalogEntryAffectsSchemaSync() throws Exception {
        CatalogManager catalogManager = ignite.catalogManager();
        catalogManager.listen(CatalogEvent.TABLE_CREATE, parameters -> neverCompletingFuture());

        triggerAnotherTableCreation(catalogManager);

        waitOutDelayDuration();

        assertThat(doPutAsync(ignite), willTimeoutIn(1, SECONDS));
    }

    private void triggerAnotherTableCreation(CatalogManager catalogManager) throws InterruptedException {
        String anotherTableName = "ANOTHER_TABLE";
        ignite.sql().executeScriptAsync("CREATE TABLE " + anotherTableName + " (ID INT PRIMARY KEY, VAL VARCHAR)");
        assertTrue(waitForCondition(() -> hasTableInCatalog(anotherTableName, catalogManager), SECONDS.toMillis(10)));
    }

    private static boolean hasTableInCatalog(String anotherTableName, CatalogManager catalogManager) {
        return catalogManager.latestCatalog().table(DEFAULT_SCHEMA_NAME, anotherTableName) != null;
    }
}
