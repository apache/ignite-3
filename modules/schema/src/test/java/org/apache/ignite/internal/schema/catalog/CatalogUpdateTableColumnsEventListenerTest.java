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

package org.apache.ignite.internal.schema.catalog;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor.INITIAL_TABLE_VERSION;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_ALTER;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_CREATE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.BaseCatalogManagerTest;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.junit.jupiter.api.Test;

/** For {@link CatalogUpdateTableColumnsEventListener} testing. */
public class CatalogUpdateTableColumnsEventListenerTest extends BaseCatalogManagerTest {
    @Test
    void testListenUpdateColumnsOnCreateTable() {
        CompletableFuture<Void> listenerInvokeFuture = new CompletableFuture<>();

        manager.listen(TABLE_CREATE, createListener(listenerInvokeFuture, INITIAL_TABLE_VERSION));

        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(listenerInvokeFuture, willCompleteSuccessfully());
    }

    @Test
    void testListenUpdateColumnsOnAddColumn() {
        CompletableFuture<Void> listenerInvokeFuture = new CompletableFuture<>();

        manager.listen(TABLE_ALTER, createListener(listenerInvokeFuture, INITIAL_TABLE_VERSION + 1));

        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.addColumn(addColumnParams(columnParams("test_name", INT32))), willCompleteSuccessfully());

        assertThat(listenerInvokeFuture, willCompleteSuccessfully());
    }

    @Test
    void testListenUpdateColumnsOnAlterColumn() {
        CompletableFuture<Void> listenerInvokeFuture = new CompletableFuture<>();

        manager.listen(TABLE_ALTER, createListener(listenerInvokeFuture, INITIAL_TABLE_VERSION + 1));

        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.alterColumn(
                        AlterColumnParams.builder()
                                .schemaName(DEFAULT_SCHEMA_NAME)
                                .tableName(TABLE_NAME)
                                .columnName("VAL")
                                .type(INT64)
                                .build()
                ),
                willCompleteSuccessfully()
        );

        assertThat(listenerInvokeFuture, willCompleteSuccessfully());
    }

    @Test
    void testListenUpdateColumnsOnDropColumn() {
        CompletableFuture<Void> listenerInvokeFuture = new CompletableFuture<>();

        manager.listen(TABLE_ALTER, createListener(listenerInvokeFuture, INITIAL_TABLE_VERSION + 1));

        assertThat(manager.createTable(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(manager.dropColumn(dropColumnParams("VAL")), willCompleteSuccessfully());

        assertThat(listenerInvokeFuture, willCompleteSuccessfully());
    }

    private CatalogUpdateTableColumnsEventListener createListener(CompletableFuture<Void> listenerInvokeFuture, int expTableVersion) {
        int catalogVersionBeforeInvokeListener = manager.latestCatalogVersion();

        return new CatalogUpdateTableColumnsEventListener(manager) {
            @Override
            protected CompletableFuture<Void> onTableColumnsUpdate(long causalityToken, int catalogVersion, CatalogTableDescriptor table) {
                try {
                    assertThat(causalityToken, greaterThan(0L));
                    assertThat(catalogVersion, greaterThan(catalogVersionBeforeInvokeListener));
                    assertThat(table.tableVersion(), equalTo(expTableVersion));

                    listenerInvokeFuture.complete(null);

                    return completedFuture(null);
                } catch (Throwable t) {
                    listenerInvokeFuture.completeExceptionally(t);

                    return failedFuture(t);
                }
            }
        };
    }
}
