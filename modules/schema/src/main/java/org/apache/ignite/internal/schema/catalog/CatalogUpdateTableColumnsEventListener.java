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

import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.TableEventParameters;
import org.apache.ignite.internal.manager.EventListener;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for table column changes, as well as table creation events. Designed to listen for events {@link CatalogEvent#TABLE_CREATE} and
 * {@link CatalogEvent#TABLE_ALTER}.
 *
 * <p>Enough of all events will override {@link #onTableColumnsUpdate(long, int, CatalogTableDescriptor)}, which will be called when the
 * following methods are called:</p>
 * <ul>
 *     <li>{@link CatalogManager#createTable(CreateTableParams)};</li>
 *     <li>{@link CatalogManager#addColumn(AlterTableAddColumnParams)};</li>
 *     <li>{@link CatalogManager#alterColumn(AlterColumnParams)};</li>
 *     <li>{@link CatalogManager#dropColumn(AlterTableDropColumnParams)};</li>
 * </ul>
 */
public abstract class CatalogUpdateTableColumnsEventListener implements EventListener<TableEventParameters> {
    private final CatalogService catalogService;

    /**
     * Constructor.
     *
     * @param catalogService Catalog service.
     */
    protected CatalogUpdateTableColumnsEventListener(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @Override
    public CompletableFuture<Boolean> notify(TableEventParameters parameters, @Nullable Throwable exception) {
        if (exception != null) {
            return failedFuture(exception);
        }

        try {
            assert !(parameters instanceof DropTableEventParameters) : "Drop table event is not supported";

            int catalogVersion = parameters.catalogVersion();

            CatalogTableDescriptor table = catalogService.table(parameters.tableId(), catalogVersion);

            assert table != null : parameters.tableId();

            return onTableColumnsUpdate(parameters.causalityToken(), catalogVersion, table).thenApply(unused -> false);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    /**
     * Callback on the table column change event.
     *
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version.
     * @param table Catalog table descriptor.
     */
    protected abstract CompletableFuture<Void> onTableColumnsUpdate(long causalityToken, int catalogVersion, CatalogTableDescriptor table);
}
