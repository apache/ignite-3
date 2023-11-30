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

package org.apache.ignite.internal.table.distributed.schema;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.InternalTransaction;

/**
 * Implements the logic defined by IEP-110. This means readTimestamp for an RO transaction
 * and MAX(beginTs, tableCreationTs) for an RW transaction.
 */
public class TransactionTimestampsImpl implements TransactionTimestamps {
    private final SchemaSyncService schemaSyncService;
    private final CatalogService catalogService;
    private final HybridClock clock;

    /**
     * Constructor.
     */
    public TransactionTimestampsImpl(SchemaSyncService schemaSyncService, CatalogService catalogService, HybridClock clock) {
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
        this.clock = clock;
    }

    @Override
    public CompletableFuture<HybridTimestamp> baseTimestamp(InternalTransaction tx, int tableId) {
        if (TransactionTimestamps.transactionSeesFutureTables(tx)) {
            return rwTransactionBaseTimestamp(tx.startTimestamp(), tableId);
        } else {
            return completedFuture(tx.startTimestamp());
        }
    }

    @Override
    public CompletableFuture<HybridTimestamp> rwTransactionBaseTimestamp(HybridTimestamp txBeginTimestamp, int tableId) {
        return tableWithSchemaSync(tableId, txBeginTimestamp)
                .thenCompose(tableByBeginTs -> {
                    if (tableByBeginTs != null) {
                        return completedFuture(txBeginTimestamp);
                    }

                    HybridTimestamp now = clock.now();

                    return tableWithSchemaSync(tableId, now)
                            .thenApply(tableAtNow -> {
                                if (tableAtNow == null) {
                                    return txBeginTimestamp;
                                }

                                if (tableAtNow.tableVersion() == CatalogTableDescriptor.INITIAL_TABLE_VERSION) {
                                    int nowCatalogVersion = catalogService.activeCatalogVersion(now.longValue());
                                    return catalogVersionTimestamp(nowCatalogVersion);
                                }

                                int fromCatalogVersion = catalogService.activeCatalogVersion(txBeginTimestamp.longValue());
                                int toCatalogVersion = catalogService.activeCatalogVersion(now.longValue());
                                for (int catalogVersion = fromCatalogVersion + 1; catalogVersion <= toCatalogVersion; catalogVersion++) {
                                    CatalogTableDescriptor table = catalogService.table(tableId, catalogVersion);

                                    if (table != null) {
                                        assert table.tableVersion() == CatalogTableDescriptor.INITIAL_TABLE_VERSION : "For table " + tableId
                                                + " first encountered version is " + table.tableVersion();

                                        return catalogVersionTimestamp(catalogVersion);
                                    }
                                }

                                throw new IllegalStateException("Did not find table " + tableId
                                        + " while scanning the Catalog, even though it's there at ts " + now);
                            });
                });
    }

    private CompletableFuture<CatalogTableDescriptor> tableWithSchemaSync(int tableId, HybridTimestamp timestamp) {
        return schemaSyncService.waitForMetadataCompleteness(timestamp)
                .thenApply(unused -> catalogService.table(tableId, timestamp.longValue()));
    }

    private HybridTimestamp catalogVersionTimestamp(int catalogVersion) {
        long time = catalogService.catalog(catalogVersion).time();

        return HybridTimestamp.hybridTimestamp(time);
    }
}
