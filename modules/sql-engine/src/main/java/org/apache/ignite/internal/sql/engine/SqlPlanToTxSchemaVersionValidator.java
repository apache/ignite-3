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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.sql.engine.exec.SqlPlanOutdatedException;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.jetbrains.annotations.TestOnly;

/**
 * Execution plan validator.
 *
 * <p>Performs validation of the catalog version from the {@link MultiStepPlan multi-step plan} relative to the started transaction.
 */
public class SqlPlanToTxSchemaVersionValidator {
    @TestOnly
    public static final SqlPlanToTxSchemaVersionValidator NOOP = new NoopSqlPlanToTxSchemaVersionValidator();

    private final SchemaSyncService schemaSyncService;
    private final CatalogService catalogService;

    /**
     * Compares the catalog version from the plan with the version of the active catalog
     * at the time of the transaction {@link InternalTransaction#schemaTimestamp() schema time}.
     *
     * @param plan {@link MultiStepPlan multi-step} execution plan.
     * @param tx Query transaction wrapper.
     * @return Successfully completed future if the provided transaction is explicit or the catalog versions match.
     *         Otherwise returns a future completed with an exception {@link SqlPlanOutdatedException},
     */
    public CompletableFuture<Void> validate(MultiStepPlan plan, QueryTransactionWrapper tx) {
        if (!tx.implicit()) {
            return nullCompletedFuture();
        }

        HybridTimestamp ts = tx.unwrap().schemaTimestamp();

        return schemaSyncService.waitForMetadataCompleteness(ts)
                .thenRun(() -> {
                    int requiredCatalog = catalogService.activeCatalogVersion(ts.longValue());

                    if (requiredCatalog != plan.catalogVersion()) {
                        throw new SqlPlanOutdatedException();
                    }
                });
    }

    private SqlPlanToTxSchemaVersionValidator(SchemaSyncService schemaSyncService, CatalogService catalogService) {
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
    }

    public static SqlPlanToTxSchemaVersionValidator create(SchemaSyncService schemaSyncService, CatalogService catalogService) {
        return new SqlPlanToTxSchemaVersionValidator(schemaSyncService, catalogService);
    }

    private static class NoopSqlPlanToTxSchemaVersionValidator extends SqlPlanToTxSchemaVersionValidator {
        @SuppressWarnings("DataFlowIssue")
        private NoopSqlPlanToTxSchemaVersionValidator() {
            super(null, null);
        }

        @Override
        public CompletableFuture<Void> validate(MultiStepPlan plan, QueryTransactionWrapper tx) {
            return nullCompletedFuture();
        }
    }
}
