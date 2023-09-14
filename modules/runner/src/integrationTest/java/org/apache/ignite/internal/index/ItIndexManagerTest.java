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

package org.apache.ignite.internal.index;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests to verify integration of {@link IndexManager} with other components.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItIndexManagerTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int nodes() {
        return 1;
    }

    @Test
    public void eventsAreFiredWhenIndexesCreatedAndDropped() {
        IgniteImpl ignite = (IgniteImpl) CLUSTER_NODES.get(0);
        IndexManager indexManager = ignite.indexManager();

        CompletableFuture<IndexEventParameters> pkCreatedFuture = registerListener(indexManager, IndexEvent.CREATE);

        String tableName = "TNAME";

        int catalogVersion = ignite.catalogManager().latestCatalogVersion();

        sql(String.format("CREATE TABLE %s (c1 INT PRIMARY KEY, c2 INT, c3 INT)", tableName));

        TableImpl table = (TableImpl) ignite.tables().table(tableName);

        {
            assertThat(pkCreatedFuture, willCompleteSuccessfully());

            IndexEventParameters parameters = pkCreatedFuture.join();

            assertThat(parameters, notNullValue());
            assertThat(parameters.tableId(), equalTo(table.tableId()));
            assertThat(parameters.catalogVersion(), greaterThan(catalogVersion));
        }

        CompletableFuture<IndexEventParameters> indexCreatedFuture = registerListener(indexManager, IndexEvent.CREATE);

        String indexName = "INAME";

        sql(String.format("CREATE INDEX %s ON %s (c3, c2)", indexName, tableName));

        int createdIndexId;
        {
            assertThat(indexCreatedFuture, willCompleteSuccessfully());

            IndexEventParameters parameters = indexCreatedFuture.join();

            assertThat(parameters, notNullValue());
            assertThat(parameters.tableId(), equalTo(table.tableId()));
            assertThat(parameters.catalogVersion(), greaterThan(catalogVersion));

            createdIndexId = parameters.indexId();
        }

        CompletableFuture<IndexEventParameters> indexDroppedFuture = registerListener(indexManager, IndexEvent.DROP);

        sql(String.format("DROP INDEX %s", indexName));

        {
            assertThat(indexDroppedFuture, willCompleteSuccessfully());

            IndexEventParameters params = indexDroppedFuture.join();

            assertThat(params, notNullValue());
            assertThat(params.indexId(), equalTo(createdIndexId));
        }
    }

    private static CompletableFuture<IndexEventParameters> registerListener(IndexManager indexManager, IndexEvent event) {
        CompletableFuture<IndexEventParameters> paramFuture = new CompletableFuture<>();

        indexManager.listen(event, (param, th) -> {
            paramFuture.complete(param);

            return CompletableFuture.completedFuture(true);
        });

        return paramFuture;
    }
}
