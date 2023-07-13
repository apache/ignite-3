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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
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
    /** {@inheritDoc} */
    @Override
    protected int nodes() {
        return 1;
    }

    @Test
    public void eventsAreFiredWhenIndexesCreatedAndDropped() {
        Ignite ignite = CLUSTER_NODES.get(0);
        IndexManager indexManager = ((IgniteImpl) ignite).indexManager();

        CompletableFuture<IndexEventParameters> pkCreatedFuture = registerListener(indexManager, IndexEvent.CREATE);

        sql("CREATE TABLE tname (c1 INT PRIMARY KEY, c2 INT, c3 INT)");

        TableImpl table = (TableImpl) ignite.tables().table("tname");

        {
            IndexEventParameters parameters = await(pkCreatedFuture);

            assertThat(parameters, notNullValue());
            assertThat(parameters.tableId(), equalTo(table.tableId()));
            assertThat(parameters.indexDescriptor().columns(), hasItems("C1"));
            assertThat(parameters.indexDescriptor().name(), equalTo("TNAME_PK"));
        }

        CompletableFuture<IndexEventParameters> indexCreatedFuture = registerListener(indexManager, IndexEvent.CREATE);

        await(indexManager.createIndexAsync(
                CreateHashIndexParams.builder()
                        .schemaName("PUBLIC")
                        .indexName("INAME")
                        .tableName("TNAME")
                        .columns(List.of("C3", "C2"))
                        .build()
                ));

        int createdIndexId;
        {
            IndexEventParameters parameters = await(indexCreatedFuture);

            assertThat(parameters, notNullValue());
            assertThat(parameters.tableId(), equalTo(table.tableId()));
            assertThat(parameters.indexDescriptor().columns(), hasItems("C3", "C2"));
            assertThat(parameters.indexDescriptor().name(), equalTo("INAME"));

            createdIndexId = parameters.indexId();
        }

        CompletableFuture<IndexEventParameters> indexDroppedFuture = registerListener(indexManager, IndexEvent.DROP);

        await(indexManager.dropIndexAsync(DropIndexParams.builder().schemaName("PUBLIC").indexName("INAME").build()));

        {
            IndexEventParameters params = await(indexDroppedFuture);

            assertThat(params, notNullValue());
            assertThat(params.indexId(), equalTo(createdIndexId));
        }
    }

    private CompletableFuture<IndexEventParameters> registerListener(IndexManager indexManager, IndexEvent event) {
        CompletableFuture<IndexEventParameters> paramFuture = new CompletableFuture<>();

        indexManager.listen(event, (param, th) -> {
            paramFuture.complete(param);

            return CompletableFuture.completedFuture(true);
        });

        return paramFuture;
    }
}
