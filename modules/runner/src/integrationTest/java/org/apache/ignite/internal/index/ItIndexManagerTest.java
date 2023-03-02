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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.schema.configuration.index.HashIndexChange;
import org.apache.ignite.internal.sql.engine.SharedClusterIntegrationTest;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests to verify integration of {@link IndexManager} with other components.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItIndexManagerTest extends SharedClusterIntegrationTest {
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
            Index<?> index = await(pkCreatedFuture).index();

            assertThat(index, notNullValue());
            assertThat(index.tableId(), equalTo(table.tableId()));
            assertThat(index.descriptor().columns(), hasItems("C1"));
            assertThat(index.name(), equalTo("TNAME_PK"));
            assertThat(index.name(), equalTo(index.descriptor().name()));
        }

        CompletableFuture<IndexEventParameters> indexCreatedFuture = registerListener(indexManager, IndexEvent.CREATE);

        await(indexManager.createIndexAsync(
                "PUBLIC",
                "INAME",
                "TNAME",
                true,
                tableIndexChange -> tableIndexChange.convert(HashIndexChange.class).changeColumnNames("C3", "C2")
                ));

        UUID createdIndexId;
        {
            Index<?> index = await(indexCreatedFuture).index();
            createdIndexId = index.id();

            assertThat(index, notNullValue());
            assertThat(index.tableId(), equalTo(table.tableId()));
            assertThat(index.descriptor().columns(), hasItems("C3", "C2"));
            assertThat(index.name(), equalTo("INAME"));
            assertThat(index.name(), equalTo(index.descriptor().name()));
        }

        CompletableFuture<IndexEventParameters> indexDroppedFuture = registerListener(indexManager, IndexEvent.DROP);

        await(indexManager.dropIndexAsync("PUBLIC", "INAME", true));

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
