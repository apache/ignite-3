/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.schemas.table.HashIndexChange;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests to verify integration of {@link IndexManager} with other components.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItIndexManagerTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override
    protected int nodes() {
        return 1;
    }

    @Test
    public void eventsAreFiredWhenIndexesCreatedAndDropped() {
        Ignite ignite = CLUSTER_NODES.get(0);

        TableImpl table = (TableImpl) ignite.tables().createTable("PUBLIC.TNAME", tableChange -> {
            tableChange.changeColumns(columnsChange -> {
                columnsChange.create("c1", columnChange ->
                        columnChange.changeType(columnTypeChange -> columnTypeChange.changeType("INT32")));
                columnsChange.create("c2", columnChange ->
                        columnChange.changeType(columnTypeChange -> columnTypeChange.changeType("INT32")));
                columnsChange.create("c3", columnChange ->
                        columnChange.changeType(columnTypeChange -> columnTypeChange.changeType("INT32")));
            });

            tableChange.changePrimaryKey(primaryKeyChange -> {
                primaryKeyChange.changeColumns("c1");
                primaryKeyChange.changeColocationColumns("c1");
            });
        });

        IndexManager indexManager = ((IgniteImpl) ignite).indexManager();

        AtomicReference<IndexEventParameters> createEventParamHolder = new AtomicReference<>();
        AtomicReference<IndexEventParameters> dropEventParamHolder = new AtomicReference<>();

        indexManager.listen(IndexEvent.CREATE, (param, th) -> {
            createEventParamHolder.set(param);

            return CompletableFuture.completedFuture(true);
        });
        indexManager.listen(IndexEvent.DROP, (param, th) -> {
            dropEventParamHolder.set(param);

            return CompletableFuture.completedFuture(true);
        });

        Index<?> index = indexManager.createIndexAsync("PUBLIC", "INAME", "TNAME", tableIndexChange ->
                tableIndexChange.convert(HashIndexChange.class).changeColumnNames("c3", "c2")).join();

        assertThat(index, notNullValue());
        assertThat(index.tableId(), equalTo(table.tableId()));
        assertThat(index.descriptor().columns(), hasItems("c3", "c2"));
        assertThat(index.name(), equalTo("PUBLIC.INAME"));
        assertThat(index.name(), equalTo(index.descriptor().name()));
        assertThat(createEventParamHolder.get(), notNullValue());
        assertThat(index, sameInstance(createEventParamHolder.get().index()));

        indexManager.dropIndexAsync("PUBLIC", "INAME").join();

        assertThat(dropEventParamHolder.get(), notNullValue());
        assertThat(index.id(), sameInstance(dropEventParamHolder.get().indexId()));
    }
}
