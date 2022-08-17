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

package org.apache.ignite.internal.storage;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Abstract class that contains tests for {@link MvTableStorage} implementations.
 */
// TODO: Use this to test RocksDB-based storage, see https://issues.apache.org/jira/browse/IGNITE-17318
// TODO: Use this to test B+tree-based storage, see https://issues.apache.org/jira/browse/IGNITE-17320
public abstract class AbstractMvTableStorageTest {
    private static final String SORTED_INDEX_NAME_1 = "SORTED_IDX_1";

    private static final String SORTED_INDEX_NAME_2 = "SORTED_IDX_2";

    private MvTableStorage tableStorage;

    protected abstract MvTableStorage tableStorage();

    @BeforeEach
    void setUp() {
        tableStorage = tableStorage();

        createTestTable();
    }

    @AfterEach
    void tearDown() {
        tableStorage.destroy();
    }

    /**
     * Tests creating a partition.
     */
    @Test
    public void testCreatePartition() {
        int partitionId = 0;

        assertThat(tableStorage.getMvPartition(partitionId), is(nullValue()));

        assertThat(tableStorage.getOrCreateMvPartition(partitionId), is(notNullValue()));

        assertThat(tableStorage.getMvPartition(partitionId), is(notNullValue()));
    }

    /**
     * Tests destroying a partition.
     */
    @Test
    public void testDestroyPartition() {
        int partitionId = 0;

        assertThat(tableStorage.getOrCreateMvPartition(partitionId), is(notNullValue()));

        tableStorage.createIndex(SORTED_INDEX_NAME_1);

        assertThat(tableStorage.getMvPartition(partitionId), is(notNullValue()));

        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_1), is(notNullValue()));

        // Validate that destroying a partition also destroys associated indices.
        assertThat(tableStorage.destroyPartition(partitionId), willCompleteSuccessfully());

        assertThat(tableStorage.getMvPartition(partitionId), is(nullValue()));

        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_1), is(nullValue()));
    }

    /**
     * Test creating an index.
     */
    @Test
    public void testCreateSortedIndex() {
        int partitionId = 0;

        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_1), is(nullValue()));

        tableStorage.createIndex(SORTED_INDEX_NAME_1);

        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_1), is(nullValue()));

        // Index should only be available after the associated partition has been created.
        tableStorage.getOrCreateMvPartition(partitionId);

        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_1), is(notNullValue()));
    }

    /**
     * Tests destroying an index.
     */
    @Test
    public void testDestroyIndex() {
        int partitionId = 0;

        tableStorage.getOrCreateMvPartition(partitionId);

        tableStorage.createIndex(SORTED_INDEX_NAME_1);
        tableStorage.createIndex(SORTED_INDEX_NAME_2);

        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_1), is(notNullValue()));
        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_2), is(notNullValue()));

        assertThat(tableStorage.destroyIndex(SORTED_INDEX_NAME_1), willCompleteSuccessfully());

        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_1), is(nullValue()));
        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_2), is(notNullValue()));

        // Check that destroying an index twice is not an error
        assertThat(tableStorage.destroyIndex(SORTED_INDEX_NAME_1), willCompleteSuccessfully());

        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_1), is(nullValue()));

        assertThat(tableStorage.destroyIndex(SORTED_INDEX_NAME_2), willCompleteSuccessfully());

        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_1), is(nullValue()));
        assertThat(tableStorage.getSortedIndex(partitionId, SORTED_INDEX_NAME_2), is(nullValue()));
    }

    private void createTestTable() {
        TableDefinition tableDefinition = SchemaBuilders.tableBuilder("PUBLIC", "TEST")
                .columns(
                        SchemaBuilders.column("ID", ColumnType.INT32).build(),
                        SchemaBuilders.column("COLUMN0", ColumnType.INT32).build()
                )
                .withPrimaryKey("ID")
                .withIndex(
                        SchemaBuilders.sortedIndex(SORTED_INDEX_NAME_1)
                                .addIndexColumn("COLUMN0").done()
                                .build()
                )
                .withIndex(
                        SchemaBuilders.sortedIndex(SORTED_INDEX_NAME_2)
                                .addIndexColumn("COLUMN0").done()
                                .build()
                )
                .build();

        CompletableFuture<Void> createTableFuture = tableStorage.configuration()
                .change(tableChange -> SchemaConfigurationConverter.convert(tableDefinition, tableChange));

        assertThat(createTableFuture, willCompleteSuccessfully());
    }
}
