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

package org.apache.ignite.internal.catalog;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.DESC_NULLS_FIRST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.RemoveIndexCommand;
import org.apache.ignite.internal.catalog.commands.RenameIndexCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StoppingIndexEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.sql.SqlCommon;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.ArgumentCaptor;

/** Tests for index related commands. */
public class CatalogIndexTest extends BaseCatalogManagerTest {

    private static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

    @Test
    public void testCreateHashIndex() {
        int tableCreationVersion = await(manager.execute(simpleTable(TABLE_NAME)));

        int indexCreationVersion = await(manager.execute(createHashIndexCommand(INDEX_NAME, List.of("VAL", "ID"))));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(tableCreationVersion);

        assertNotNull(schema);
        assertNull(schema.aliveIndex(INDEX_NAME));
        assertNull(manager.aliveIndex(INDEX_NAME, 123L));

        // Validate actual catalog
        schema = manager.schema(indexCreationVersion);

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) schema.aliveIndex(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, manager.aliveIndex(INDEX_NAME, clock.nowLong()));
        assertSame(index, manager.index(index.id(), clock.nowLong()));

        // Validate newly created hash index
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals(List.of("VAL", "ID"), index.columns());
        assertFalse(index.unique());
        assertEquals(REGISTERED, index.status());
    }

    @Test
    public void testCreateSortedIndex() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        CatalogCommand command = createSortedIndexCommand(
                INDEX_NAME,
                true,
                List.of("VAL", "ID"),
                List.of(DESC_NULLS_FIRST, ASC_NULLS_LAST)
        );

        int indexCreationVersion = await(manager.execute(command));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(indexCreationVersion - 1);

        assertNotNull(schema);
        assertNull(schema.aliveIndex(INDEX_NAME));
        assertNull(manager.aliveIndex(INDEX_NAME, 123L));
        assertNull(manager.index(4, 123L));

        // Validate actual catalog
        schema = manager.schema(indexCreationVersion);

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) schema.aliveIndex(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, manager.aliveIndex(INDEX_NAME, clock.nowLong()));
        assertSame(index, manager.index(index.id(), clock.nowLong()));

        // Validate newly created sorted index
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals("VAL", index.columns().get(0).name());
        assertEquals("ID", index.columns().get(1).name());
        assertEquals(DESC_NULLS_FIRST, index.columns().get(0).collation());
        assertEquals(ASC_NULLS_LAST, index.columns().get(1).collation());
        assertTrue(index.unique());
        assertEquals(REGISTERED, index.status());
    }

    @Test
    public void testDropTableWithIndex() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.execute(simpleIndex(TABLE_NAME, INDEX_NAME)), willCompleteSuccessfully());
        startBuildingIndex(indexId(INDEX_NAME));
        makeIndexAvailable(indexId(INDEX_NAME));

        long beforeDropTimestamp = clock.nowLong();
        int beforeDropVersion = manager.latestCatalogVersion();

        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(beforeDropVersion);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        CatalogIndexDescriptor index = schema.aliveIndex(INDEX_NAME);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(beforeDropTimestamp));

        assertSame(table, manager.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(table, manager.table(table.id(), beforeDropTimestamp));

        assertSame(index, manager.aliveIndex(INDEX_NAME, beforeDropTimestamp));
        assertSame(index, manager.index(index.id(), beforeDropTimestamp));

        // Validate actual catalog
        schema = manager.schema(manager.latestCatalogVersion());

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, clock.nowLong()));
        assertNull(manager.table(table.id(), clock.nowLong()));

        assertThat(schema.aliveIndex(INDEX_NAME), is(nullValue()));
        assertThat(manager.aliveIndex(INDEX_NAME, clock.nowLong()), is(nullValue()));
        assertThat(manager.index(index.id(), clock.nowLong()), is(nullValue()));
    }


    @Test
    public void testGetTableIdOnDropIndexEvent() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(manager.execute(createHashIndexCommand(INDEX_NAME, List.of("VAL"))), willCompleteSuccessfully());

        int indexId = manager.aliveIndex(INDEX_NAME, clock.nowLong()).id();

        startBuildingIndex(indexId);
        makeIndexAvailable(indexId);

        int tableId = manager.table(TABLE_NAME, clock.nowLong()).id();
        int pkIndexId = manager.aliveIndex(pkIndexName(TABLE_NAME), clock.nowLong()).id();

        assertNotEquals(tableId, indexId);

        EventListener<StoppingIndexEventParameters> stoppingListener = mock(EventListener.class);
        EventListener<RemoveIndexEventParameters> removedListener = mock(EventListener.class);

        ArgumentCaptor<StoppingIndexEventParameters> stoppingCaptor = ArgumentCaptor.forClass(StoppingIndexEventParameters.class);
        ArgumentCaptor<RemoveIndexEventParameters> removingCaptor = ArgumentCaptor.forClass(RemoveIndexEventParameters.class);

        doReturn(falseCompletedFuture()).when(stoppingListener).notify(stoppingCaptor.capture());
        doReturn(falseCompletedFuture()).when(removedListener).notify(removingCaptor.capture());

        manager.listen(CatalogEvent.INDEX_STOPPING, stoppingListener);
        manager.listen(CatalogEvent.INDEX_REMOVED, removedListener);

        // Let's drop the index.
        assertThat(
                manager.execute(DropIndexCommand.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).build()),
                willCompleteSuccessfully()
        );

        StoppingIndexEventParameters stoppingEventParameters = stoppingCaptor.getValue();

        assertEquals(indexId, stoppingEventParameters.indexId());

        // Let's drop the table.
        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willCompleteSuccessfully());

        // Let's make sure that the PK index has been removed.
        RemoveIndexEventParameters pkRemovedEventParameters = removingCaptor.getAllValues().get(0);

        assertEquals(pkIndexId, pkRemovedEventParameters.indexId());
    }

    @Test
    public void testReCreateIndexWithSameName() {
        createSomeTable(TABLE_NAME);
        createSomeIndex(TABLE_NAME, INDEX_NAME);

        int catalogVersion = manager.latestCatalogVersion();
        CatalogIndexDescriptor index1 = manager.aliveIndex(INDEX_NAME, clock.nowLong());
        assertNotNull(index1);

        int indexId1 = index1.id();
        startBuildingIndex(indexId1);
        makeIndexAvailable(indexId1);

        // Drop index.
        dropIndex(INDEX_NAME);
        removeIndex(indexId1);
        assertNull(manager.aliveIndex(INDEX_NAME, clock.nowLong()));

        // Re-create index with same name.
        createSomeSortedIndex(TABLE_NAME, INDEX_NAME);

        CatalogIndexDescriptor index2 = manager.aliveIndex(INDEX_NAME, clock.nowLong());
        assertNotNull(index2);
        assertThat(index2.indexType(), equalTo(CatalogIndexDescriptorType.SORTED));

        // Ensure these are different indexes.
        int indexId2 = index2.id();
        assertNotEquals(indexId1, indexId2);

        // Ensure dropped index is available for historical queries.
        assertNotNull(manager.index(indexId1, catalogVersion));
        assertThat(manager.index(indexId1, catalogVersion).indexType(), equalTo(CatalogIndexDescriptorType.HASH));
        assertNull(manager.index(indexId2, catalogVersion));
    }

    @Test
    public void droppingAnAvailableIndexMovesItToStoppingState() {
        createSomeTable(TABLE_NAME);
        createSomeIndex(TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(indexId);
        makeIndexAvailable(indexId);

        dropIndex(INDEX_NAME);

        CatalogIndexDescriptor index = manager.index(indexId, manager.latestCatalogVersion());

        assertThat(index, is(notNullValue()));
        assertThat(index.status(), is(STOPPING));
    }

    @ParameterizedTest
    @EnumSource(value = CatalogIndexStatus.class, names = {"REGISTERED", "BUILDING"}, mode = Mode.INCLUDE)
    public void droppingNotAvailableIndexRemovesIt(CatalogIndexStatus status) {
        createSomeTable(TABLE_NAME);
        createSomeIndex(TABLE_NAME, INDEX_NAME);

        rollIndexStatusTo(status, indexId(INDEX_NAME));

        dropIndex(INDEX_NAME);

        CatalogIndexDescriptor index = index(manager.latestCatalogVersion(), INDEX_NAME);

        assertThat(index, is(nullValue()));
    }

    private void startBuildingIndex(int indexId) {
        assertThat(manager.execute(StartBuildingIndexCommand.builder().indexId(indexId).build()), willCompleteSuccessfully());
    }

    @Test
    public void removingStoppedIndexRemovesItFromCatalog() {
        createSomeTable(TABLE_NAME);
        createSomeIndex(TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        rollIndexStatusTo(STOPPING, indexId);

        assertThat(manager.index(indexId, manager.latestCatalogVersion()).status(), is(STOPPING));

        removeIndex(indexId);

        CatalogIndexDescriptor index = manager.index(indexId, manager.latestCatalogVersion());

        assertThat(index, is(nullValue()));
    }

    private void rollIndexStatusTo(CatalogIndexStatus status, int indexId) {
        for (CatalogIndexStatus currentStatus : List.of(REGISTERED, BUILDING, AVAILABLE, STOPPING)) {
            if (currentStatus == status) {
                break;
            }

            switch (currentStatus) {
                case REGISTERED:
                    startBuildingIndex(indexId);
                    break;
                case BUILDING:
                    makeIndexAvailable(indexId);
                    break;
                case AVAILABLE:
                    dropIndex(indexId);
                    break;
                case STOPPING:
                    removeIndex(indexId);
                    break;
                default:
                    fail("Unsupported state: " + currentStatus);
                    break;
            }
        }
    }

    private void removeIndex(int indexId) {
        assertThat(
                manager.execute(RemoveIndexCommand.builder().indexId(indexId).build()),
                willCompleteSuccessfully()
        );
    }

    private void dropIndex(String indexName) {
        assertThat(
                manager.execute(DropIndexCommand.builder().indexName(indexName).schemaName(SqlCommon.DEFAULT_SCHEMA_NAME).build()),
                willCompleteSuccessfully()
        );
    }

    private void dropIndex(int indexId) {
        CatalogIndexDescriptor index = manager.index(indexId, Long.MAX_VALUE);
        assertThat(index, is(notNullValue()));

        dropIndex(index.name());
    }

    @Test
    public void testDropNotExistingIndex() {
        assertThat(
                manager.execute(DropIndexCommand.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).build()),
                willThrowFast(IndexNotFoundValidationException.class)
        );
    }

    @Test
    public void testStartHashIndexBuilding() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("key1"))),
                willCompleteSuccessfully()
        );

        assertThat(
                manager.execute(StartBuildingIndexCommand.builder().indexId(indexId(INDEX_NAME)).build()),
                willCompleteSuccessfully()
        );

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) index(manager.latestCatalogVersion(), INDEX_NAME);

        assertEquals(BUILDING, index.status());
    }

    @Test
    public void testStartSortedIndexBuilding() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, List.of("key1"), List.of(ASC_NULLS_LAST))),
                willCompleteSuccessfully()
        );

        assertThat(
                manager.execute(StartBuildingIndexCommand.builder().indexId(indexId(INDEX_NAME)).build()),
                willCompleteSuccessfully()
        );

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) index(manager.latestCatalogVersion(), INDEX_NAME);

        assertEquals(BUILDING, index.status());
    }

    @Test
    public void testStartBuildingIndexEvent() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("key1"))),
                willCompleteSuccessfully()
        );

        int indexId = index(manager.latestCatalogVersion(), INDEX_NAME).id();

        var fireEventFuture = new CompletableFuture<Void>();

        manager.listen(CatalogEvent.INDEX_BUILDING, fromConsumer(fireEventFuture, (StartBuildingIndexEventParameters parameters) -> {
            assertEquals(indexId, parameters.indexId());
        }));

        assertThat(
                manager.execute(startBuildingIndexCommand(indexId)),
                willCompleteSuccessfully()
        );

        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    public void testIndexEvents() {
        CatalogCommand createIndexCmd = createHashIndexCommand(INDEX_NAME, List.of("ID"));

        CatalogCommand dropIndexCmd = DropIndexCommand.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).build();

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any())).thenReturn(falseCompletedFuture());

        manager.listen(CatalogEvent.INDEX_CREATE, eventListener);
        manager.listen(CatalogEvent.INDEX_BUILDING, eventListener);
        manager.listen(CatalogEvent.INDEX_AVAILABLE, eventListener);
        manager.listen(CatalogEvent.INDEX_STOPPING, eventListener);
        manager.listen(CatalogEvent.INDEX_REMOVED, eventListener);

        // Try to create index without table.
        assertThat(manager.execute(createIndexCmd), willThrow(TableNotFoundValidationException.class));
        verifyNoInteractions(eventListener);

        // Create table with PK index.
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        verify(eventListener).notify(any(CreateIndexEventParameters.class));
        verify(eventListener).notify(any(MakeIndexAvailableEventParameters.class));

        verifyNoMoreInteractions(eventListener);
        clearInvocations(eventListener);

        // Create index.
        assertThat(manager.execute(createIndexCmd), willCompleteSuccessfully());
        verify(eventListener).notify(any(CreateIndexEventParameters.class));

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(indexId);
        verify(eventListener).notify(any(StartBuildingIndexEventParameters.class));

        makeIndexAvailable(indexId);
        verify(eventListener).notify(any(MakeIndexAvailableEventParameters.class));

        verifyNoMoreInteractions(eventListener);
        clearInvocations(eventListener);

        // Drop index.
        assertThat(manager.execute(dropIndexCmd), willCompleteSuccessfully());
        verify(eventListener).notify(any(StoppingIndexEventParameters.class));

        // Remove index.
        removeIndex(indexId);
        verify(eventListener).notify(any(RemoveIndexEventParameters.class));

        verifyNoMoreInteractions(eventListener);
        clearInvocations(eventListener);

        // Drop table with pk index.
        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willCompleteSuccessfully());

        // Try drop index once again.
        assertThat(manager.execute(dropIndexCmd), willThrow(IndexNotFoundValidationException.class));

        verify(eventListener).notify(any(RemoveIndexEventParameters.class));
        verifyNoMoreInteractions(eventListener);
        clearInvocations(eventListener);


    }

    @Test
    public void testMakeHashIndexAvailable() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("key1"))),
                willCompleteSuccessfully()
        );

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(indexId);
        makeIndexAvailable(indexId);

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) index(manager.latestCatalogVersion(), INDEX_NAME);

        assertEquals(AVAILABLE, index.status());
    }

    private void makeIndexAvailable(int indexId) {
        assertThat(
                manager.execute(MakeIndexAvailableCommand.builder().indexId(indexId).build()),
                willCompleteSuccessfully()
        );
    }

    @Test
    public void testMakeSortedIndexAvailable() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, List.of("key1"), List.of(ASC_NULLS_LAST))),
                willCompleteSuccessfully()
        );

        int indexId = indexId(INDEX_NAME);

        assertThat(
                manager.execute(startBuildingIndexCommand(indexId)),
                willCompleteSuccessfully()
        );

        makeIndexAvailable(indexId);

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) index(manager.latestCatalogVersion(), INDEX_NAME);

        assertEquals(AVAILABLE, index.status());
    }

    @Test
    public void testAvailableIndexEvent() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("key1"))),
                willCompleteSuccessfully()
        );

        int indexId = index(manager.latestCatalogVersion(), INDEX_NAME).id();

        var fireEventFuture = new CompletableFuture<Void>();

        manager.listen(CatalogEvent.INDEX_AVAILABLE, fromConsumer(fireEventFuture, (MakeIndexAvailableEventParameters parameters) -> {
            assertEquals(indexId, parameters.indexId());
        }));

        assertThat(
                manager.execute(startBuildingIndexCommand(indexId)),
                willCompleteSuccessfully()
        );

        makeIndexAvailable(indexId);

        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    public void testPkAvailableIndexEvent() {
        String tableName = TABLE_NAME + "_new";

        var fireEventFuture = new CompletableFuture<Void>();

        manager.listen(CatalogEvent.INDEX_AVAILABLE, fromConsumer(fireEventFuture, (MakeIndexAvailableEventParameters parameters) -> {
            CatalogIndexDescriptor catalogIndexDescriptor = manager.index(parameters.indexId(), parameters.catalogVersion());

            assertNotNull(catalogIndexDescriptor);
            assertEquals(pkIndexName(tableName), catalogIndexDescriptor.name());
        }));

        createSomeTable(tableName);

        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    public void testPkAvailableOnCreateIndexEvent() {
        var fireEventFuture = new CompletableFuture<Void>();

        manager.listen(CatalogEvent.INDEX_CREATE, fromConsumer(fireEventFuture, (CreateIndexEventParameters parameters) -> {
            assertEquals(AVAILABLE, parameters.indexDescriptor().status());
        }));

        createSomeTable(TABLE_NAME);

        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    public void testCreateIndexWithAlreadyExistingName() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.execute(simpleIndex()), willCompleteSuccessfully());

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("VAL"))),
                willThrowFast(IndexExistsValidationException.class)
        );

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(IndexExistsValidationException.class)
        );
    }

    @Test
    public void testCreateIndexWithSameNameAsExistingTable() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.execute(createHashIndexCommand(TABLE_NAME, List.of("VAL"))),
                willThrowFast(TableExistsValidationException.class)
        );

        assertThat(
                manager.execute(createSortedIndexCommand(TABLE_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(TableExistsValidationException.class)
        );
    }

    @Test
    public void testCreateIndexWithNotExistingTable() {
        assertThat(
                manager.execute(createHashIndexCommand(TABLE_NAME, List.of("VAL"))),
                willThrowFast(TableNotFoundValidationException.class)
        );

        assertThat(
                manager.execute(createSortedIndexCommand(TABLE_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(TableNotFoundValidationException.class)
        );
    }

    @Test
    public void testCreateIndexWithMissingTableColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("fake"))),
                willThrowFast(CatalogValidationException.class)
        );

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, List.of("fake"), List.of(ASC_NULLS_LAST))),
                willThrowFast(CatalogValidationException.class)
        );
    }

    @Test
    public void testCreateUniqIndexWithMissingTableColocationColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, true, List.of("VAL"))),
                willThrowFast(CatalogValidationException.class, "Unique index must include all colocation columns")
        );

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, true, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(CatalogValidationException.class, "Unique index must include all colocation columns")
        );
    }

    @Test
    public void testIndexes() {
        int initialVersion = manager.latestCatalogVersion();

        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.execute(simpleIndex()), willCompleteSuccessfully());

        assertThat(manager.indexes(initialVersion), empty());
        assertThat(
                manager.indexes(initialVersion + 1),
                hasItems(index(initialVersion + 1, pkIndexName(TABLE_NAME)))
        );
        assertThat(
                manager.indexes(initialVersion + 2),
                hasItems(index(initialVersion + 2, pkIndexName(TABLE_NAME)), index(initialVersion + 2, INDEX_NAME))
        );
    }

    @Test
    public void testGetIndexesForTables() {
        String tableName0 = TABLE_NAME + 0;
        String tableName1 = TABLE_NAME + 1;

        createSomeTable(tableName0);
        createSomeTable(tableName1);

        createSomeIndex(tableName1, INDEX_NAME);

        int catalogVersion = manager.latestCatalogVersion();

        // Let's check for a non-existent table.
        assertThat(tableIndexIds(catalogVersion, Integer.MAX_VALUE), empty());

        // Let's check for an existing tables.
        int tableId0 = tableId(tableName0);
        int tableId1 = tableId(tableName1);

        assertThat(tableIndexIds(catalogVersion, tableId0), hasItems(indexId(pkIndexName(tableName0))));
        assertThat(tableIndexIds(catalogVersion, tableId1), hasItems(indexId(pkIndexName(tableName1)), indexId(INDEX_NAME)));
    }

    @Test
    public void testGetIndexesForTableInSortedOrderById() {
        createSomeTable(TABLE_NAME);

        String indexName0 = INDEX_NAME + 0;
        String indexName1 = INDEX_NAME + 1;

        createSomeIndex(TABLE_NAME, indexName0);
        createSomeIndex(TABLE_NAME, indexName1);

        int indexId0 = indexId(pkIndexName(TABLE_NAME));
        int indexId1 = indexId(indexName0);
        int indexId2 = indexId(indexName1);

        int catalogVersion = manager.latestCatalogVersion();

        assertThat(tableIndexIds(catalogVersion, tableId(TABLE_NAME)), equalTo(List.of(indexId0, indexId1, indexId2)));
    }

    @Test
    public void testRenameIndex() {
        createSomeTable(TABLE_NAME);
        createSomeIndex(TABLE_NAME, INDEX_NAME);

        long beforeRename = clock.nowLong();

        CatalogIndexDescriptor index = manager.aliveIndex(INDEX_NAME, clock.nowLong());
        assertThat(index, is(notNullValue()));

        int indexId = index.id();

        // Rename index.
        renameIndex(INDEX_NAME, INDEX_NAME_2);

        // Ensure index is available by new name.
        assertThat(manager.aliveIndex(INDEX_NAME, clock.nowLong()), is(nullValue()));

        index = manager.aliveIndex(INDEX_NAME_2, clock.nowLong());
        assertThat(index, is(notNullValue()));
        assertThat(index.id(), is(indexId));
        assertThat(index.name(), is(INDEX_NAME_2));

        // Ensure renamed index is available for historical queries.
        CatalogIndexDescriptor oldDescriptor = manager.aliveIndex(INDEX_NAME, beforeRename);
        assertThat(oldDescriptor, is(notNullValue()));
        assertThat(oldDescriptor.id(), is(indexId));

        // Ensure can create new index with same name.
        createSomeIndex(TABLE_NAME, INDEX_NAME);

        index = manager.aliveIndex(INDEX_NAME, clock.nowLong());
        assertThat(index, is(notNullValue()));
        assertThat(index.id(), not(indexId));
    }

    @Test
    public void testRenamePkIndex() {
        createSomeTable(TABLE_NAME);

        CatalogTableDescriptor table = manager.table(TABLE_NAME, clock.nowLong());
        assertThat(table, is(notNullValue()));

        assertThat(manager.aliveIndex(INDEX_NAME, clock.nowLong()), is(nullValue()));
        assertThat(manager.aliveIndex(pkIndexName(TABLE_NAME), clock.nowLong()), is(notNullValue()));

        int primaryKeyIndexId = table.primaryKeyIndexId();

        // Rename index.
        renameIndex(pkIndexName(TABLE_NAME), INDEX_NAME);

        CatalogIndexDescriptor index = manager.aliveIndex(INDEX_NAME, clock.nowLong());
        assertThat(index, is(notNullValue()));
        assertThat(index.id(), is(primaryKeyIndexId));
        assertThat(index.name(), is(INDEX_NAME));

        assertThat(manager.aliveIndex(pkIndexName(TABLE_NAME), clock.nowLong()), is(nullValue()));
    }

    @Test
    public void testRenameNonExistingIndex() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(RenameIndexCommand.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).newIndexName("TEST").build()),
                willThrowFast(IndexNotFoundValidationException.class)
        );
    }

    private @Nullable CatalogIndexDescriptor index(int catalogVersion, String indexName) {
        return manager.schema(catalogVersion).aliveIndex(indexName);
    }

    private int indexId(String indexName) {
        CatalogIndexDescriptor index = manager.aliveIndex(indexName, clock.nowLong());

        assertNotNull(index, indexName);

        return index.id();
    }

    private List<Integer> tableIndexIds(int catalogVersion, int tableId) {
        return manager.indexes(catalogVersion, tableId).stream().map(CatalogObjectDescriptor::id).collect(toList());
    }

    private int tableId(String tableName) {
        CatalogTableDescriptor table = manager.table(tableName, clock.nowLong());

        assertNotNull(table, tableName);

        return table.id();
    }

    private void createSomeIndex(String tableName, String indexName) {
        assertThat(
                manager.execute(createHashIndexCommand(tableName, indexName, false, List.of("key1"))),
                willCompleteSuccessfully()
        );
    }

    private void createSomeSortedIndex(String tableName, String indexName) {
        assertThat(
                manager.execute(createSortedIndexCommand(tableName, indexName, false, List.of("key1"), List.of(ASC_NULLS_LAST))),
                willCompleteSuccessfully()
        );
    }

    private void renameIndex(String indexName, String newIndexName) {
        assertThat(
                manager.execute(renameIndexCommand(indexName, newIndexName)),
                willCompleteSuccessfully()
        );
    }
}
