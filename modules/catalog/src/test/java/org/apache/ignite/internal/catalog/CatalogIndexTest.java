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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CollectionUtils.view;
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
import static org.junit.jupiter.api.Assertions.assertNotSame;
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

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.RemoveIndexCommand;
import org.apache.ignite.internal.catalog.commands.RenameIndexCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
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
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.ArgumentCaptor;

/** Tests for index related commands. */
public class CatalogIndexTest extends BaseCatalogManagerTest {

    @Test
    public void testCreateHashIndex() {
        int tableCreationVersion = tryApplyAndExpectApplied(simpleTable(TABLE_NAME)).getCatalogVersion();

        int indexCreationVersion = tryApplyAndExpectApplied(createHashIndexCommand(INDEX_NAME, List.of("VAL", "ID"))).getCatalogVersion();

        // Validate catalog version from the past.
        Catalog catalog = manager.catalog(tableCreationVersion);

        assertNotNull(catalog);
        assertNotNull(catalog.table(SCHEMA_NAME, TABLE_NAME));
        assertNull(catalog.aliveIndex(SCHEMA_NAME, INDEX_NAME));
        assertNull(catalog.schema(SCHEMA_NAME).aliveIndex(INDEX_NAME));

        // Validate actual catalog
        catalog = manager.catalog(indexCreationVersion);

        assertNotNull(catalog);

        CatalogTableDescriptor table = catalog.table(SCHEMA_NAME, TABLE_NAME);
        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) catalog.aliveIndex(SCHEMA_NAME, INDEX_NAME);

        assertNotNull(table);
        assertNotNull(index);
        assertSame(index, catalog.schema(SCHEMA_NAME).aliveIndex(INDEX_NAME));
        assertSame(index, catalog.index(index.id()));

        // Validate newly created hash index
        assertEquals(INDEX_NAME, index.name());
        assertEquals(CatalogIndexDescriptorType.HASH, index.indexType());
        assertEquals(table.id(), index.tableId());
        assertEquals(IntList.of(1, 0), index.columnIds());
        assertFalse(index.unique());
        assertEquals(REGISTERED, index.status());
    }

    /** The index created with the table must be in the {@link CatalogIndexStatus#AVAILABLE} state. */
    @Test
    public void testCreateHashIndexWithTable() {
        int catalogVersion = tryApplyAndCheckExpect(
                List.of(
                        simpleTable(TABLE_NAME),
                        createHashIndexCommand(INDEX_NAME, List.of("VAL", "ID"))),
                true, true).getCatalogVersion();

        Catalog catalog = manager.catalog(catalogVersion);
        assertNotNull(catalog);

        // Validate newly created hash index.
        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) catalog.aliveIndex(SCHEMA_NAME, INDEX_NAME);
        assertEquals(INDEX_NAME, index.name());
        assertEquals(catalog.table(SCHEMA_NAME, TABLE_NAME).id(), index.tableId());
        assertEquals(IntList.of(1, 0), index.columnIds());
        assertFalse(index.unique());
        assertEquals(AVAILABLE, index.status());
    }

    @Test
    public void testCreateSortedIndex() {
        int tableCreationVersion = tryApplyAndExpectApplied(simpleTable(TABLE_NAME)).getCatalogVersion();

        int indexCreationVersion = tryApplyAndExpectApplied(
                createSortedIndexCommand(
                    INDEX_NAME,
                    true,
                    List.of("VAL", "ID"),
                    List.of(DESC_NULLS_FIRST, ASC_NULLS_LAST)
        )).getCatalogVersion();

        // Validate catalog version from the past.
        Catalog catalog = manager.catalog(tableCreationVersion);

        assertNotNull(catalog);
        assertNotNull(catalog.table(SCHEMA_NAME, TABLE_NAME));
        assertNull(catalog.aliveIndex(SCHEMA_NAME, INDEX_NAME));
        assertNull(catalog.schema(SCHEMA_NAME).aliveIndex(INDEX_NAME));

        // Validate actual catalog
        catalog = manager.catalog(indexCreationVersion);

        assertNotNull(catalog);

        CatalogTableDescriptor table = catalog.table(SCHEMA_NAME, TABLE_NAME);
        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) catalog.aliveIndex(SCHEMA_NAME, INDEX_NAME);

        assertNotNull(table);
        assertNotNull(index);
        assertSame(index, catalog.schema(SCHEMA_NAME).aliveIndex(INDEX_NAME));
        assertSame(index, catalog.index(index.id()));

        // Validate newly created sorted index
        assertEquals(INDEX_NAME, index.name());
        assertEquals(CatalogIndexDescriptorType.SORTED, index.indexType());
        assertEquals(table.id(), index.tableId());
        assertEquals(List.of(1, 0), view(index.columns(), CatalogIndexColumnDescriptor::columnId));
        assertEquals(List.of(DESC_NULLS_FIRST, ASC_NULLS_LAST), view(index.columns(), CatalogIndexColumnDescriptor::collation));
        assertTrue(index.unique());
        assertEquals(REGISTERED, index.status());

    }

    /** The index created with the table must be in the {@link CatalogIndexStatus#AVAILABLE} state. */
    @Test
    public void testCreateSortedIndexWithTable() {
        int catalogVersion = tryApplyAndCheckExpect(
                List.of(
                        simpleTable(TABLE_NAME),
                        createSortedIndexCommand(
                                INDEX_NAME,
                                true,
                                List.of("VAL", "ID"),
                                List.of(DESC_NULLS_FIRST, ASC_NULLS_LAST)
                        )),
                true, true).getCatalogVersion();

        Catalog catalog = manager.catalog(catalogVersion);
        assertNotNull(catalog);

        // Validate newly created sorted index.
        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) catalog.aliveIndex(SCHEMA_NAME, INDEX_NAME);
        assertEquals(INDEX_NAME, index.name());
        assertEquals(catalog.table(SCHEMA_NAME, TABLE_NAME).id(), index.tableId());
        assertEquals(1, index.columns().get(0).columnId());
        assertEquals(0, index.columns().get(1).columnId());
        assertEquals(DESC_NULLS_FIRST, index.columns().get(0).collation());
        assertEquals(ASC_NULLS_LAST, index.columns().get(1).collation());
        assertTrue(index.unique());
        assertEquals(AVAILABLE, index.status());
    }

    @Test
    public void testDropTableWithIndex() {
        createTableWithIndex(TABLE_NAME, INDEX_NAME);

        long beforeDropTimestamp = clock.nowLong();
        int beforeDropVersion = manager.latestCatalogVersion();

        tryApplyAndExpectApplied(dropTableCommand(TABLE_NAME));

        // Validate catalog version from the past.
        Catalog catalog = manager.catalog(beforeDropVersion);

        assertNotNull(catalog);
        assertSame(catalog, manager.activeCatalog(beforeDropTimestamp));

        CatalogTableDescriptor table = catalog.table(SCHEMA_NAME, TABLE_NAME);
        CatalogIndexDescriptor index = catalog.aliveIndex(SCHEMA_NAME, INDEX_NAME);

        assertNotNull(table);
        assertNotNull(index);
        assertSame(table, catalog.table(table.id()));
        assertSame(index, catalog.index(index.id()));

        assertThat(index.status(), is(AVAILABLE));

        // Validate actual catalog
        Catalog latestCatalog = manager.latestCatalog();

        assertNotNull(latestCatalog);
        assertSame(latestCatalog, manager.activeCatalog(clock.nowLong()));
        assertNotSame(catalog, latestCatalog);

        assertNull(latestCatalog.table(SCHEMA_NAME, TABLE_NAME));
        assertNull(latestCatalog.table(table.id()));

        assertNull(latestCatalog.aliveIndex(SCHEMA_NAME, INDEX_NAME));
        assertNull(latestCatalog.index(index.id()));
    }

    @Test
    public void testGetTableIdOnDropIndexEvent() {
        createTableWithIndex(TABLE_NAME, INDEX_NAME);

        Catalog catalog = manager.activeCatalog(clock.nowLong());

        CatalogTableDescriptor table = catalog.table(SCHEMA_NAME, TABLE_NAME);
        CatalogIndexDescriptor pkIndex = catalog.aliveIndex(SCHEMA_NAME, pkIndexName(TABLE_NAME));
        CatalogIndexDescriptor index = catalog.aliveIndex(SCHEMA_NAME, INDEX_NAME);

        assertNotNull(table);
        assertNotNull(pkIndex);
        assertNotNull(index);

        assertThat(index.status(), is(AVAILABLE));
        assertThat(index.indexType(), is(CatalogIndexDescriptorType.HASH));
        assertNotEquals(pkIndex.id(), index.id());

        EventListener<StoppingIndexEventParameters> stoppingListener = mock(EventListener.class);
        EventListener<RemoveIndexEventParameters> removedListener = mock(EventListener.class);

        ArgumentCaptor<StoppingIndexEventParameters> stoppingCaptor = ArgumentCaptor.forClass(StoppingIndexEventParameters.class);
        ArgumentCaptor<RemoveIndexEventParameters> removingCaptor = ArgumentCaptor.forClass(RemoveIndexEventParameters.class);

        doReturn(falseCompletedFuture()).when(stoppingListener).notify(stoppingCaptor.capture());
        doReturn(falseCompletedFuture()).when(removedListener).notify(removingCaptor.capture());

        manager.listen(CatalogEvent.INDEX_STOPPING, stoppingListener);
        manager.listen(CatalogEvent.INDEX_REMOVED, removedListener);

        // Let's drop the index.
        tryApplyAndExpectApplied(DropIndexCommand.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).build());

        StoppingIndexEventParameters stoppingEventParameters = stoppingCaptor.getValue();

        assertEquals(index.id(), stoppingEventParameters.indexId());

        // Let's drop the table.
        tryApplyAndExpectApplied(dropTableCommand(TABLE_NAME));

        // Let's make sure that the PK index has been removed.
        RemoveIndexEventParameters pkRemovedEventParameters = removingCaptor.getAllValues().get(0);

        assertEquals(pkIndex.id(), pkRemovedEventParameters.indexId());
    }

    @Test
    public void testReCreateIndexWithSameName() {
        createTableWithIndex(TABLE_NAME, INDEX_NAME);

        int beforeDropVersion = manager.latestCatalogVersion();

        CatalogIndexDescriptor index1 = index(beforeDropVersion, INDEX_NAME);

        assertNotNull(index1);

        int indexId1 = index1.id();

        // Drop index.
        dropIndex(INDEX_NAME);
        removeIndex(indexId1);
        assertNull(index(manager.latestCatalogVersion(), INDEX_NAME));

        // Re-create index with same name.
        createSomeSortedIndex(TABLE_NAME, INDEX_NAME);

        CatalogIndexDescriptor index2 = index(manager.latestCatalogVersion(), INDEX_NAME);
        assertNotNull(index2);
        assertThat(index2.indexType(), equalTo(CatalogIndexDescriptorType.SORTED));

        // Ensure these are different indexes.
        int indexId2 = index2.id();
        assertNotEquals(indexId1, indexId2);

        // Ensure dropped index is available for historical queries.
        assertSame(index1, manager.catalog(beforeDropVersion).index(indexId1));
        assertNull(manager.catalog(beforeDropVersion).index(indexId2));
    }

    @Test
    public void droppingAnAvailableIndexMovesItToStoppingState() {
        createSomeTable(TABLE_NAME);
        createSomeIndex(TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        rollIndexStatusTo(AVAILABLE, indexId);

        dropIndex(INDEX_NAME);

        CatalogIndexDescriptor index = manager.activeCatalog(clock.nowLong()).index(indexId);

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
        tryApplyAndExpectApplied(StartBuildingIndexCommand.builder().indexId(indexId).build());
    }

    @Test
    public void removingStoppedIndexRemovesItFromCatalog() {
        createSomeTable(TABLE_NAME);
        createSomeIndex(TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        rollIndexStatusTo(STOPPING, indexId);

        assertThat(manager.activeCatalog(clock.nowLong()).index(indexId).status(), is(STOPPING));
        // Stopping index can't be resolved by name.
        assertNull(manager.activeCatalog(clock.nowLong()).aliveIndex(SCHEMA_NAME, INDEX_NAME));

        removeIndex(indexId);

        assertNull(manager.activeCatalog(clock.nowLong()).index(indexId));
        assertNull(manager.activeCatalog(clock.nowLong()).aliveIndex(SCHEMA_NAME, INDEX_NAME));
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
        tryApplyAndExpectApplied(RemoveIndexCommand.builder().indexId(indexId).build());
    }

    private void dropIndex(String indexName) {
        tryApplyAndExpectApplied(DropIndexCommand.builder().indexName(indexName).schemaName(SCHEMA_NAME).build());
    }

    private void dropIndex(int indexId) {
        CatalogIndexDescriptor index = manager.activeCatalog(Long.MAX_VALUE).index(indexId);
        assertThat(index, is(notNullValue()));

        dropIndex(index.name());
    }

    @Test
    public void testDropNotExistingIndex() {
        assertNull(manager.activeCatalog(clock.nowLong()).aliveIndex(SCHEMA_NAME, INDEX_NAME));

        assertThat(
                manager.execute(DropIndexCommand.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).build()),
                willThrowFast(CatalogValidationException.class, "Index with name 'PUBLIC.myIndex' not found.")
        );
    }

    @Test
    public void testStartHashIndexBuilding() {
        createSomeTable(TABLE_NAME);

        tryApplyAndExpectApplied(createHashIndexCommand(INDEX_NAME, List.of("key1")));

        tryApplyAndExpectApplied(StartBuildingIndexCommand.builder().indexId(indexId(INDEX_NAME)).build());

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) index(manager.latestCatalogVersion(), INDEX_NAME);

        assertEquals(BUILDING, index.status());
    }

    @Test
    public void testStartSortedIndexBuilding() {
        createSomeTable(TABLE_NAME);

        tryApplyAndExpectApplied(createSortedIndexCommand(INDEX_NAME, List.of("key1"), List.of(ASC_NULLS_LAST)));

        tryApplyAndExpectApplied(StartBuildingIndexCommand.builder().indexId(indexId(INDEX_NAME)).build());

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) index(manager.latestCatalogVersion(), INDEX_NAME);

        assertEquals(BUILDING, index.status());
    }

    @Test
    public void testStartBuildingIndexEvent() {
        createSomeTable(TABLE_NAME);

        tryApplyAndExpectApplied(createHashIndexCommand(INDEX_NAME, List.of("key1")));

        int indexId = index(manager.latestCatalogVersion(), INDEX_NAME).id();

        var fireEventFuture = new CompletableFuture<Void>();

        manager.listen(CatalogEvent.INDEX_BUILDING, fromConsumer(fireEventFuture, (StartBuildingIndexEventParameters parameters) -> {
            assertEquals(indexId, parameters.indexId());
        }));

        tryApplyAndExpectApplied(startBuildingIndexCommand(indexId));

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
        assertThat(manager.execute(createIndexCmd),
                willThrow(CatalogValidationException.class, "Table with name 'PUBLIC.test_table' not found"));
        verifyNoInteractions(eventListener);

        // Create table with PK index.
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        verify(eventListener).notify(any(CreateIndexEventParameters.class));

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
        tryApplyAndExpectApplied(dropTableCommand(TABLE_NAME));

        // Try drop index once again.
        assertThat(manager.execute(dropIndexCmd),
                willThrowFast(CatalogValidationException.class, "Index with name 'PUBLIC.myIndex' not found."));

        verify(eventListener).notify(any(RemoveIndexEventParameters.class));
        verifyNoMoreInteractions(eventListener);
        clearInvocations(eventListener);
    }

    @Test
    public void testMakeHashIndexAvailable() {
        createSomeTable(TABLE_NAME);

        tryApplyAndExpectApplied(createHashIndexCommand(INDEX_NAME, List.of("key1")));

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(indexId);
        makeIndexAvailable(indexId);

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) index(manager.latestCatalogVersion(), INDEX_NAME);

        assertEquals(AVAILABLE, index.status());
    }

    private void makeIndexAvailable(int indexId) {
        tryApplyAndExpectApplied(MakeIndexAvailableCommand.builder().indexId(indexId).build());
    }

    @Test
    public void testMakeSortedIndexAvailable() {
        createSomeTable(TABLE_NAME);

        tryApplyAndExpectApplied(createSortedIndexCommand(INDEX_NAME, List.of("key1"), List.of(ASC_NULLS_LAST)));

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(indexId);
        makeIndexAvailable(indexId);

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) index(manager.latestCatalogVersion(), INDEX_NAME);

        assertEquals(AVAILABLE, index.status());
    }

    @Test
    public void testAvailableIndexEvent() {
        createSomeTable(TABLE_NAME);

        tryApplyAndExpectApplied(createHashIndexCommand(INDEX_NAME, List.of("key1")));

        int indexId = index(manager.latestCatalogVersion(), INDEX_NAME).id();

        var fireEventFuture = new CompletableFuture<Void>();

        manager.listen(CatalogEvent.INDEX_AVAILABLE, fromConsumer(fireEventFuture, (MakeIndexAvailableEventParameters parameters) -> {
            assertEquals(indexId, parameters.indexId());
        }));

        tryApplyAndExpectApplied(startBuildingIndexCommand(indexId));

        makeIndexAvailable(indexId);

        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    public void testPkAvailableOnCreateIndexEvent() {
        var fireEventFuture = new CompletableFuture<Void>();

        manager.listen(CatalogEvent.INDEX_CREATE, fromConsumer(fireEventFuture, (CreateIndexEventParameters parameters) -> {
            assertEquals(pkIndexName(TABLE_NAME), parameters.indexDescriptor().name());
            assertEquals(CatalogIndexDescriptorType.HASH, parameters.indexDescriptor().indexType());
            assertEquals(AVAILABLE, parameters.indexDescriptor().status());
            assertTrue(parameters.indexDescriptor().unique());
            assertTrue(parameters.indexDescriptor().isCreatedWithTable());
        }));

        createSomeTable(TABLE_NAME);

        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    public void testCreateIndexWithAlreadyExistingName() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));
        tryApplyAndExpectApplied(simpleIndex());

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("VAL"))),
                willThrowFast(CatalogValidationException.class, "Index with name 'PUBLIC.myIndex' already exists.")
        );

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(CatalogValidationException.class, "Index with name 'PUBLIC.myIndex' already exists.")
        );
    }

    @Test
    public void testCreateIndexWithSameNameAsExistingTable() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        assertThat(
                manager.execute(createHashIndexCommand(TABLE_NAME, List.of("VAL"))),
                willThrowFast(CatalogValidationException.class, "Table with name 'PUBLIC.test_table' already exists.")
        );

        assertThat(
                manager.execute(createSortedIndexCommand(TABLE_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(CatalogValidationException.class, "Table with name 'PUBLIC.test_table' already exists.")
        );
    }

    @Test
    public void testCreateIndexWithNotExistingTable() {
        assertThat(
                manager.execute(createHashIndexCommand(TABLE_NAME, List.of("VAL"))),
                willThrowFast(CatalogValidationException.class, "Table with name 'PUBLIC.test_table' not found.")
        );

        assertThat(
                manager.execute(createSortedIndexCommand(TABLE_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(CatalogValidationException.class, "Table with name 'PUBLIC.test_table' not found.")
        );
    }

    @Test
    public void testCreateIndexWithMissingTableColumns() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("fake"))),
                willThrowFast(CatalogValidationException.class, "Column with name 'fake' not found in table 'PUBLIC.test_table'.")
        );

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, List.of("fake"), List.of(ASC_NULLS_LAST))),
                willThrowFast(CatalogValidationException.class, "Column with name 'fake' not found in table 'PUBLIC.test_table'.")
        );
    }

    @Test
    public void testCreateUniqIndexWithMissingTableColocationColumns() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

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

        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));

        int afterTableCreated = manager.latestCatalogVersion();

        tryApplyAndExpectApplied(simpleIndex());

        assertThat(manager.catalog(initialVersion).indexes(), empty());
        assertThat(
                manager.catalog(afterTableCreated).indexes(),
                hasItems(index(afterTableCreated, pkIndexName(TABLE_NAME)))
        );

        int latest = manager.latestCatalogVersion();
        assertThat(
                manager.catalog(latest).indexes(),
                hasItems(index(latest, pkIndexName(TABLE_NAME)), index(latest, INDEX_NAME))
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

        CatalogIndexDescriptor index = manager.activeCatalog(beforeRename).aliveIndex(SCHEMA_NAME, INDEX_NAME);
        assertThat(index, is(notNullValue()));

        int indexId = index.id();

        // Rename index.
        renameIndex(INDEX_NAME, INDEX_NAME_2);

        // Ensure index is available by new name.
        assertThat(manager.activeCatalog(clock.nowLong()).aliveIndex(SCHEMA_NAME, INDEX_NAME), is(nullValue()));

        index = manager.activeCatalog(clock.nowLong()).aliveIndex(SCHEMA_NAME, INDEX_NAME_2);
        assertThat(index, is(notNullValue()));
        assertThat(index.id(), is(indexId));
        assertThat(index.name(), is(INDEX_NAME_2));

        // Ensure renamed index is available for historical queries.
        CatalogIndexDescriptor oldDescriptor = manager.activeCatalog(beforeRename).aliveIndex(SCHEMA_NAME, INDEX_NAME);
        assertThat(oldDescriptor, is(notNullValue()));
        assertThat(oldDescriptor.id(), is(indexId));
        assertThat(manager.activeCatalog(beforeRename).aliveIndex(SCHEMA_NAME, INDEX_NAME_2), is(nullValue()));

        // Ensure can create new index with same name.
        createSomeIndex(TABLE_NAME, INDEX_NAME);

        index = manager.activeCatalog(clock.nowLong()).aliveIndex(SCHEMA_NAME, INDEX_NAME);
        assertThat(index, is(notNullValue()));
        assertThat(index.id(), not(indexId));
    }

    @Test
    public void testRenamePkIndex() {
        createSomeTable(TABLE_NAME);

        Catalog catalog = manager.activeCatalog(clock.nowLong());

        CatalogTableDescriptor table = catalog.table(SCHEMA_NAME, TABLE_NAME);
        assertThat(table, is(notNullValue()));
        assertThat(catalog.aliveIndex(SCHEMA_NAME, pkIndexName(TABLE_NAME)), is(notNullValue()));

        int primaryKeyIndexId = table.primaryKeyIndexId();

        // Rename index.
        renameIndex(pkIndexName(TABLE_NAME), INDEX_NAME);

        catalog = manager.activeCatalog(clock.nowLong());

        CatalogIndexDescriptor index = catalog.aliveIndex(SCHEMA_NAME, INDEX_NAME);
        assertThat(index, is(notNullValue()));
        assertThat(index.id(), is(primaryKeyIndexId));
        assertThat(index.name(), is(INDEX_NAME));

        assertThat(catalog.aliveIndex(SCHEMA_NAME, pkIndexName(TABLE_NAME)), is(nullValue()));
    }

    @Test
    public void testRenameNonExistingIndex() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(RenameIndexCommand.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).newIndexName("TEST").build()),
                willThrowFast(CatalogValidationException.class)
        );
    }

    private @Nullable CatalogIndexDescriptor index(int catalogVersion, String indexName) {
        return manager.catalog(catalogVersion).aliveIndex(SCHEMA_NAME, indexName);
    }

    private int indexId(String indexName) {
        Catalog catalog = manager.activeCatalog(clock.nowLong());
        CatalogIndexDescriptor index = catalog.aliveIndex(SCHEMA_NAME, indexName);

        assertNotNull(index, indexName);

        return index.id();
    }

    private List<Integer> tableIndexIds(int catalogVersion, int tableId) {
        return manager.catalog(catalogVersion)
                .indexes(tableId)
                .stream()
                .map(CatalogObjectDescriptor::id)
                .collect(toList());
    }

    private int tableId(String tableName) {
        Catalog catalog = manager.activeCatalog(clock.nowLong());
        CatalogTableDescriptor table = catalog.table(SCHEMA_NAME, tableName);

        assertNotNull(table, tableName);

        return table.id();
    }

    private void createSomeIndex(String tableName, String indexName) {
        tryApplyAndExpectApplied(createHashIndexCommand(tableName, indexName, false, List.of("key1")));
    }

    private void createSomeSortedIndex(String tableName, String indexName) {
        CatalogCommand newSortedIndexCommand = createSortedIndexCommand(
                SCHEMA_NAME, tableName, indexName, false, List.of("key1"), List.of(ASC_NULLS_LAST));

        tryApplyAndExpectApplied(newSortedIndexCommand);
    }

    private void renameIndex(String indexName, String newIndexName) {
        tryApplyAndExpectApplied(renameIndexCommand(indexName, newIndexName));
    }

    private void createTableWithIndex(String tableName, String indexName) {
        createSomeTable(tableName);
        createSomeIndex(tableName, indexName);

        int indexId = indexId(indexName);

        rollIndexStatusTo(AVAILABLE, indexId);
    }
}
