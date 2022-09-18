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

import static org.apache.ignite.internal.schema.SchemaUtils.canonicalName;
import static org.apache.ignite.internal.util.IgniteObjectName.parseCanonicalName;
import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ColumnDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.EntryCountBudgetConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexChange;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexView;
import org.apache.ignite.configuration.schemas.table.LogStorageBudgetConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexChange;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableIndexConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.schemas.table.UnlimitedBudgetConfigurationSchema;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Table;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.TableNotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test class to verify {@link IndexManager}.
 */
@ExtendWith(ConfigurationExtension.class)
public class IndexManagerTest {
    @InjectConfiguration(polymorphicExtensions = {
            HashIndexConfigurationSchema.class,
            SortedIndexConfigurationSchema.class,
            UnknownDataStorageConfigurationSchema.class,
            ConstantValueDefaultConfigurationSchema.class,
            FunctionCallDefaultConfigurationSchema.class,
            NullValueDefaultConfigurationSchema.class,
            UnlimitedBudgetConfigurationSchema.class,
            EntryCountBudgetConfigurationSchema.class
    })
    TablesConfiguration tablesConfig;

    @Test
    void configurationChangedWhenCreateIsInvoked() {
        var tableManagerMock = mock(TableManager.class);
        var canonicalName = canonicalName("sName", "tName");

        TableChange tableChange = createNode(TableConfigurationSchema.class);

        tableChange.changeColumns(columnListChange -> {
            columnListChange.create("c1", columnChange -> {});
            columnListChange.create("c2", columnChange -> {});
        });

        var successfulCompletion = new RuntimeException("This is expected");

        TableImpl tbl = mock(TableImpl.class);

        when(tableManagerMock.tableAsyncInternal(eq(parseCanonicalName(canonicalName))))
                .thenReturn(CompletableFuture.completedFuture(tbl));

        UUID tableId = UUID.randomUUID();

        mockColumns(tbl, List.of("c1", "c2"));
        when(tableManagerMock.tableImpl(any())).thenReturn(tbl);
        when(tbl.tableId()).thenReturn(tableId);

        when(tableManagerMock.alterTableAsync(any(), any())).thenAnswer(answer -> {
            Consumer<TableChange> changeConsumer = answer.getArgument(1);

            try {
                changeConsumer.accept(tableChange);
            } catch (Throwable th) {
                return CompletableFuture.failedFuture(th);
            }

            // return failed future here to prevent index manager to further process the
            // create request, because after returned future is competed, index manager expects
            // to all configuration listeners to be executed, but no one have configured them
            // for this test
            return CompletableFuture.failedFuture(successfulCompletion);
        });

        var indexManager = new IndexManager(tableManagerMock, tablesConfig);

        indexManager.start();

        try {
            indexManager.createIndexAsync("sName", "idx", "tName", true, indexChange -> {
                SortedIndexChange sortedIndexChange = indexChange.convert(SortedIndexChange.class);

                sortedIndexChange.changeColumns(columns -> {
                    columns.create("c1", columnChange -> columnChange.changeAsc(true));
                    columns.create("c2", columnChange -> columnChange.changeAsc(false));
                });

                sortedIndexChange.changeTableId(tableId);
            }).join();
        } catch (CompletionException ex) {
            if (ex.getCause() != successfulCompletion) {
                throw ex;
            }
        }

        String awaitIdxName = parseCanonicalName(canonicalName("sName", "idx"));

        var expected = List.of(
                Map.of(
                        "columns", List.of(
                                Map.of(
                                        "asc", true,
                                        "name", "c1"
                                ),
                                Map.of(
                                        "asc", false,
                                        "name", "c2"
                                )
                        ),
                        "name", awaitIdxName,
                        "type", "SORTED",
                        "uniq", false,
                        "tableId", tableId
                )
        );

        assertSameObjects(expected, toMap(tablesConfig.indexes().value()));
    }

    @Test
    public void createIndexForNonExistingTable() {
        var tableManagerMock = mock(TableManager.class);
        var canonicalName = parseCanonicalName(canonicalName("sName", "tName"));

        when(tableManagerMock.tableAsyncInternal(eq(canonicalName))).thenReturn(CompletableFuture.completedFuture(null));

        var indexManager = new IndexManager(tableManagerMock, tablesConfig);
        indexManager.start();

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "idx", "tName", true, indexChange -> {/* doesn't matter */}).join()
        );

        assertThat(completionException.getCause(), instanceOf(TableNotFoundException.class));
        assertThat(completionException.getCause().getMessage(), containsString(canonicalName));
    }

    @Test
    public void createIndexWithEmptyName() {
        var tableManagerMock = mock(TableManager.class);

        var indexManager = new IndexManager(tableManagerMock, tablesConfig);
        indexManager.start();

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "", "tName", true, indexChange -> {/* doesn't matter */}).join()
        );

        assertThat(completionException.getCause(), instanceOf(IgniteInternalException.class));
        assertThat(
                ((IgniteInternalException) completionException.getCause()).code(),
                equalTo(ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR)
        );
    }

    @Test
    public void createIndexWithEmptyColumnList() {
        var tableManagerMock = mock(TableManager.class);
        var tableMock = mock(TableImpl.class);
        TableChange tableChange = createNode(TableConfigurationSchema.class);

        when(tableManagerMock.tableAsyncInternal(any())).thenReturn(CompletableFuture.completedFuture(tableMock));
        when(tableManagerMock.alterTableAsync(any(), any())).thenAnswer(answer -> {
            Consumer<TableChange> changeConsumer = answer.getArgument(1);

            Throwable exception = null;
            try {
                changeConsumer.accept(tableChange);
            } catch (Exception ex) {
                exception = ex;
            }

            if (exception == null) {
                // consumer expected to fail on validation
                exception = new AssertionError();
            }

            return CompletableFuture.failedFuture(exception);
        });

        var indexManager = new IndexManager(tableManagerMock, tablesConfig);
        indexManager.start();

        mockColumns(tableMock, List.of());
        when(tableMock.tableId()).thenReturn(UUID.randomUUID());

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "idx", "tName", true,
                        indexChange -> indexChange.convert(HashIndexChange.class).changeColumnNames()
                                .changeTableId(UUID.randomUUID())).join()
        );

        assertThat(completionException.getCause(), instanceOf(IgniteInternalException.class));
        assertThat(
                ((IgniteInternalException) completionException.getCause()).code(),
                equalTo(ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR)
        );
    }

    @Test
    public void IndexValidatorImplcreateIndexForNonExistingColumn() {
        var tableManagerMock = mock(TableManager.class);
        var canonicalName = parseCanonicalName(canonicalName("sName", "tName"));
        var tableMock = mock(TableImpl.class);
        TableChange tableChange = createNode(TableConfigurationSchema.class);

        when(tableManagerMock.tableAsyncInternal(eq(canonicalName))).thenReturn(CompletableFuture.completedFuture(tableMock));
        when(tableManagerMock.alterTableAsync(any(), any())).thenAnswer(answer -> {
            Consumer<TableChange> changeConsumer = answer.getArgument(1);

            Throwable exception = null;
            try {
                changeConsumer.accept(tableChange);
            } catch (Exception ex) {
                exception = ex;
            }

            if (exception == null) {
                // consumer expected to fail on validation
                exception = new AssertionError();
            }

            return CompletableFuture.failedFuture(exception);
        });

        var indexManager = new IndexManager(tableManagerMock, tablesConfig);
        indexManager.start();

        mockColumns(tableMock, List.of());
        when(tableMock.tableId()).thenReturn(UUID.randomUUID());

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "idx", "tName", true,
                        indexChange ->
                                indexChange.convert(HashIndexChange.class).changeColumnNames("nonExistingColumn")
                                        .changeTableId(UUID.randomUUID())).join()
        );

        assertThat(completionException.getCause(), instanceOf(IgniteInternalException.class));
        assertThat(
                ((IgniteInternalException) completionException.getCause()).code(),
                equalTo(Table.COLUMN_NOT_FOUND_ERR)
        );
    }

    @Test
    public void dropNonExistingIndex() {
        var tableManagerMock = mock(TableManager.class);
        var tblsConfigMock = mock(TablesConfiguration.class);

        NamedConfigurationTree cfgTree = mock(NamedConfigurationTree.class);
        when(tblsConfigMock.indexes()).thenReturn(cfgTree);

        when(cfgTree.get(any())).thenReturn(null);

        var indexManager = new IndexManager(tableManagerMock, tblsConfigMock);

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.dropIndexAsync("sName", "nonExisting", true).join()
        );

        assertThat(completionException.getCause(), instanceOf(IndexNotFoundException.class));
        assertThat(
                ((IndexNotFoundException) completionException.getCause()).code(),
                equalTo(ErrorGroups.Index.INDEX_NOT_FOUND_ERR)
        );
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void eventIsFiredWhenIndexCreated() throws ExecutionException, InterruptedException {
        var tableManagerMock = mock(TableManager.class);
        var indexId = UUID.randomUUID();
        var tableId = UUID.randomUUID();
        var indexName = "idxName";
        var canonicalName = canonicalName("sName", "tName");

        TablesConfiguration tablesCfg = mock(TablesConfiguration.class);
        NamedConfigurationTree cfgTree = mock(NamedConfigurationTree.class);

        when(tablesCfg.indexes()).thenReturn(cfgTree);

        var indexManager = new IndexManager(tableManagerMock, tablesCfg);

        TableImpl tblImp = mock(TableImpl.class);

        when(tableManagerMock.tableImpl(any())).thenReturn(tblImp);
        when(tblImp.tableId()).thenReturn(tableId);

        AtomicReference<IndexEventParameters> holder = new AtomicReference<>();

        indexManager.listen(IndexEvent.CREATE, (param, th) -> {
            holder.set(param);

            return CompletableFuture.completedFuture(true);
        });

        indexManager.start();

        ConfigurationNotificationEvent<TableIndexView> evt0 =
                createConfigurationEventIndexAddedMock(indexId, indexName, tableId);

        indexManager.onIndexCreate(evt0);

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().index().id(), equalTo(indexId));
        assertThat(holder.get().index().tableId(), equalTo(tableId));
        assertThat(holder.get().index().name(), equalTo(indexName));
    }

    private void mockColumns(TableImpl tbl, List<String> columns) {
        SchemaRegistry schView = mock(SchemaRegistry.class);
        when(tbl.schemaView()).thenReturn(schView);
        SchemaDescriptor schDesc = mock(SchemaDescriptor.class);
        when(schView.schema()).thenReturn(schDesc);
        when(schDesc.columnNames()).thenReturn(columns);
    }

    @SuppressWarnings("unchecked")
    private ConfigurationNotificationEvent<TableIndexView> createConfigurationEventIndexAddedMock(
            UUID indexId,
            String canonicalIndexName,
            UUID tableId
    ) {
        ConfigurationValue<UUID> confValueMock = mock(ConfigurationValue.class);
        when(confValueMock.value()).thenReturn(tableId).getMock();

        TableIndexConfiguration extendedConfMock = mock(TableIndexConfiguration.class);
        when(extendedConfMock.tableId()).thenReturn(confValueMock);

        HashIndexView indexViewMock = mock(HashIndexView.class);
        when(indexViewMock.id()).thenReturn(indexId);
        when(indexViewMock.tableId()).thenReturn(tableId);
        when(indexViewMock.name()).thenReturn(canonicalIndexName);
        when(indexViewMock.columnNames()).thenReturn(new String[] {"c1", "c2"});

        ConfigurationNotificationEvent<TableIndexView> configurationEventMock = mock(ConfigurationNotificationEvent.class);
        when(configurationEventMock.config(any())).thenReturn(extendedConfMock);
        when(configurationEventMock.newValue()).thenReturn(indexViewMock);

        return configurationEventMock;
    }

    /** Creates configuration node for given *SchemaConfiguration class. */
    @SuppressWarnings({"unchecked", "SameParameterValue"})
    private <T> T createNode(Class<?> cls) {
        var cgen = new ConfigurationAsmGenerator();

        Map<Class<?>, Set<Class<?>>> polymorphicExtensions = Map.of(
                DataStorageConfigurationSchema.class,
                Set.of(
                        UnknownDataStorageConfigurationSchema.class
                ),
                TableIndexConfigurationSchema.class,
                Set.of(
                        HashIndexConfigurationSchema.class,
                        SortedIndexConfigurationSchema.class
                ),
                ColumnDefaultConfigurationSchema.class,
                Set.of(
                        ConstantValueDefaultConfigurationSchema.class,
                        NullValueDefaultConfigurationSchema.class,
                        FunctionCallDefaultConfigurationSchema.class
                ),
                LogStorageBudgetConfigurationSchema.class,
                Set.of(
                        UnlimitedBudgetConfigurationSchema.class,
                        EntryCountBudgetConfigurationSchema.class
                )
        );

        cgen.compileRootSchema(TablesConfiguration.KEY.schemaClass(), Map.of(), polymorphicExtensions);

        return (T) cgen.instantiateNode(cls);
    }

    private static Object toMap(Object obj) {
        assert obj instanceof TraversableTreeNode;

        return ((TraversableTreeNode) obj).accept(null, new ConverterToMapVisitor(false));
    }

    private static void assertSameObjects(Object expected, Object actual) {
        try {
            contentEquals(expected, actual);
        } catch (ObjectsNotEqualException ex) {
            fail(
                    format(
                            "Objects are not equal at position {}:\n\texpected={}\n\tactual={}",
                            String.join(".", ex.path), ex.o1, ex.o2)
            );
        }
    }

    private static void contentEquals(Object o1, Object o2) {
        if (o1 instanceof Map && o2 instanceof Map) {
            var m1 = (Map<?, ?>) o1;
            var m2 = (Map<?, ?>) o2;

            if (m1.size() != m2.size()) {
                throw new ObjectsNotEqualException(m1, m2);
            }

            for (Map.Entry<?, ?> entry : m1.entrySet()) {
                var v1 = entry.getValue();
                var v2 = m2.get(entry.getKey());

                try {
                    contentEquals(v1, v2);
                } catch (ObjectsNotEqualException ex) {
                    ex.path.add(0, entry.getKey().toString());

                    throw ex;
                }
            }
        } else if (o1 instanceof List && o2 instanceof List) {
            var l1 = (List<?>) o1;
            var l2 = (List<?>) o2;

            if (l1.size() != l2.size()) {
                throw new ObjectsNotEqualException(l1, l2);
            }

            var it1 = l1.iterator();
            var it2 = l2.iterator();

            int idx = 0;
            while (it1.hasNext()) {
                var v1 = it1.next();
                var v2 = it2.next();

                try {
                    contentEquals(v1, v2);
                } catch (ObjectsNotEqualException ex) {
                    ex.path.add(0, "[" + idx + ']');

                    throw ex;
                }
            }
        } else if (!Objects.equals(o1, o2)) {
            throw new ObjectsNotEqualException(o1, o2);
        }
    }

    static class ObjectsNotEqualException extends RuntimeException {
        private final Object o1;
        private final Object o2;

        private final List<String> path = new ArrayList<>();

        public ObjectsNotEqualException(Object o1, Object o2) {
            super(null, null, false, false);
            this.o1 = o1;
            this.o2 = o2;
        }
    }
}
