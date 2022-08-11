/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ColumnDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexChange;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexView;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexChange;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Table;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.TableNotFoundException;
import org.junit.jupiter.api.Test;

/**
 * Test class to verify {@link IndexManager}.
 */
public class IndexManagerTest {
    @Test
    void indexManagerSubscribedToIdxConfigurationChangesOnStart() {
        var indicesConfigurationChangeSubscription = new AtomicReference<>();

        var indexManager = new IndexManager(mock(TableManager.class), indicesConfigurationChangeSubscription::set);

        indexManager.start();

        assertThat(indicesConfigurationChangeSubscription.get(), notNullValue());
    }

    @Test
    void configurationChangedWhenCreateIsInvoked() {
        var tableManagerMock = mock(TableManager.class);
        var canonicalName = "sName.tName";

        TableChange tableChange = createNode(TableConfigurationSchema.class);

        tableChange.changeColumns(columnListChange -> {
            columnListChange.create("c1", columnChange -> {});
            columnListChange.create("c2", columnChange -> {});
        });

        var successfulCompletion = new RuntimeException("This is expected");

        when(tableManagerMock.tableAsyncInternal(eq(canonicalName))).thenReturn(CompletableFuture.completedFuture(mock(TableImpl.class)));
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

        var indexManager = new IndexManager(tableManagerMock, listener -> {});

        try {
            indexManager.createIndexAsync("sName", "idx", "tName", indexChange -> {
                var sortedIndexChange = indexChange.convert(SortedIndexChange.class);

                sortedIndexChange.changeColumns(columns -> {
                    columns.create("c1", columnChange -> columnChange.changeAsc(true));
                    columns.create("c2", columnChange -> columnChange.changeAsc(false));
                });
            }).join();
        } catch (CompletionException ex) {
            if (ex.getCause() != successfulCompletion) {
                throw ex;
            }
        }

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
                        "name", "idx",
                        "type", "SORTED",
                        "uniq", false
                )
        );

        assertSameObjects(expected, toMap(tableChange.indices()));
    }

    @Test
    public void createIndexForNonExistingTable() {
        var tableManagerMock = mock(TableManager.class);
        var canonicalName = "sName.tName";

        when(tableManagerMock.tableAsyncInternal(eq(canonicalName))).thenReturn(CompletableFuture.completedFuture(null));

        var indexManager = new IndexManager(tableManagerMock, listener -> {});

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "idx", "tName", indexChange -> {/* doesn't matter */}).join()
        );

        assertThat(completionException.getCause(), instanceOf(TableNotFoundException.class));
        assertThat(completionException.getCause().getMessage(), containsString(canonicalName));
    }

    @Test
    public void createIndexWithEmptyName() {
        var tableManagerMock = mock(TableManager.class);

        var indexManager = new IndexManager(tableManagerMock, listener -> {});

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "", "tName", indexChange -> {/* doesn't matter */}).join()
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

        var indexManager = new IndexManager(tableManagerMock, listener -> {});

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "idx", "tName",
                        indexChange -> indexChange.convert(HashIndexChange.class).changeColumnNames()).join()
        );

        assertThat(completionException.getCause(), instanceOf(IgniteInternalException.class));
        assertThat(
                ((IgniteInternalException) completionException.getCause()).code(),
                equalTo(ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR)
        );
    }

    @Test
    public void createIndexForNonExistingColumn() {
        var tableManagerMock = mock(TableManager.class);
        var canonicalName = "sName.tName";
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

        var indexManager = new IndexManager(tableManagerMock, listener -> {});

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "idx", "tName",
                        indexChange -> indexChange.convert(HashIndexChange.class).changeColumnNames("nonExistingColumn")).join()
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

        var indexManager = new IndexManager(tableManagerMock, listener -> {});

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.dropIndexAsync("sName", "nonExisting").join()
        );

        assertThat(completionException.getCause(), instanceOf(IndexNotFoundException.class));
        assertThat(
                ((IndexNotFoundException) completionException.getCause()).code(),
                equalTo(ErrorGroups.Index.INDEX_NOT_FOUND_ERR)
        );
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void eventIsFiredWhenIndexCreated() {
        var tableManagerMock = mock(TableManager.class);
        var indexId = UUID.randomUUID();
        var tableId = UUID.randomUUID();
        var indexName = "idxName";

        Consumer<ConfigurationNamedListListener<TableIndexView>> listenerConsumer = listener -> {
            listener.onCreate(createConfigurationEventIndexAddedMock(indexId, tableId, indexName));
        };

        var indexManager = new IndexManager(tableManagerMock, listenerConsumer);

        AtomicReference<IndexEventParameters> holder = new AtomicReference<>();

        indexManager.listen(IndexEvent.CREATE, (param, th) -> {
            holder.set(param);

            return CompletableFuture.completedFuture(true);
        });

        indexManager.start();

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().index().id(), equalTo(indexId));
        assertThat(holder.get().index().tableId(), equalTo(tableId));
        assertThat(holder.get().index().name(), equalTo("PUBLIC." + indexName));
    }

    @SuppressWarnings("unchecked")
    private ConfigurationNotificationEvent<TableIndexView> createConfigurationEventIndexAddedMock(
            UUID indexId,
            UUID tableId,
            String canonicalIndexName
    ) {
        ConfigurationValue<UUID> confValueMock = mock(ConfigurationValue.class);
        when(confValueMock.value()).thenReturn(tableId).getMock();

        ExtendedTableConfiguration extendedConfMock = mock(ExtendedTableConfiguration.class);
        when(extendedConfMock.id()).thenReturn(confValueMock);

        HashIndexView indexViewMock = mock(HashIndexView.class);
        when(indexViewMock.id()).thenReturn(indexId);
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