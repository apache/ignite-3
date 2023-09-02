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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.ExtendedTableChange;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexChange;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.index.IndexBuildController;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test class to verify {@link IndexManager}.
 */
@ExtendWith(ConfigurationExtension.class)
public class IndexManagerTest extends BaseIgniteAbstractTest {
    @InjectConfiguration(
            "mock.tables.tName {"
                    + "id: 1, "
                    + "columns.c1 {type.type: STRING}, "
                    + "columns.c2 {type.type: STRING}, "
                    + "primaryKey {columns: [c1], colocationColumns: [c1]}"
                    + "}"
    )
    private TablesConfiguration tablesConfig;

    private IndexManager indexManager;

    @BeforeEach
    public void setUp() {
        TableManager tableManagerMock = mock(TableManager.class);

        when(tableManagerMock.tableAsync(anyLong(), anyInt())).thenAnswer(inv -> {
            InternalTable tbl = mock(InternalTable.class);

            doReturn(inv.getArgument(1)).when(tbl).tableId();

            return completedFuture(new TableImpl(tbl, new HeapLockManager()));
        });

        when(tableManagerMock.getTable(anyInt())).thenAnswer(inv -> {
            InternalTable tbl = mock(InternalTable.class);

            doReturn(inv.getArgument(0)).when(tbl).tableId();

            return new TableImpl(tbl, new HeapLockManager());
        });

        when(tableManagerMock.localPartitionSetAsync(anyLong(), anyInt())).thenReturn(completedFuture(PartitionSet.EMPTY_SET));

        SchemaManager schManager = mock(SchemaManager.class);

        when(schManager.schemaRegistry(anyLong(), anyInt())).thenReturn(completedFuture(null));

        indexManager = new IndexManager(tablesConfig, schManager, tableManagerMock, mock(IndexBuildController.class));
        indexManager.start();

        assertThat(
                tablesConfig.tables().get("tName")
                        .change(tableChange -> ((ExtendedTableChange) tableChange).changeSchemaId(1)),
                willCompleteSuccessfully()
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        if (indexManager != null) {
            indexManager.stop();
        }
    }

    @Test
    void configurationChangedWhenCreateIsInvoked() {
        String indexName = "idx";

        assertThat(indexManager.createIndexAsync("sName", indexName, "tName", true, indexChange -> {
            SortedIndexChange sortedIndexChange = indexChange.convert(SortedIndexChange.class);

            sortedIndexChange.changeColumns(columns -> {
                columns.create("c1", columnChange -> columnChange.changeAsc(true));
                columns.create("c2", columnChange -> columnChange.changeAsc(false));
            });

            sortedIndexChange.changeTableId(tableId());
        }), willCompleteSuccessfully());

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
                        "name", indexName,
                        "type", "SORTED",
                        "uniq", false,
                        "tableId", tableId(),
                        "id", 1
                )
        );

        assertSameObjects(expected, toMap(tablesConfig.indexes().value()));
    }

    @Test
    public void createIndexWithEmptyName() {
        assertThat(
                indexManager.createIndexAsync("sName", "", "tName", true, indexChange -> {/* doesn't matter */}),
                willThrowFast(IgniteInternalException.class, "Index name should be at least 1 character long")
        );
    }

    @Test
    public void dropNonExistingIndex() {
        assertThat(
                indexManager.dropIndexAsync("sName", "nonExisting", true),
                willThrowFast(IndexNotFoundException.class, "Index does not exist [name=\"sName\".\"nonExisting\"]")
        );
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void eventIsFiredWhenIndexCreated() {
        String indexName = "idx";

        AtomicReference<IndexEventParameters> holder = new AtomicReference<>();

        indexManager.listen(IndexEvent.CREATE, (param, th) -> {
            holder.set(param);

            return completedFuture(true);
        });

        indexManager.listen(IndexEvent.DROP, (param, th) -> {
            holder.set(param);

            return completedFuture(true);
        });

        assertThat(indexManager.createIndexAsync("sName", indexName, "tName", true, indexChange -> {
            SortedIndexChange sortedIndexChange = indexChange.convert(SortedIndexChange.class);

            sortedIndexChange.changeColumns(columns -> {
                columns.create("c2", columnChange -> columnChange.changeAsc(true));
            });

            sortedIndexChange.changeTableId(tableId());
        }), willCompleteSuccessfully());

        List<Integer> indexIds = tablesConfig.indexes().value().stream()
                .map(TableIndexView::id)
                .collect(toList());

        assertThat(indexIds, hasSize(1));

        int indexId = indexIds.get(0);

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().indexId(), equalTo(indexId));
        assertThat(holder.get().tableId(), equalTo(tableId()));
        assertThat(holder.get().indexDescriptor().name(), equalTo(indexName));

        assertThat(indexManager.dropIndexAsync("sName", indexName, true), willCompleteSuccessfully());

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().indexId(), equalTo(indexId));
    }

    private static Object toMap(Object obj) {
        assert obj instanceof TraversableTreeNode;

        return ((TraversableTreeNode) obj).accept(
                null,
                null,
                ConverterToMapVisitor.builder()
                        .includeInternal(false)
                        .build()
        );
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

        ObjectsNotEqualException(Object o1, Object o2) {
            super(null, null, false, false);
            this.o1 = o1;
            this.o2 = o2;
        }
    }

    private int tableId() {
        return tablesConfig.tables().get("tName").id().value();
    }
}
