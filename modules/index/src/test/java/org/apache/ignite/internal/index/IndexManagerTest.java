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

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.schema.SchemaUtils.canonicalName;
import static org.apache.ignite.internal.util.IgniteObjectName.parseCanonicalName;
import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.EntryCountBudgetConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexChange;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.IndexValidator;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexChange;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.schemas.table.UnlimitedBudgetConfigurationSchema;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.schema.configuration.IndexValidatorImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.TableNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test class to verify {@link IndexManager}.
 */
@ExtendWith(ConfigurationExtension.class)
public class IndexManagerTest {
    /** Configuration registry with one table for each test. */
    private ConfigurationRegistry confRegistry;

    /**
     * Prepare configuration registry for test.
     */
    @BeforeEach
    public void createRegistry() {
        confRegistry = new ConfigurationRegistry(
                List.of(TablesConfiguration.KEY),
                Map.of(IndexValidator.class, Set.of(IndexValidatorImpl.INSTANCE)),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of(
                        HashIndexConfigurationSchema.class,
                        SortedIndexConfigurationSchema.class,

                        UnknownDataStorageConfigurationSchema.class,
                        ConstantValueDefaultConfigurationSchema.class,
                        FunctionCallDefaultConfigurationSchema.class,
                        NullValueDefaultConfigurationSchema.class,
                        UnlimitedBudgetConfigurationSchema.class,
                        EntryCountBudgetConfigurationSchema.class
                )
        );

        confRegistry.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        confRegistry.stop();
    }

    @Test
    void configurationChangedWhenCreateIsInvoked() throws Exception {
        TablesConfiguration tablesConfig = confRegistry.getConfiguration(TablesConfiguration.KEY);

        var indexManager = new IndexManager(tablesConfig);
        indexManager.start();

        tablesConfig.tables().change(tableChange -> tableChange.create("SNAME.TNAME", chg -> {
            chg.changeColumns(cols ->
                    cols.create("c1", col -> col.changeType(t -> t.changeType("STRING")))
                            .create("c2", col -> col.changeType(t -> t.changeType("STRING"))));

            chg.changePrimaryKey(pk -> pk.changeColumns("c1").changeColocationColumns("c1"));
        })).get();

        NamedConfigurationTree<TableConfiguration, TableView, TableChange> cfg0 = tablesConfig.tables();

        List<UUID> ids = ((NamedListConfiguration<TableConfiguration, ?, ?>) cfg0).internalIds();

        assertEquals(1, ids.size());

        UUID tableId = ids.get(0);

        indexManager.createIndexAsync("sName", "idx", "tName", true, indexChange -> {
            SortedIndexChange sortedIndexChange = indexChange.convert(SortedIndexChange.class);

            sortedIndexChange.changeColumns(columns -> {
                columns.create("c1", columnChange -> columnChange.changeAsc(true));
                columns.create("c2", columnChange -> columnChange.changeAsc(false));
            });

            sortedIndexChange.changeTableId(tableId);
        }).join();


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
                        "tableId", tableId.toString()
                )
        );

        assertSameObjects(expected, toMap(tablesConfig.indexes().value()));
    }

    @Test
    public void createIndexForNonExistingTable() {
        var canonicalName = parseCanonicalName(canonicalName("sName", "tName"));

        TablesConfiguration tablesConfig = confRegistry.getConfiguration(TablesConfiguration.KEY);

        var indexManager = new IndexManager(tablesConfig);
        indexManager.start();

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "idx", "tName", true, indexChange -> {/* doesn't matter */}).join()
        );

        assertTrue(IgniteTestUtils.hasCause(completionException, TableNotFoundException.class,
                "The table does not exist [name=SNAME.TNAME]"));
    }

    @Test
    public void createIndexWithEmptyName() {
        TablesConfiguration tablesConfig = confRegistry.getConfiguration(TablesConfiguration.KEY);

        var indexManager = new IndexManager(tablesConfig);
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
    public void createIndexWithEmptyColumnList() throws Exception {
        TablesConfiguration tablesConfig = confRegistry.getConfiguration(TablesConfiguration.KEY);

        var indexManager = new IndexManager(tablesConfig);
        indexManager.start();

        tablesConfig.tables().change(tableChange -> tableChange.create("SNAME.TNAME", chg -> {
            chg.changeColumns(cols ->
                    cols.create("id", col -> col.changeType(t -> t.changeType("STRING"))));

            chg.changePrimaryKey(pk -> pk.changeColumns("id").changeColocationColumns("id"));
        })).get();

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "idx", "tName", true,
                        indexChange -> indexChange.convert(HashIndexChange.class).changeColumnNames()
                                .changeTableId(UUID.randomUUID())).join()
        );

        assertTrue(IgniteTestUtils.hasCause(completionException, ConfigurationValidationException.class,
                "Index must include at least one column"));

        assertThat(
                (IgniteTestUtils.cause(completionException, ConfigurationValidationException.class)).getMessage(),
                containsString(ErrorGroups.Index.INDEX_ERR_GROUP.name())
        );
    }

    @Test
    public void createIndexForNonExistingColumn() throws Exception {
        TablesConfiguration tablesConfig = confRegistry.getConfiguration(TablesConfiguration.KEY);

        var indexManager = new IndexManager(tablesConfig);
        indexManager.start();

        tablesConfig.tables().change(tableChange -> tableChange.create("SNAME.TNAME", chg -> {
            chg.changeColumns(cols ->
                    cols.create("id", col -> col.changeType(t -> t.changeType("STRING"))));

            chg.changePrimaryKey(pk -> pk.changeColumns("id").changeColocationColumns("id"));
        })).get();

        CompletionException completionException = assertThrows(
                CompletionException.class,
                () -> indexManager.createIndexAsync("sName", "idx", "tName", true,
                        indexChange ->
                                indexChange.convert(HashIndexChange.class).changeColumnNames("nonExistingColumn")
                                        .changeTableId(UUID.randomUUID())).join()
        );

        assertTrue(IgniteTestUtils.hasCause(completionException, ConfigurationValidationException.class,
                "Columns don't exist"));
    }

    @Test
    public void dropNonExistingIndex() throws Exception {
        TablesConfiguration tablesConfig = confRegistry.getConfiguration(TablesConfiguration.KEY);

        var indexManager = new IndexManager(tablesConfig);
        indexManager.start();

        tablesConfig.tables().change(tableChange -> tableChange.create("SNAME.TNAME", chg -> {
            chg.changeColumns(cols ->
                    cols.create("id", col -> col.changeType(t -> t.changeType("STRING"))));

            chg.changePrimaryKey(pk -> pk.changeColumns("id").changeColocationColumns("id"));
        })).get();

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
    public void eventIsFiredWhenIndexCreated() throws Exception {
        var indexName = "SCHEMA.INDEXNAME";

        TablesConfiguration tablesConfig = confRegistry.getConfiguration(TablesConfiguration.KEY);

        var indexManager = new IndexManager(tablesConfig);
        indexManager.start();

        tablesConfig.tables().change(tableChange -> tableChange.create("SCHEMA.TNAME", chg -> {
            chg.changeColumns(cols ->
                    cols.create("id", col -> col.changeType(t -> t.changeType("STRING"))));

            chg.changePrimaryKey(pk -> pk.changeColumns("id").changeColocationColumns("id"));
        })).get();

        AtomicReference<IndexEventParameters> holder = new AtomicReference<>();

        indexManager.listen(IndexEvent.CREATE, (param, th) -> {
            holder.set(param);

            return CompletableFuture.completedFuture(true);
        });

        indexManager.start();

        NamedConfigurationTree<TableConfiguration, TableView, TableChange> cfg0 = tablesConfig.tables();

        List<UUID> ids = ((NamedListConfiguration<TableConfiguration, ?, ?>) cfg0).internalIds();

        assertEquals(1, ids.size());

        UUID tableId = ids.get(0);

        indexManager.createIndexAsync("SCHEMA", "indexName", "tName", true, indexChange -> {
            SortedIndexChange sortedIndexChange = indexChange.convert(SortedIndexChange.class);

            sortedIndexChange.changeColumns(columns -> {
                columns.create("id", columnChange -> columnChange.changeAsc(true));
            });

            sortedIndexChange.changeTableId(tableId);
        }).join();

        ids = ((NamedListConfiguration<TableIndexConfiguration, ?, ?>) tablesConfig.indexes()).internalIds();

        assertEquals(1, ids.size());

        UUID indexId = ids.get(0);

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().index().id(), equalTo(indexId));
        assertThat(holder.get().index().tableId(), equalTo(tableId));
        assertThat(holder.get().index().name(), equalTo(indexName));
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
