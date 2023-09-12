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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.SYSTEM_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSystemViewCommand;
import org.apache.ignite.internal.catalog.commands.CreateSystemViewCommandBuilder;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor.Type;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;


/**
 * Tests to verify system view related commands.
 */
public class CatalogSystemViewTest extends BaseCatalogManagerTest {

    @ParameterizedTest
    @EnumSource(SystemViewType.class)
    public void testCreateSystemView(SystemViewType type) {
        CreateSystemViewCommand command = CreateSystemViewCommand.builder()
                .name("view1")
                .columns(List.of(
                        ColumnParams.builder().name("col1").type(INT32).build(),
                        ColumnParams.builder().name("col2").type(STRING).build()
                ))
                .type(type)
                .build();

        assertThat(manager.execute(command), willCompleteSuccessfully());

        int catalogVersion = manager.latestCatalogVersion();

        CatalogSchemaDescriptor systemSchema = manager.schema(SYSTEM_SCHEMA_NAME, catalogVersion);
        assertNotNull(systemSchema, "systemSchema");

        CatalogSystemViewDescriptor view1 = systemSchema.systemView("view1");

        assertNotNull(view1, "view1");
        assertEquals(3L, view1.id(), "system view id");
        assertEquals("view1", view1.name());
        assertEquals(Type.SYSTEM_VIEW, view1.type(), "type");
        assertEquals(type, view1.systemViewType(), "system view type");

        List<CatalogTableColumnDescriptor> columns = view1.columns();
        assertEquals(2, columns.size(), "columns: " + columns);

        CatalogTableColumnDescriptor col1 = columns.get(0);
        assertEquals("col1", col1.name());
        assertEquals(INT32, col1.type());

        CatalogTableColumnDescriptor col2 = columns.get(1);
        assertEquals("col2", col2.name());
        assertEquals(STRING, col2.type());
    }

    @ParameterizedTest
    @EnumSource(SystemViewModification.class)
    public void testCreateSystemViewReplacesExistingViewWhenViewChanges(SystemViewModification systemViewModification) {
        CreateSystemViewCommandBuilder viewBuilder = SystemViewModification.newSystemView();

        CreateSystemViewCommand initialCommand = viewBuilder.build();

        assertThat(manager.execute(initialCommand), willCompleteSuccessfully());

        CatalogSchemaDescriptor systemSchema = manager.schema(SYSTEM_SCHEMA_NAME, manager.latestCatalogVersion());
        assertNotNull(systemSchema, "systemSchema");

        List<CatalogSystemViewDescriptor> initialViews = Arrays.stream(systemSchema.systemViews())
                .sorted(Comparator.comparing(CatalogObjectDescriptor::name)).collect(toList());

        systemViewModification.apply(viewBuilder);

        assertThat(manager.execute(viewBuilder.build()), willCompleteSuccessfully());

        CatalogSchemaDescriptor mostRecentSchema = manager.schema(SYSTEM_SCHEMA_NAME, manager.latestCatalogVersion());
        assertNotNull(mostRecentSchema, "systemSchema");

        // Retrieve the most actual system views
        List<CatalogSystemViewDescriptor> views = Arrays.stream(mostRecentSchema.systemViews())
                .sorted(Comparator.comparing(CatalogObjectDescriptor::name)).collect(toList());

        assertEquals(1, views.size());

        // System view should have been updated.
        CatalogSystemViewDescriptor view1 = views.get(0);
        assertNotEquals(view1.id(), initialViews.get(0).id(), "view id didn't change");
    }

    /**
     * System view modifications.
     */
    public enum SystemViewModification {
        CHANGE_TYPE,
        ADD_COLUMN,
        REMOVE_COLUMN,
        MODIFY_COLUMN;

        private static final List<ColumnParams> COLUMNS = List.of(
                ColumnParams.builder().name("col1").type(INT32).build(),
                ColumnParams.builder().name("col2").type(STRING).build()
        );

        static CreateSystemViewCommandBuilder newSystemView() {
            return CreateSystemViewCommand.builder()
                    .name("view1")
                    .columns(COLUMNS)
                    .type(SystemViewType.LOCAL);
        }

        void apply(CreateSystemViewCommandBuilder builder) {
            switch (this) {
                case CHANGE_TYPE: {
                    builder.type(SystemViewType.GLOBAL);
                    break;
                }
                case ADD_COLUMN: {
                    ColumnParams column = ColumnParams.builder()
                            .name("col-x")
                            .type(ColumnType.BYTE_ARRAY)
                            .build();

                    List<ColumnParams> columns = new ArrayList<>(COLUMNS);
                    columns.add(column);

                    builder.columns(columns);
                    break;
                }
                case REMOVE_COLUMN: {
                    List<ColumnParams> columns = new ArrayList<>(COLUMNS);
                    columns.remove(0);

                    builder.columns(columns);
                    break;
                }
                case MODIFY_COLUMN: {
                    ColumnParams column = ColumnParams.builder()
                            .name(COLUMNS.get(0).name())
                            .type(ColumnType.BYTE_ARRAY)
                            .build();

                    List<ColumnParams> columns = new ArrayList<>(COLUMNS);
                    columns.set(0, column);

                    builder.columns(columns);
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unexpected modification type: " + this);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(SystemViewType.class)
    public void testCreateSystemViewDoesNotReplaceExistingViewWithTheSameNameIfItsStructureIsTheSame(SystemViewType type) {
        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        manager.listen(CatalogEvent.SYSTEM_VIEW_CREATE, eventListener);

        CreateSystemViewCommand command = CreateSystemViewCommand.builder()
                .name("view1")
                .columns(List.of(
                        ColumnParams.builder().name("col1").type(INT32).build(),
                        ColumnParams.builder().name("col2").type(STRING).build()
                ))
                .type(type)
                .build();

        assertThat(manager.execute(command), willCompleteSuccessfully());

        verify(eventListener, times(1)).notify(any(), any());
        Mockito.reset(eventListener);

        // Create view
        int catalogVersion = manager.latestCatalogVersion();

        CatalogSchemaDescriptor systemSchema1 = manager.schema(SYSTEM_SCHEMA_NAME, catalogVersion);
        assertNotNull(systemSchema1, "systemSchema");

        CatalogSystemViewDescriptor view1 = systemSchema1.systemView("view1");

        // Use the same command to create an identical view.

        assertThat(manager.execute(command), willCompleteSuccessfully());

        CatalogSchemaDescriptor systemSchema2 = manager.schema(SYSTEM_SCHEMA_NAME, catalogVersion);
        assertNotNull(systemSchema2, "systemSchema");

        // view1 should be the same.

        CatalogSystemViewDescriptor view2 = systemSchema2.systemView("view1");
        assertSame(view1, view2, "system view was replaced");

        // Event listener should not have been called.
        verify(eventListener, never()).notify(any(), any());
    }

    @ParameterizedTest
    @EnumSource(SystemViewType.class)
    public void testCreateSystemViewFailsWhenTableWithTheSameNameExistsInTheSystemSchema(SystemViewType type) {
        CatalogCommand createTable = createTableCommandBuilder("view",
                List.of(columnParams("key1", INT32), columnParams("val1", INT32)),
                List.of("key1"),
                List.of("key1"))
                .schemaName(SYSTEM_SCHEMA_NAME)
                .build();

        assertThat(manager.execute(createTable), willCompleteSuccessfully());

        CreateSystemViewCommand createView = CreateSystemViewCommand.builder()
                .name("view")
                .columns(List.of(
                        ColumnParams.builder().name("col1").type(INT32).build(),
                        ColumnParams.builder().name("col2").type(STRING).build()
                ))
                .type(type)
                .build();

        expectAsyncValidationError(() -> manager.execute(createView), "Table with name 'SYSTEM.view' already exists");
    }

    @ParameterizedTest
    @EnumSource(SystemViewType.class)
    public void createTableFailsWhenSystemViewWithTheSameNameExistsInTheSystemSchema(SystemViewType type) {
        CreateSystemViewCommand createView = CreateSystemViewCommand.builder()
                .name("view")
                .columns(List.of(
                        ColumnParams.builder().name("col1").type(INT32).build(),
                        ColumnParams.builder().name("col2").type(STRING).build()
                ))
                .type(type)
                .build();

        assertThat(manager.execute(createView), willCompleteSuccessfully());

        CatalogCommand createTable = createTableCommandBuilder("view",
                List.of(columnParams("key1", INT32), columnParams("val1", INT32)),
                List.of("key1"),
                List.of("key1"))
                .schemaName(SYSTEM_SCHEMA_NAME)
                .build();

        expectAsyncValidationError(() -> manager.execute(createTable), "System view with name 'SYSTEM.view' already exists");
    }

    @ParameterizedTest
    @EnumSource(SystemViewType.class)
    public void createIndexFailsWhenSystemViewWithTheSameNameExistsInTheSystemSchema(SystemViewType type) {
        CreateSystemViewCommand createView = CreateSystemViewCommand.builder()
                .name("view")
                .columns(List.of(
                        ColumnParams.builder().name("col1").type(INT32).build(),
                        ColumnParams.builder().name("col2").type(STRING).build()
                ))
                .type(type)
                .build();

        assertThat(manager.execute(createView), willCompleteSuccessfully());

        CatalogCommand createTable = createTableCommandBuilder("table",
                List.of(columnParams("key1", INT32), columnParams("val1", INT32)),
                List.of("key1"),
                List.of("key1"))
                .schemaName(SYSTEM_SCHEMA_NAME)
                .build();

        assertThat(manager.execute(createTable), willCompleteSuccessfully());

        CreateHashIndexParams params = CreateHashIndexParams.builder()
                .indexName("view")
                .tableName("table")
                .columns(List.of("val1"))
                .schemaName(SYSTEM_SCHEMA_NAME)
                .build();

        expectAsyncValidationError(() -> manager.createIndex(params), "System view with name 'SYSTEM.view' already exists");
    }

    private static void expectAsyncValidationError(Supplier<CompletableFuture<Void>> action, String message) {
        CompletionException ex = assertThrows(CompletionException.class, () -> action.get().join());
        CatalogValidationException cause = assertInstanceOf(CatalogValidationException.class, ex.getCause());

        assertThat(cause.getMessage(), containsString(message));
    }
}
