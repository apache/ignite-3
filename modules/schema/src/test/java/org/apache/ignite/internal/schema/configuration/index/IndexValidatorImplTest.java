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

package org.apache.ignite.internal.schema.configuration.index;

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link IndexValidatorImpl} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class IndexValidatorImplTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID = 1;

    @InjectConfiguration("mock.tables.fooTable {columns.column0 {type.type: STRING}}")
    private TablesConfiguration tablesConfig;

    @BeforeEach
    void setupTableConfig() {
        assertThat(tablesConfig.tables().get("fooTable").id().update(TABLE_ID), willSucceedFast());
    }

    @Test
    void testMissingTable() {
        createIndex("fooIndex", indexChange -> indexChange.changeTableId(999).convert(HashIndexChange.class));
        createIndex("barIndex", indexChange -> indexChange.changeTableId(999).convert(SortedIndexChange.class));

        validate0(
                "Unable to create index [name=fooIndex]. Table not found.",
                "Unable to create index [name=barIndex]. Table not found."
        );
    }

    @Test
    void testMissingColumns() {
        createIndex("fooIndex", indexChange -> indexChange.convert(HashIndexChange.class).changeColumnNames().changeTableId(tableId()));
        createIndex("barIndex", indexChange -> indexChange.convert(SortedIndexChange.class).changeTableId(tableId()));

        validate0(
                "Index must include at least one column",
                "Index must include at least one column"
        );
    }

    @Test
    void testColumnsDoesNotExists() {
        createIndex(
                "fooIndex",
                indexChange -> indexChange.convert(HashIndexChange.class)
                        .changeColumnNames("notExists0")
                        .changeTableId(tableId())
        );

        createIndex(
                "barIndex",
                indexChange -> indexChange.convert(SortedIndexChange.class)
                        .changeColumns(indexColumnsChange -> indexColumnsChange.create("notExists1", indexColumnChange -> {}))
                        .changeTableId(tableId())
        );

        validate0(
                "Columns don't exist [columns=[notExists0]]",
                "Columns don't exist [columns=[notExists1]]"
        );
    }

    @Test
    void testCreateIndexWithSameTableName() {
        createIndex(
                "fooTable",
                indexChange -> indexChange.convert(HashIndexChange.class).changeColumnNames("column0").changeTableId(tableId())
        );

        validate0("Unable to create index. Table with the same name already exists.");
    }

    @Test
    void testCorrectIndexes() {
        createIndex(
                "fooIndex",
                indexChange -> indexChange.convert(HashIndexChange.class).changeColumnNames("column0").changeTableId(tableId())
        );

        createIndex(
                "barIndex",
                indexChange -> indexChange.convert(SortedIndexChange.class)
                        .changeColumns(indexColumnsChange -> indexColumnsChange.create("column0", indexColumnChange -> {}))
                        .changeTableId(tableId())
        );

        validate0((String[]) null);
    }

    private void validate0(String @Nullable ... errorMessagePrefixes) {
        ValidationContext<NamedListView<TableIndexView>> validationContext = mock(ValidationContext.class);

        when(validationContext.getNewValue()).then(invocation -> tablesConfig.indexes().value());

        when(validationContext.getNewRoot(TablesConfiguration.KEY)).then(invocation -> tablesConfig.value());

        validate(IndexValidatorImpl.INSTANCE, mock(IndexValidator.class), validationContext, errorMessagePrefixes);
    }

    private int tableId() {
        return tablesConfig.tables().get("fooTable").id().value();
    }

    private void createIndex(String indexName, Consumer<TableIndexChange> indexChange) {
        assertThat(
                tablesConfig.indexes().change(indexesChange -> indexesChange.create(indexName, indexChange)),
                willCompleteSuccessfully()
        );
    }
}
