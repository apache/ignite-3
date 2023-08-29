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

package org.apache.ignite.internal.schema.configuration;

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.index.HashIndexChange;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * TableValidatorImplTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@ExtendWith(ConfigurationExtension.class)
public class TableValidatorImplTest extends BaseIgniteAbstractTest {
    @InjectConfiguration(
            "mock.tables.table {"
                    + "columns.id {type.type: INT32}, "
                    + "columns.affId {type.type: INT32}, "
                    + "columns.id2 {type.type: STRING}, "
                    + "primaryKey {columns: [affId, id], colocationColumns: [affId]}"
                    + "}"
    )
    private TablesConfiguration tablesCfg;

    /** Tests that validator finds no issues in a simple valid configuration. */
    @Test
    public void testNoIssues() {
        validate0((String[]) null);
    }

    @Test
    void testCreateTableWithSameIndexName() {
        assertThat(
                tablesCfg.indexes().change(indexesChange ->
                        indexesChange.create("table", indexChange -> indexChange.convert(HashIndexChange.class))
                ),
                willCompleteSuccessfully()
        );

        validate0("Unable to create table. Index with the same name already exists.");
    }

    private void validate0(String @Nullable ... errorMessagePrefixes) {
        ValidationContext<NamedListView<TableView>> validationContext = mock(ValidationContext.class);

        when(validationContext.getNewValue()).then(invocation -> tablesCfg.tables().value());

        when(validationContext.getNewRoot(TablesConfiguration.KEY)).then(invocation -> tablesCfg.value());

        validate(TableValidatorImpl.INSTANCE, mock(TableValidator.class), validationContext, errorMessagePrefixes);
    }
}
