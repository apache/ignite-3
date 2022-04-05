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

package org.apache.ignite.internal.schema.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.PartialIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.schema.TestDataStorageConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.schema.TestRocksDbDataStorageConfigurationSchema;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * TableValidatorImplTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@ExtendWith(ConfigurationExtension.class)
public class TableValidatorImplTest {
    /** Basic table configuration to mutate and then validate. */
    @InjectConfiguration(
            value = "mock.tables.table {\n"
                    + "    columns.id {name = id, type.type = STRING, nullable = true},\n"
                    + "    primaryKey {columns = [id], colocationColumns = [id]},\n"
                    + "    indices.foo {type = HASH, name = foo, colNames = [id]}"
                    + "}",
            name = "schema.table",
            polymorphicExtensions = {
                    HashIndexConfigurationSchema.class,
                    SortedIndexConfigurationSchema.class,
                    PartialIndexConfigurationSchema.class,
                    TestRocksDbDataStorageConfigurationSchema.class,
                    TestDataStorageConfigurationSchema.class
            }
    )
    private TablesConfiguration tablesCfg;

    /** Tests that validator finds no issues in a simple valid configuration. */
    @Test
    public void testNoIssues() {
        ValidationContext<NamedListView<TableView>> ctx = mockContext(null);

        ArgumentCaptor<ValidationIssue> issuesCaptor = validate(ctx);

        assertThat(issuesCaptor.getAllValues(), is(empty()));
    }

    private ValidationContext<NamedListView<TableView>> mockContext(
            @Nullable NamedListView<TableView> oldValue
    ) {
        ValidationContext<NamedListView<TableView>> ctx = mock(ValidationContext.class);

        NamedListView<TableView> newValue = tablesCfg.tables().value();

        when(ctx.getOldValue()).thenReturn(oldValue);
        when(ctx.getNewValue()).thenReturn(newValue);

        return ctx;
    }

    private static ArgumentCaptor<ValidationIssue> validate(ValidationContext<NamedListView<TableView>> ctx) {
        ArgumentCaptor<ValidationIssue> issuesCaptor = ArgumentCaptor.forClass(ValidationIssue.class);

        doNothing().when(ctx).addIssue(issuesCaptor.capture());

        TableValidatorImpl.INSTANCE.validate(null, ctx);

        return issuesCaptor;
    }
}
