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

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockAddIssue;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexChange;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.PartialIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.schema.TestDataStorageConfigurationSchema;
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
            value = "mock.tables = [{\n"
                    + "    name = schema.table,\n"
                    + "    columns.id {type.type = STRING, nullable = true},\n"
                    + "    primaryKey {columns = [id], colocationColumns = [id]},\n"
                    + "    indices.foo {type = HASH, colNames = [id]}\n"
                    + "}]",
            polymorphicExtensions = {
                    HashIndexConfigurationSchema.class,
                    SortedIndexConfigurationSchema.class,
                    PartialIndexConfigurationSchema.class,
                    UnknownDataStorageConfigurationSchema.class,
                    TestDataStorageConfigurationSchema.class
            }
    )
    private TablesConfiguration tablesCfg;

    /** Tests that validator finds no issues in a simple valid configuration. */
    @Test
    public void testNoIssues() {
        ValidationContext<NamedListView<TableView>> ctx = mockValidationContext(null, tablesCfg.tables().value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = mockAddIssue(ctx);

        TableValidatorImpl.INSTANCE.validate(null, ctx);

        assertThat(issuesCaptor.getAllValues(), is(empty()));
    }

    /** Tests that column names and column keys inside a Named List must be equal. */
    @Test
    void testMisalignedColumnNamedListKeys() {
        NamedListView<TableView> oldValue = tablesCfg.tables().value();

        TableConfiguration tableCfg = tablesCfg.tables().get("table");

        CompletableFuture<Void> tableChangeFuture = tableCfg.columns()
                .change(columnsChange -> columnsChange
                        .create("ololo", columnChange -> columnChange
                                .changeName("not ololo")
                                .changeType(columnTypeChange -> columnTypeChange.changeType("STRING"))
                                .changeNullable(true)));

        assertThat(tableChangeFuture, willBe(nullValue(Void.class)));

        ValidationContext<NamedListView<TableView>> ctx = mockValidationContext(oldValue, tablesCfg.tables().value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = mockAddIssue(ctx);

        TableValidatorImpl.INSTANCE.validate(null, ctx);

        assertThat(issuesCaptor.getAllValues(), hasSize(1));

        assertThat(
                issuesCaptor.getValue().message(),
                is(equalTo("Column name \"not ololo\" does not match its Named List key: \"ololo\""))
        );
    }

    /** Tests that index names and index keys inside a Named List must be equal. */
    @Test
    void testMisalignedIndexNamedListKeys() {
        NamedListView<TableView> oldValue = tablesCfg.tables().value();

        TableConfiguration tableCfg = tablesCfg.tables().get("table");

        CompletableFuture<Void> tableChangeFuture = tableCfg.indices()
                .change(indicesChange -> indicesChange
                        .create("ololo", indexChange -> indexChange
                                .changeName("not ololo")
                                .convert(HashIndexChange.class)
                                .changeColNames("id")));

        assertThat(tableChangeFuture, willBe(nullValue(Void.class)));

        ValidationContext<NamedListView<TableView>> ctx = mockValidationContext(oldValue, tablesCfg.tables().value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = mockAddIssue(ctx);

        TableValidatorImpl.INSTANCE.validate(null, ctx);

        assertThat(issuesCaptor.getAllValues(), hasSize(1));

        assertThat(
                issuesCaptor.getValue().message(),
                is(equalTo("Index name \"not ololo\" does not match its Named List key: \"ololo\""))
        );
    }
}
