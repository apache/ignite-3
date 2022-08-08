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

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.mockito.Mockito.mock;

import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableValidator;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.schema.TestDataStorageConfigurationSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
                    + "    indices.foo {type = HASH, columnNames = [id]}\n"
                    + "}]",
            polymorphicExtensions = {
                    HashIndexConfigurationSchema.class,
                    SortedIndexConfigurationSchema.class,
                    UnknownDataStorageConfigurationSchema.class,
                    TestDataStorageConfigurationSchema.class,
                    ConstantValueDefaultConfigurationSchema.class,
                    FunctionCallDefaultConfigurationSchema.class,
                    NullValueDefaultConfigurationSchema.class
            }
    )
    private TablesConfiguration tablesCfg;

    /** Tests that validator finds no issues in a simple valid configuration. */
    @Test
    public void testNoIssues() {
        ValidationContext<NamedListView<TableView>> ctx = mockValidationContext(null, tablesCfg.tables().value());

        validate(TableValidatorImpl.INSTANCE, mock(TableValidator.class), ctx, null);
    }
}
