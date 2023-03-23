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

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.builder.TableDefinitionBuilder;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for table and index validation tests.
 */
@ExtendWith(ConfigurationExtension.class)
public class AbstractTableIndexValidatorTest {
    protected static final String TABLE_NAME = "schema.table";

    /** Basic table configuration to mutate and then validate. */
    @InjectConfiguration
    protected TablesConfiguration tablesCfg;

    @BeforeEach
    public void setup() throws Exception {
        final TableDefinitionBuilder builder = SchemaBuilders.tableBuilder("schema", "table");

        TableDefinition def = builder
                .columns(
                        SchemaBuilders.column("id", ColumnType.INT32).build(),
                        SchemaBuilders.column("affId", ColumnType.INT32).build(),
                        SchemaBuilders.column("id2", ColumnType.string()).asNullable(true).build()
                )

                .withPrimaryKey(
                        SchemaBuilders.primaryKey()  // Declare index column in order.
                                .withColumns("affId", "id")
                                .withColocationColumns("affId")
                                .build()
                ).build();

        tablesCfg.tables().change(c -> c.create("schema.table", tblChg -> SchemaConfigurationConverter.convert(def, tblChg))).get();
    }
}
