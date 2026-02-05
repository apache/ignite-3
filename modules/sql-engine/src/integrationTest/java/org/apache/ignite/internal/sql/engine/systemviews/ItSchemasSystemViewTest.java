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

package org.apache.ignite.internal.sql.engine.systemviews;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify the {@code SCHEMAS} system view.
 */
public class ItSchemasSystemViewTest extends AbstractSystemViewTest {
    @Test
    public void metadata() {
        assertQuery("SELECT * FROM system.schemas")
                .columnMetadata(
                        new MetadataMatcher()
                                .name("SCHEMA_ID")
                                .type(ColumnType.INT32)
                                .nullable(true),
                        new MetadataMatcher()
                                .name("SCHEMA_NAME")
                                .type(ColumnType.STRING)
                                .precision(CatalogUtils.DEFAULT_VARLEN_LENGTH)
                                .nullable(true)
                )
                .check();
    }

    @Test
    public void test() {
        CatalogManager catalogManager = TestWrappers.unwrapIgniteImpl(node(0)).catalogManager();

        Map<String, Integer> initialSchemas = catalogManager.latestCatalog()
                .schemas()
                .stream()
                .map(s -> Map.entry(s.name(), s.id()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        int publicSchemaId = initialSchemas.get("PUBLIC");
        int systemSchemaId = initialSchemas.get("SYSTEM");

        // Initial state

        assertQuery("SELECT schema_id, schema_name FROM system.schemas ORDER BY schema_name")
                .returns(publicSchemaId, "PUBLIC")
                .returns(systemSchemaId, "SYSTEM")
                .check();

        // Create new schema

        sql("CREATE SCHEMA TEST_SCHEMA");

        CatalogSchemaDescriptor newSchema = catalogManager.latestCatalog().schema("TEST_SCHEMA");
        assertNotNull(newSchema);

        assertQuery("SELECT schema_id, schema_name FROM system.schemas ORDER BY schema_name")
                .returns(publicSchemaId, "PUBLIC")
                .returns(systemSchemaId, "SYSTEM")
                .returns(newSchema.id(), "TEST_SCHEMA")
                .check();

        // Drop new schema
        sql("DROP SCHEMA TEST_SCHEMA");

        assertQuery("SELECT schema_id, schema_name FROM system.schemas ORDER BY schema_name")
                .returns(publicSchemaId, "PUBLIC")
                .returns(systemSchemaId, "SYSTEM")
                .check();
    }
}
