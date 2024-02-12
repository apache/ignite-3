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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PRECISION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_SCALE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.SYSTEM_SCHEMAS;
import static org.apache.ignite.sql.ColumnType.INT32;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Abstract test class for all command-related validation tests.
 *
 * <p>Provides a convenient set of utility methods to reuse in test.
 */
abstract class AbstractCommandValidationTest extends BaseIgniteAbstractTest {
    static final String SCHEMA_NAME = "PUBLIC";
    static final String TABLE_NAME = "TEST";
    static final String ZONE_NAME = "Default";

    private static final CatalogZoneDescriptor DEFAULT_ZONE = new CatalogZoneDescriptor(
            0, ZONE_NAME, 1, -1, -1, -1, -1, "", null
    );

    static Stream<Arguments> nullAndBlankStrings() {
        return Stream.of(null, "", " ", "  ").map(Arguments::of);
    }

    static Stream<Arguments> reservedSchemaNames() {
        return SYSTEM_SCHEMAS.stream().map(Arguments::of);
    }

    static Stream<Arguments> nullAndEmptyLists() {
        return Stream.of(null, List.of()).map(Arguments::of);
    }

    static Stream<Arguments> nullAndEmptySets() {
        return Stream.of(null, Set.of()).map(Arguments::of);
    }

    static Catalog emptyCatalog() {
        return catalog(1, new CatalogTableDescriptor[0], new CatalogIndexDescriptor[0], new CatalogSystemViewDescriptor[0]);
    }

    static Catalog catalogWithTable(String name) {
        return catalog(
                createTableCommand(name)
        );
    }

    static Catalog catalogWithTable(Consumer<CreateTableCommandBuilder> tableDef) {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        tableDef.accept(builder);

        return catalog(builder.build());
    }

    static Catalog catalogWithZone(String name) {
        return catalog(
                createZoneCommand(name)
        );
    }

    static Catalog catalogWithZones(String zone1, String zone2) {
        return catalog(
                List.of(createZoneCommand(zone1), createZoneCommand(zone2))
        );
    }

    static Catalog catalogWithIndex(String name) {
        return catalog(List.of(
                createTableCommand(TABLE_NAME),
                createIndexCommand(TABLE_NAME, name)
        ));
    }

    static CatalogCommand createIndexCommand(String tableName, String indexName) {
        return CreateHashIndexCommand.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(indexName)
                .tableName(tableName)
                .columns(List.of("VAL"))
                .build();
    }

    static CatalogCommand createTableCommand(String tableName) {
        return createTableCommand(ZONE_NAME, tableName);
    }

    static CatalogCommand createTableCommand(String zoneName, String tableName) {
        return CreateTableCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(tableName)
                .zone(zoneName)
                .columns(List.of(
                        ColumnParams.builder().name("ID").type(INT32).build(),
                        ColumnParams.builder().name("VAL").type(INT32).build()
                ))
                .primaryKeyColumns(List.of("ID"))
                .build();
    }

    static CatalogCommand createZoneCommand(String zoneName) {
        return CreateZoneCommand.builder()
                .zoneName(zoneName)
                .build();
    }

    static Catalog catalog(CatalogCommand commandToApply) {
        return catalog(List.of(commandToApply));
    }

    static Catalog catalog(List<CatalogCommand> commandsToApply) {
        Catalog catalog = emptyCatalog();

        for (CatalogCommand command : commandsToApply) {
            for (UpdateEntry updates : command.get(catalog)) {
                catalog = updates.applyUpdate(catalog, INITIAL_CAUSALITY_TOKEN);
            }
        }

        return catalog;
    }

    static Catalog catalog(
            int version,
            CatalogTableDescriptor[] tables,
            CatalogIndexDescriptor[] indexes,
            CatalogSystemViewDescriptor[] systemViews
    ) {
        return new Catalog(
                version,
                0L,
                1,
                List.of(DEFAULT_ZONE),
                List.of(new CatalogSchemaDescriptor(
                        0,
                        SCHEMA_NAME,
                        tables,
                        indexes,
                        systemViews,
                        INITIAL_CAUSALITY_TOKEN
                ))
        );
    }

    static CatalogTableDescriptor table(int tableId, int schemaId, int zoneId, int pkIndexId, String columnName) {
        return new CatalogTableDescriptor(
                tableId,
                schemaId,
                pkIndexId,
                "TEST_TABLE",
                zoneId,
                List.of(tableColumn(columnName)),
                List.of(columnName),
                null
        );
    }

    static CatalogTableColumnDescriptor tableColumn(String columnName) {
        return new CatalogTableColumnDescriptor(columnName, INT32, false, DEFAULT_PRECISION, DEFAULT_SCALE, DEFAULT_LENGTH, null);
    }
}
