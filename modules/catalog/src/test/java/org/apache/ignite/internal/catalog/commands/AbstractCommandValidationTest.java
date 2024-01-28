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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.util.Constants.DUMMY_STORAGE_PROFILE;
import static org.apache.ignite.sql.ColumnType.INT32;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
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
            0, ZONE_NAME, 1, -1, -1, -1, -1, "", null,
            fromParams(List.of(StorageProfileParams.builder().storageProfile(DUMMY_STORAGE_PROFILE).build()))
    );

    static Stream<Arguments> nullAndBlankStrings() {
        return Stream.of(null, "", " ", "  ").map(Arguments::of);
    }

    static Stream<Arguments> nullAndEmptyLists() {
        return Stream.of(null, List.of()).map(Arguments::of);
    }

    static Stream<Arguments> nullAndEmptySets() {
        return Stream.of(null, Set.of()).map(Arguments::of);
    }

    static Catalog emptyCatalog() {
        return catalog(new CatalogTableDescriptor[0], new CatalogIndexDescriptor[0], new CatalogSystemViewDescriptor[0]);
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
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DUMMY_STORAGE_PROFILE).build()))
                .build();
    }

    static CatalogCommand createZoneCommand(String zoneName, List<String> storageProfiles) {
        List<StorageProfileParams> params = storageProfiles.stream()
                .map(p -> StorageProfileParams.builder().storageProfile(p).build())
                .collect(Collectors.toList());

        return CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(params)
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
            CatalogTableDescriptor[] tables,
            CatalogIndexDescriptor[] indexes,
            CatalogSystemViewDescriptor[] systemViews
    ) {
        return new Catalog(
                1,
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
}
