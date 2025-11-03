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

import static org.apache.ignite.internal.catalog.CatalogManager.INITIAL_TIMESTAMP;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PRECISION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_SCALE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.SYSTEM_SCHEMAS;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
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
    static final String INDEX_NAME = "IDX";
    static final TablePrimaryKey ID_PK = TableHashPrimaryKey.builder()
            .columns(List.of("ID"))
            .build();

    private static final CatalogZoneDescriptor DEFAULT_ZONE = new CatalogZoneDescriptor(
            0,
            ZONE_NAME,
            1,
            1,
            1,
            -1,
            -1,
            "",
            fromParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build())),
            ConsistencyMode.STRONG_CONSISTENCY
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

    /**
     * Test cases for the create/alter zone parameter validation. The order is: number of replicas, default quorum size, default consensus
     * group size, minimum quorum size, maximum quorum size.
     *
     * @return Stream of arguments for parameterized test.
     */
    static Stream<Arguments> quorumTable() {
        return Stream.of(
                Arguments.of(null, 1, 1, 1, 1), // default replicas count is 1
                Arguments.of(1, 1, 1, 1, 1),
                Arguments.of(2, 2, 2, 2, 2),
                Arguments.of(3, 2, 3, 2, 2),
                Arguments.of(4, 2, 3, 2, 2),
                Arguments.of(5, 3, 5, 2, 3),
                Arguments.of(6, 3, 5, 2, 3),
                Arguments.of(7, 3, 5, 2, 4),
                Arguments.of(8, 3, 5, 2, 4),
                Arguments.of(9, 3, 5, 2, 5),
                Arguments.of(10, 3, 5, 2, 5)
        );
    }

    static Catalog emptyCatalog() {
        return new Catalog(
                0,
                0L,
                1,
                List.of(),
                List.of(new CatalogSchemaDescriptor(
                        0,
                        SCHEMA_NAME,
                        new CatalogTableDescriptor[0],
                        new CatalogIndexDescriptor[0],
                        new CatalogSystemViewDescriptor[0],
                        INITIAL_TIMESTAMP
                )),
                null
        );
    }

    static Catalog catalogWithDefaultZone() {
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
                createZoneCommand(zone1),
                createZoneCommand(zone2)
        );
    }

    static Catalog catalogWithIndex(String name) {
        return catalog(
                createTableCommand(TABLE_NAME),
                createIndexCommand(TABLE_NAME, name)
        );
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
                .primaryKey(ID_PK)
                .build();
    }

    static CatalogCommand createZoneCommand(String zoneName) {
        return CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
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

    static Catalog applyCommandsToCatalog(Catalog catalog, CatalogCommand... commandsToApply) {
        UpdateContext updateContext = new UpdateContext(catalog);
        for (CatalogCommand command : commandsToApply) {
            for (UpdateEntry updates : command.get(updateContext)) {
                updateContext.updateCatalog(catalog0 -> updates.applyUpdate(catalog0, INITIAL_TIMESTAMP));
            }
        }

        return updateContext.catalog();
    }

    static Catalog catalog(CatalogCommand... commandsToApply) {
        return applyCommandsToCatalog(catalogWithDefaultZone(), commandsToApply);
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
                        INITIAL_TIMESTAMP
                )),
                DEFAULT_ZONE.id());
    }

    static CatalogTableDescriptor table(int tableId, int schemaId, int zoneId, int pkIndexId, String columnName) {
        return CatalogTableDescriptor.builder()
                .id(tableId)
                .schemaId(schemaId)
                .primaryKeyIndexId(pkIndexId)
                .name("TEST_TABLE")
                .zoneId(zoneId)
                .newColumns(List.of(tableColumn(columnName)))
                .primaryKeyColumns(IntList.of(0))
                .storageProfile(DEFAULT_STORAGE_PROFILE)
                .build();
    }

    static CatalogTableColumnDescriptor tableColumn(String columnName) {
        return new CatalogTableColumnDescriptor(columnName, INT32, false, DEFAULT_PRECISION, DEFAULT_SCALE, DEFAULT_LENGTH, null);
    }

    /**
     * Transitions a given index from {@link CatalogIndexStatus#REGISTERED} to {@link CatalogIndexStatus#STOPPING} state.
     *
     * @throws NoSuchElementException if the given index does not exist.
     */
    static Catalog transitionIndexToStoppingState(Catalog catalog, String indexName) {
        int indexId = findIndex(catalog, indexName).id();

        CatalogCommand startIndexBuildingCommand = StartBuildingIndexCommand.builder()
                .indexId(indexId)
                .build();

        CatalogCommand makeIndexAvailableCommand = MakeIndexAvailableCommand.builder()
                .indexId(indexId)
                .build();

        CatalogCommand dropIndexCommand = DropIndexCommand.builder()
                .schemaName(SCHEMA_NAME)
                .indexName(indexName)
                .build();

        catalog = applyCommandsToCatalog(catalog, startIndexBuildingCommand, makeIndexAvailableCommand, dropIndexCommand);

        assertThat(findIndex(catalog, indexName).status(), is(CatalogIndexStatus.STOPPING));

        return catalog;
    }

    /** Creates a primary key with a hash index that consists of the given columns. */
    static TablePrimaryKey primaryKey(String column, String... columns) {
        List<String> pkColumns = new ArrayList<>();
        pkColumns.add(column);
        pkColumns.addAll(Arrays.asList(columns));

        return TableHashPrimaryKey.builder()
                .columns(pkColumns)
                .build();
    }

    private static CatalogIndexDescriptor findIndex(Catalog catalog, String indexName) {
        return catalog.indexes().stream()
                .filter(index -> index.name().equals(indexName))
                .findAny()
                .orElseThrow();
    }
}
