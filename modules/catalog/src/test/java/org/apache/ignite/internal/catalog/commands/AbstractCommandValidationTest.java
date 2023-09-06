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

import static org.apache.ignite.sql.ColumnType.INT32;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
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
        int tableId = 0;
        int tableVersion = 1;
        int zoneId = 0;
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("ID", INT32, false, -1, -1, -1, null),
                new CatalogTableColumnDescriptor("VAL", INT32, true, -1, -1, -1, null)
        );
        List<String> pkColumns = List.of("ID");

        CatalogTableDescriptor table = new CatalogTableDescriptor(
                tableId, name, zoneId, tableVersion, columns, pkColumns, pkColumns
        );

        return catalog(new CatalogTableDescriptor[]{table}, new CatalogIndexDescriptor[0], new CatalogSystemViewDescriptor[0]);
    }

    static Catalog catalogWithTable(Consumer<CreateTableCommandBuilder> tableDef) {
        CreateTableCommandBuilder builder = CreateTableCommand.builder();

        tableDef.accept(builder);

        Catalog catalog = emptyCatalog();
        for (UpdateEntry entry : builder.build().get(catalog)) {
            catalog = entry.applyUpdate(catalog);
        }

        return catalog;
    }

    static Catalog catalogWithIndex(String name) {
        CatalogTableColumnDescriptor id = new CatalogTableColumnDescriptor(
                "ID", INT32, false, -1, -1, -1, null
        );
        CatalogTableColumnDescriptor val = new CatalogTableColumnDescriptor(
                "VAL", INT32, true, -1, -1, -1, null
        );
        CatalogTableDescriptor table = new CatalogTableDescriptor(
                0, TABLE_NAME, 0, 1, List.of(id, val), List.of("ID"), List.of("ID")
        );
        CatalogIndexDescriptor index = new CatalogHashIndexDescriptor(
                1, name, 0, false, List.of("VAL")
        );

        return catalog(new CatalogTableDescriptor[]{table}, new CatalogIndexDescriptor[]{index}, new CatalogSystemViewDescriptor[0]);
    }

    private static Catalog catalog(CatalogTableDescriptor[] tables,
            CatalogIndexDescriptor[] indexes,
            CatalogSystemViewDescriptor[] systemViews) {

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
                        systemViews
                ))
        );
    }
}
