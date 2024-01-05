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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PRECISION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_SCALE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.sql.ColumnType.INT32;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.IndexAlreadyAvailableValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.junit.jupiter.api.Test;

/** Tests to verify validation of {@link MakeIndexAvailableCommand}. */
@SuppressWarnings("ThrowableNotThrown")
public class MakeIndexAvailableCommandValidationTest extends AbstractCommandValidationTest {
    @Test
    void exceptionIsThrownIfIndexWithGivenNameNotFound() {
        Catalog catalog = emptyCatalog();

        CatalogCommand command = MakeIndexAvailableCommand.builder()
                .indexId(1)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                IndexNotFoundValidationException.class,
                "Index with ID '1' not found"
        );
    }

    @Test
    void exceptionIsThrownIfIndexIsAlreadyAvailable() {
        int id = 0;

        int tableId = id++;
        int indexId = id++;

        String columnName = "c";

        Catalog catalog = catalog(
                new CatalogTableDescriptor[]{
                        table(tableId, id++, id++, id++, columnName)
                },
                new CatalogIndexDescriptor[]{
                        new CatalogHashIndexDescriptor(indexId, "TEST_INDEX", tableId, false, List.of(columnName), true)
                },
                new CatalogSystemViewDescriptor[]{}
        );

        CatalogCommand command = MakeIndexAvailableCommand.builder()
                .indexId(indexId)
                .build();

        assertThrowsWithCause(
                () -> command.get(catalog),
                IndexAlreadyAvailableValidationException.class,
                format("Index is already available '{}'", indexId)
        );
    }

    private static CatalogTableDescriptor table(int tableId, int schemaId, int zoneId, int pkIndexId, String columnName) {
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

    private static CatalogTableColumnDescriptor tableColumn(String columnName) {
        return new CatalogTableColumnDescriptor(columnName, INT32, false, DEFAULT_PRECISION, DEFAULT_SCALE, DEFAULT_LENGTH, null);
    }
}
