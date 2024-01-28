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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.ChangeIndexStatusValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Abstract test for commands that change the index {@link CatalogIndexDescriptor#status()}. */
@SuppressWarnings("ThrowableNotThrown")
public abstract class AbstractChangeIndexStatusCommandValidationTest extends AbstractCommandValidationTest {
    /**
     * Returns a new command that changes the {@link CatalogIndexDescriptor#status() index status}.
     *
     * @param indexId Index ID.
     */
    abstract CatalogCommand createCommand(int indexId);

    /**
     * Returns {@code true} if the index status is invalid when executing the command.
     *
     * @param indexStatus Index status.
     */
    abstract boolean isInvalidPreviousIndexStatus(CatalogIndexStatus indexStatus);

    @Test
    void exceptionIsThrownIfIndexWithGivenNameNotFound() {
        Catalog catalog = emptyCatalog();

        CatalogCommand command = createCommand(1);

        assertThrowsWithCause(
                () -> command.get(catalog),
                IndexNotFoundValidationException.class,
                "Index with ID '1' not found"
        );
    }

    @ParameterizedTest
    @MethodSource("indexStatuses")
    void exceptionIsThrownIfIndexHasInvalidPreviousStatus(CatalogIndexStatus invalidPreviousIndexStatus) {
        assumeTrue(isInvalidPreviousIndexStatus(invalidPreviousIndexStatus), "Index status for command is valid.");

        int id = 0;

        int tableId = id++;
        int indexId = id++;

        String columnName = "c";

        Catalog catalog = catalog(
                new CatalogTableDescriptor[]{
                        table(tableId, id++, id++, id++, columnName)
                },
                new CatalogIndexDescriptor[]{
                        new CatalogHashIndexDescriptor(
                                indexId,
                                "TEST_INDEX",
                                tableId,
                                false,
                                List.of(columnName),
                                invalidPreviousIndexStatus
                        )
                },
                new CatalogSystemViewDescriptor[]{}
        );

        CatalogCommand command = createCommand(indexId);

        assertThrowsWithCause(
                () -> command.get(catalog),
                ChangeIndexStatusValidationException.class,
                "It is impossible to change the index status:"
        );
    }

    private static Stream<Arguments> indexStatuses() {
        return Stream.of(CatalogIndexStatus.values()).map(Arguments::of);
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
