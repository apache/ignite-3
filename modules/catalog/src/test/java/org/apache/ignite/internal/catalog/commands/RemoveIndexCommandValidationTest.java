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

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests to verify validation of {@link RemoveIndexCommand}. */
public class RemoveIndexCommandValidationTest extends AbstractIndexByIdCommandValidationTest {
    private static Stream<Arguments> indexStatuses() {
        return Stream.of(CatalogIndexStatus.values()).map(Arguments::of);
    }

    @Override
    public CatalogCommand createCommand(int indexId) {
        return RemoveIndexCommand.builder().indexId(indexId).schemaName(CatalogManager.DEFAULT_SCHEMA_NAME).build();
    }

    private boolean isInvalidPreviousIndexStatus(CatalogIndexStatus indexStatus) {
        return indexStatus != REGISTERED && indexStatus != BUILDING && indexStatus != STOPPING;
    }

    @SuppressWarnings("ThrowableNotThrown")
    @ParameterizedTest
    @MethodSource("indexStatuses")
    void exceptionIsThrownIfIndexHasInvalidPreviousStatus(CatalogIndexStatus invalidPreviousIndexStatus) {
        assumeTrue(isInvalidPreviousIndexStatus(invalidPreviousIndexStatus), "Index status for command is valid.");

        int id = 0;

        int tableId = id++;
        int indexId = id++;

        String columnName = "c";

        int version = 1;

        Catalog catalog = catalog(
                version,
                new CatalogTableDescriptor[]{
                        table(tableId, id++, id++, id++, columnName)
                },
                new CatalogIndexDescriptor[]{
                        new CatalogHashIndexDescriptor(
                                indexId,
                                "TEST_INDEX",
                                tableId,
                                false,
                                invalidPreviousIndexStatus,
                                version,
                                List.of(columnName)
                        )
                },
                new CatalogSystemViewDescriptor[]{}
        );

        CatalogCommand command = createCommand(indexId);

        assertThrowsWithCause(
                () -> command.get(catalog),
                CatalogValidationException.class,
                "Cannot remove index"
        );
    }
}
