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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.ChangeIndexStatusValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
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

    Class<? extends Exception> expectedExceptionClassForWrongStatus() {
        return ChangeIndexStatusValidationException.class;
    }

    String expectedExceptionMessageSubstringForWrongStatus() {
        return "It is impossible to change the index status:";
    }

    @ParameterizedTest
    @MethodSource("indexStatuses")
    void exceptionIsThrownIfIndexHasInvalidPreviousStatus(CatalogIndexStatus invalidPreviousIndexStatus) {
        assumeTrue(isInvalidPreviousIndexStatus(invalidPreviousIndexStatus), "Index status for command is valid.");

        int id = 0;

        int tableId = id++;
        int indexId = id++;

        String columnName = "c";

        int version = 1;

        Catalog catalog = catalogWithDefaultZone(
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
                                IntList.of(0),
                                false
                        )
                },
                new CatalogSystemViewDescriptor[]{}
        );

        CatalogCommand command = createCommand(indexId);

        assertThrowsWithCause(
                () -> command.get(new UpdateContext(catalog)),
                expectedExceptionClassForWrongStatus(),
                expectedExceptionMessageSubstringForWrongStatus()
        );
    }

    private static Stream<Arguments> indexStatuses() {
        return Stream.of(CatalogIndexStatus.values()).map(Arguments::of);
    }

    @Test
    void exceptionIsThrownIfIndexWithGivenIdNotFound() {
        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = createCommand(1);

        assertThrowsWithCause(
                () -> command.get(new UpdateContext(catalog)),
                CatalogValidationException.class,
                "Index with ID '1' not found"
        );
    }
}
