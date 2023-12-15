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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link DropZoneCommand}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class DropZoneCommandValidationTest extends AbstractCommandValidationTest {
    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void zoneNameMustNotBeNullOrBlank(String zone) {
        assertThrows(
                CatalogValidationException.class,
                () -> DropZoneCommand.builder().zoneName(zone).build(),
                "Name of the zone can't be null or blank"
        );
    }

    @Test
    void rejectToDropDefaultZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> DropZoneCommand.builder().zoneName(DEFAULT_ZONE_NAME).build(),
                "Default distribution zone can't be dropped"
        );
    }

    @Test
    void exceptionIsThrownIfZoneWithGivenNameNotFound() {
        DropZoneCommandBuilder builder = DropZoneCommand.builder();

        Catalog catalog = emptyCatalog();

        CatalogCommand command = builder
                .zoneName("some_zone")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Distribution zone with name 'some_zone' not found"
        );
    }
}
