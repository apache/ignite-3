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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link RenameZoneCommand}.
 */
public class RenameZoneCommandValidationTest extends AbstractCommandValidationTest {
    private static final String ZONE_NAME = "test_zone";

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void srcDestZoneNamesMustNotBeNullOrBlank(String zone) {
        assertThrows(
                CatalogValidationException.class,
                () -> RenameZoneCommand.builder().zoneName(zone).build(),
                "Name of the zone can't be null or blank");

        assertThrows(
                CatalogValidationException.class,
                () -> RenameZoneCommand.builder().zoneName(ZONE_NAME).newZoneName(zone).build(),
                "New zone name can't be null or blank");
    }

    @Test
    void exceptionIsThrownIfZoneWithGivenNameNotFound() {
        RenameZoneCommandBuilder builder = RenameZoneCommand.builder();

        Catalog catalog = catalogWithDefaultZone();

        CatalogCommand command = builder
                .zoneName("some_zone")
                .newZoneName("some_zone1")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Distribution zone with name 'some_zone' not found"
        );
    }

    @Test
    void exceptionIsThrownIfZoneToRenameAlreadyExist() {
        RenameZoneCommandBuilder builder = RenameZoneCommand.builder();

        Catalog catalog = catalogWithZones("some_zone", "some_zone1");

        CatalogCommand command = builder
                .zoneName("some_zone")
                .newZoneName("some_zone1")
                .build();

        assertThrows(
                CatalogValidationException.class,
                () -> command.get(catalog),
                "Distribution zone with name 'some_zone1' already exists"
        );
    }
}
