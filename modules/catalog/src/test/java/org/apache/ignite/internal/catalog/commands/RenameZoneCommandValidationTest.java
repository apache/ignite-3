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

import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify validation of {@link RenameZoneCommand}.
 */
public class RenameZoneCommandValidationTest extends AbstractCommandValidationTest {
    private static final String ZONE_NAME = "test_zone";

    @Test
    void testValidateZoneNamesOnRenameZone() {
        assertThrows(
                CatalogValidationException.class,
                () -> RenameZoneCommand.builder().build(),
                "Name of the zone can't be null or blank");

        assertThrows(
                CatalogValidationException.class,
                () -> RenameZoneCommand.builder().zoneName(ZONE_NAME).build(),
                "New zone name can't be null or blank");

        assertThrows(
                CatalogValidationException.class,
                () -> RenameZoneCommand.builder().zoneName(DEFAULT_ZONE_NAME).newZoneName("some").build(),
                "Default distribution zone can't be renamed");

        // Let's check the success cases.
        RenameZoneCommand.builder().zoneName(ZONE_NAME).newZoneName(ZONE_NAME + 0).build();
    }
}
