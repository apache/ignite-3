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

package org.apache.ignite.internal.table.distributed.schema;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CatalogVersionSufficiencyTest extends BaseIgniteAbstractTest {
    @Mock
    private CatalogService catalogService;

    @Test
    void exceedingLocalVersionIsSufficient() {
        when(catalogService.latestCatalogVersion()).thenReturn(10);

        assertTrue(CatalogVersionSufficiency.isMetadataAvailableFor(8, catalogService));
    }

    @Test
    void equalLocalVersionIsSufficient() {
        when(catalogService.latestCatalogVersion()).thenReturn(10);

        assertTrue(CatalogVersionSufficiency.isMetadataAvailableFor(10, catalogService));
    }

    @Test
    void lowerLocalVersionIsSufficient() {
        when(catalogService.latestCatalogVersion()).thenReturn(10);

        assertFalse(CatalogVersionSufficiency.isMetadataAvailableFor(12, catalogService));
    }
}
