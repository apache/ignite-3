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

package org.apache.ignite.internal.storage.util;

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.InconsistentIndexStateException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** For {@link StorageUtils} testing. */
public class StorageUtilsTest {
    private static final int PARTITION_ID = 0;

    @Test
    void testNotThrowExceptionIfIndexIsNotBuiltInReadableStatusForBuiltIndex() {
        for (CatalogIndexStatus indexStatus : CatalogIndexStatus.values()) {
            assertDoesNotThrow(() -> throwExceptionIfIndexIsNotBuiltInReadableStatus(null, indexStatus), indexStatus.toString());
        }

        assertDoesNotThrow(() -> throwExceptionIfIndexIsNotBuiltInReadableStatus(null, null));
    }

    @Test
    void testNotThrowExceptionIfIndexIsNotBuiltInReadableStatusForNotBuiltIndexInWriteOnlyStatus() {
        var nextRowIdToBuild = new RowId(PARTITION_ID);

        assertDoesNotThrow(() -> throwExceptionIfIndexIsNotBuiltInReadableStatus(nextRowIdToBuild, REGISTERED));
        assertDoesNotThrow(() -> throwExceptionIfIndexIsNotBuiltInReadableStatus(nextRowIdToBuild, BUILDING));
    }

    @Test
    void testThrowExceptionIfIndexIsNotBuiltInReadableStatusForNotBuiltIndexInReadableStatus() {
        var nextRowIdToBuild = new RowId(PARTITION_ID);

        assertThrows(
                InconsistentIndexStateException.class,
                () -> throwExceptionIfIndexIsNotBuiltInReadableStatus(nextRowIdToBuild, AVAILABLE)
        );

        assertThrows(
                InconsistentIndexStateException.class,
                () -> throwExceptionIfIndexIsNotBuiltInReadableStatus(nextRowIdToBuild, STOPPING)
        );
    }

    @Test
    void testThrowExceptionIfIndexIsNotBuiltInReadableStatusForDestroyedIndex() {
        assertThrows(
                InconsistentIndexStateException.class,
                () -> throwExceptionIfIndexIsNotBuiltInReadableStatus(new RowId(PARTITION_ID), null)
        );
    }



    private static void throwExceptionIfIndexIsNotBuiltInReadableStatus(
            @Nullable RowId nextRowIdToBuild,
            @Nullable CatalogIndexStatus indexStatus
    ) {
        StorageUtils.throwExceptionIfIndexIsNotBuiltInReadableStatus(nextRowIdToBuild, () -> indexStatus, () -> "storage");
    }
}
