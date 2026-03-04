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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.junit.jupiter.api.Test;

class VolatileTxStateMetaStorageTest extends BaseIgniteAbstractTest {
    @Test
    void enrichMetaAllowsOnlyMetadataChanges() {
        VolatileTxStateMetaStorage storage = VolatileTxStateMetaStorage.createStarted();
        UUID txId = UUID.randomUUID();

        storage.updateMeta(txId, old -> TxStateMeta.builder(PENDING).build());

        TxStateMeta meta = storage.enrichMeta(txId, old -> old.mutate()
                .lastException(new RuntimeException("test"))
                .build());

        assertNotNull(meta);
        assertNotNull(meta.lastException());
        assertEquals("test", meta.lastException().getMessage());
    }

    @Test
    void enrichMetaRejectsStateCorrelatedFieldChanges() {
        VolatileTxStateMetaStorage storage = VolatileTxStateMetaStorage.createStarted();
        UUID txId = UUID.randomUUID();

        storage.updateMeta(txId, old -> TxStateMeta.builder(PENDING).build());

        IgniteInternalException ex = assertThrows(IgniteInternalException.class, () -> storage.enrichMeta(txId, old -> old.mutate()
                .commitTimestamp(HybridTimestamp.hybridTimestamp(1L))
                .build()));

        assertEquals(
                "enrichMeta must not change transaction state-correlated fields [txId=" + txId + ']',
                ex.getMessage()
        );
    }
}
