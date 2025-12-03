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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxStateMeta.mutate;
import static org.apache.ignite.internal.tx.test.TxStateMetaTestUtils.assertTxStateMetaIsSame;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments.ArgumentSet;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link TxStateMeta}.
 */
public class TxStateMetaTest {
    private static final UUID COORDINATOR_ID = UUID.randomUUID();

    private static final TxStateMeta PENDING_META = new TxStateMeta(
            PENDING,
            COORDINATOR_ID,
            new ZonePartitionId(0, 0),
            null,
            null,
            null,
            null,
            false,
            "my-tx-label"
    );

    private static final TxStateMeta COMMITTED_META = new TxStateMeta(
            COMMITTED,
            COORDINATOR_ID,
            new ZonePartitionId(0, 0),
            hybridTimestamp(1),
            null,
            null,
            null,
            false,
            "my-tx-label"
    );

    private static final TxStateMeta BASE_WITH_IVOT = new TxStateMeta(
            COMMITTED,
            COORDINATOR_ID,
            new ZonePartitionId(0, 0),
            hybridTimestamp(1),
            null,
            1000L,
            null,
            false,
            "my-tx-label"
    );

    private static final TxStateMeta BASE_WITH_CCT = new TxStateMeta(
            COMMITTED,
            COORDINATOR_ID,
            new ZonePartitionId(0, 0),
            hybridTimestamp(1),
            null,
            null,
            1000L,
            false,
            "my-tx-label"
    );

    @ParameterizedTest
    @MethodSource("testMutateParameters")
    public void testMutate(@Nullable TxStateMeta meta, TxStateMeta expectedMeta, Function<TxStateMeta, TxStateMeta> mutator) {
        assertTxStateMetaIsSame(expectedMeta, mutator.apply(meta));
    }

    private static Stream<ArgumentSet> testMutateParameters() {
        return Stream.of(
                args("setInitialTs", COMMITTED_META, BASE_WITH_IVOT, m -> m.mutate().initialVacuumObservationTimestamp(1000L).build()),
                args("setCleanupTs", COMMITTED_META, BASE_WITH_CCT, m -> m.mutate().cleanupCompletionTimestamp(1000L).build()),
                args("initMeta", null, COMMITTED_META, m -> mutate(null, COMMITTED)
                        .txCoordinatorId(COMMITTED_META.txCoordinatorId())
                        .commitPartitionId(COMMITTED_META.commitPartitionId())
                        .commitTimestamp(COMMITTED_META.commitTimestamp())
                        .txLabel(COMMITTED_META.txLabel())
                        .build()
                ),
                args("abandoned", PENDING_META.abandoned(), PENDING_META.abandoned(), m -> {
                    TxStateMetaAbandoned abandonedMeta = (TxStateMetaAbandoned) m;
                    return abandonedMeta.mutate().build();
                }),
                args("finishing", PENDING_META.finishing(false), PENDING_META.finishing(false), m -> {
                    TxStateMetaFinishing finishing = (TxStateMetaFinishing) m;
                    return finishing.mutate().build();
                })
        );
    }

    private static ArgumentSet args(
            String name,
            @Nullable TxStateMeta meta,
            TxStateMeta expectedMeta,
            Function<TxStateMeta, TxStateMeta> mutator
    ) {
        return argumentSet(name, meta, expectedMeta, mutator);
    }
}
