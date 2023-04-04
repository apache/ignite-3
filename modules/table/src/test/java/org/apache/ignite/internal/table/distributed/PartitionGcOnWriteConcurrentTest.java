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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils.RunnableX;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests for concurrent cooperative GC (GC that is executed on write). */
@ExtendWith(ConfigurationExtension.class)
public class PartitionGcOnWriteConcurrentTest {
    private static final int PARTITION_ID = 1;
    private static final TablePartitionId TABLE_PARTITION_ID = new TablePartitionId(UUID.randomUUID(), PARTITION_ID);
    private static final HybridClock CLOCK = new HybridClockImpl();

    private MvPartitionStorage storage;
    private StorageUpdateHandler storageUpdateHandler;

    @BeforeEach
    void setUp(@InjectConfiguration DataStorageConfiguration dsCfg) {
        storage = mock(MvPartitionStorage.class);
        doAnswer(invocation -> {
            WriteClosure<?> cls = invocation.getArgument(0);

            return cls.execute();
        }).when(storage).runConsistently(any());

        when(storage.pollForVacuum(any())).thenReturn(null);

        storageUpdateHandler = new StorageUpdateHandler(
                PARTITION_ID,
                new TestPartitionDataStorage(storage),
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of()),
                dsCfg
        );
    }

    @ParameterizedTest
    @EnumSource(UpdateType.class)
    void testSameLwm(UpdateType updateType) {
        HybridTimestamp lwm = CLOCK.now();

        runRace(createRunnable(updateType, lwm), createRunnable(updateType, lwm));

        verify(storage, times(1)).pollForVacuum(lwm);
    }

    @ParameterizedTest
    @EnumSource(UpdateType.class)
    void testDifferentLwm(UpdateType updateType) {
        int count = 10;

        HybridTimestamp[] timestamps = new HybridTimestamp[count];

        RunnableX[] runnables = new RunnableX[count];

        for (int i = 0; i < count; i++) {
            HybridTimestamp ts = CLOCK.now();

            timestamps[i] = ts;

            runnables[i] = createRunnable(updateType, ts);
        }

        runRace(runnables);

        for (int i = 0; i < count - 1; i++) {
            verify(storage, atMostOnce()).pollForVacuum(timestamps[i]);
        }

        verify(storage, times(1)).pollForVacuum(timestamps[count - 1]);
    }

    @ParameterizedTest
    @EnumSource(UpdateType.class)
    void testDifferentLwmWithPreviousVacuums(UpdateType updateType) throws Throwable {
        HybridTimestamp lwm1 = CLOCK.now();
        HybridTimestamp lwm2 = CLOCK.now();
        HybridTimestamp lwm3 = CLOCK.now();

        createRunnable(updateType, lwm1).run();

        runRace(createRunnable(updateType, lwm2), createRunnable(updateType, lwm3));

        verify(storage, times(1)).pollForVacuum(lwm1);
        verify(storage, atMostOnce()).pollForVacuum(lwm2);
        verify(storage, times(1)).pollForVacuum(lwm3);
    }

    private RunnableX createRunnable(UpdateType updateType, HybridTimestamp lwm) {
        if (updateType == UpdateType.UPDATE) {
            //noinspection unchecked
            return () -> storageUpdateHandler.handleUpdate(
                    UUID.randomUUID(),
                    UUID.randomUUID(),
                    TABLE_PARTITION_ID,
                    buffer(),
                    mock(Consumer.class),
                    lwm
            );
        } else {
            //noinspection unchecked
            return () -> storageUpdateHandler.handleUpdateAll(
                    UUID.randomUUID(),
                    Collections.emptyMap(),
                    TABLE_PARTITION_ID,
                    mock(Consumer.class),
                    lwm
            );
        }
    }

    private static ByteBuffer buffer() {
        ByteBuffer buf = mock(ByteBuffer.class);
        when(buf.order()).thenReturn(ByteBufferRow.ORDER);

        return buf;
    }

    private enum UpdateType {
        UPDATE,
        UPDATE_ALL;
    }
}
