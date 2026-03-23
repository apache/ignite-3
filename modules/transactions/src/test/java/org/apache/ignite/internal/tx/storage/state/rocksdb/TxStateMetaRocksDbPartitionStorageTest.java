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

package org.apache.ignite.internal.tx.storage.state.rocksdb;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Tests for {@link TxStateMetaRocksDbPartitionStorage}.
 */
@ExtendWith(ExecutorServiceExtension.class)
class TxStateMetaRocksDbPartitionStorageTest extends IgniteAbstractTest {
    private TxStateRocksDbSharedStorage sharedStorage;

    @BeforeEach
    void setUp(
            @InjectExecutorService ScheduledExecutorService scheduledExecutor,
            @InjectExecutorService ExecutorService executor
    ) {
        sharedStorage = new TxStateRocksDbSharedStorage(
                "test",
                workDir,
                scheduledExecutor,
                executor,
                mock(LogSyncer.class),
                mock(FailureProcessor.class),
                () -> 0
        );

        assertThat(sharedStorage.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        assertThat(sharedStorage.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    private TxStateMetaRocksDbPartitionStorage createMetaStorage(int zoneId, int partitionId) {
        return new TxStateMetaRocksDbPartitionStorage(sharedStorage.txStateMetaColumnFamily(), zoneId, partitionId);
    }

    @Test
    void testSettersAndGetters() throws RocksDBException {
        TxStateMetaRocksDbPartitionStorage storage = createMetaStorage(1, 2);

        storage.start();

        try (var writeBatch = new WriteBatch()) {
            storage.updateLastApplied(writeBatch, 10, 15);

            assertThat(storage.lastAppliedIndex(), is(10L));
            assertThat(storage.lastAppliedTerm(), is(15L));

            storage.updateConfiguration(writeBatch, new byte[] {1, 2, 3});

            assertThat(storage.configuration(), is(new byte[] {1, 2, 3}));

            LeaseInfo leaseInfo = randomLeaseInfo();

            storage.updateLease(writeBatch, leaseInfo);

            assertThat(storage.leaseInfo(), is(leaseInfo));
        }
    }

    @Test
    void testSettersAndGettersPersistence() throws RocksDBException {
        TxStateMetaRocksDbPartitionStorage storage = createMetaStorage(1, 2);

        storage.start();

        assertThat(storage.lastAppliedIndex(), is(0L));
        assertThat(storage.lastAppliedTerm(), is(0L));
        assertThat(storage.configuration(), is(nullValue()));
        assertThat(storage.leaseInfo(), is(nullValue()));

        LeaseInfo leaseInfo = randomLeaseInfo();

        try (
                var writeBatch = new WriteBatch();
                var options = new WriteOptions()
        ) {
            storage.updateLastApplied(writeBatch, 10, 15);
            storage.updateConfiguration(writeBatch, new byte[] {1, 2, 3});
            storage.updateLease(writeBatch, leaseInfo);

            sharedStorage.db().write(options, writeBatch);
        }

        storage = createMetaStorage(1, 2);

        storage.start();

        assertThat(storage.lastAppliedIndex(), is(10L));
        assertThat(storage.lastAppliedTerm(), is(15L));
        assertThat(storage.configuration(), is(new byte[] {1, 2, 3}));
        assertThat(storage.leaseInfo(), is(leaseInfo));
    }

    @Test
    void testClear() throws RocksDBException {
        TxStateMetaRocksDbPartitionStorage storage = createMetaStorage(1, 2);

        storage.start();

        LeaseInfo leaseInfo = randomLeaseInfo();

        try (
                var writeBatch = new WriteBatch();
                var options = new WriteOptions()
        ) {
            storage.updateLastApplied(writeBatch, 10, 15);
            storage.updateConfiguration(writeBatch, new byte[] {1, 2, 3});
            storage.updateLease(writeBatch, leaseInfo);

            sharedStorage.db().write(options, writeBatch);
        }

        assertThat(storage.lastAppliedIndex(), is(10L));
        assertThat(storage.lastAppliedTerm(), is(15L));
        assertThat(storage.configuration(), is(new byte[] {1, 2, 3}));
        assertThat(storage.leaseInfo(), is(leaseInfo));

        try (
                var writeBatch = new WriteBatch();
                var options = new WriteOptions()
        ) {
            storage.clear(writeBatch);

            assertThat(storage.lastAppliedIndex(), is(0L));
            assertThat(storage.lastAppliedTerm(), is(0L));
            assertThat(storage.configuration(), is(nullValue()));
            assertThat(storage.leaseInfo(), is(nullValue()));

            sharedStorage.db().write(options, writeBatch);
        }

        storage = createMetaStorage(1, 2);

        storage.start();

        assertThat(storage.lastAppliedIndex(), is(0L));
        assertThat(storage.lastAppliedTerm(), is(0L));
        assertThat(storage.configuration(), is(nullValue()));
        assertThat(storage.leaseInfo(), is(nullValue()));
    }

    @Test
    void testTableAndPartitionIndependence() throws RocksDBException {
        TxStateMetaRocksDbPartitionStorage storage0 = createMetaStorage(1, 1);
        TxStateMetaRocksDbPartitionStorage storage1 = createMetaStorage(1, 2);
        TxStateMetaRocksDbPartitionStorage storage2 = createMetaStorage(2, 1);

        storage0.start();
        storage1.start();
        storage2.start();

        LeaseInfo leaseInfo0 = randomLeaseInfo();
        LeaseInfo leaseInfo1 = randomLeaseInfo();
        LeaseInfo leaseInfo2 = randomLeaseInfo();

        try (
                var writeBatch = new WriteBatch();
                var options = new WriteOptions()
        ) {
            storage0.updateLastApplied(writeBatch, 10, 15);
            storage1.updateLastApplied(writeBatch, 20, 25);
            storage2.updateLastApplied(writeBatch, 30, 35);

            storage0.updateConfiguration(writeBatch, new byte[] {1, 2, 3});
            storage1.updateConfiguration(writeBatch, new byte[] {4, 5, 6});
            storage2.updateConfiguration(writeBatch, new byte[] {7, 8, 9});

            storage0.updateLease(writeBatch, leaseInfo0);
            storage1.updateLease(writeBatch, leaseInfo1);
            storage2.updateLease(writeBatch, leaseInfo2);

            assertThat(storage0.lastAppliedIndex(), is(10L));
            assertThat(storage0.lastAppliedTerm(), is(15L));
            assertThat(storage0.configuration(), is(new byte[] {1, 2, 3}));
            assertThat(storage0.leaseInfo(), is(leaseInfo0));

            assertThat(storage1.lastAppliedIndex(), is(20L));
            assertThat(storage1.lastAppliedTerm(), is(25L));
            assertThat(storage1.configuration(), is(new byte[] {4, 5, 6}));
            assertThat(storage1.leaseInfo(), is(leaseInfo1));

            assertThat(storage2.lastAppliedIndex(), is(30L));
            assertThat(storage2.lastAppliedTerm(), is(35L));
            assertThat(storage2.configuration(), is(new byte[] {7, 8, 9}));
            assertThat(storage2.leaseInfo(), is(leaseInfo2));

            sharedStorage.db().write(options, writeBatch);
        }

        storage0 = createMetaStorage(1, 1);
        storage1 = createMetaStorage(1, 2);
        storage2 = createMetaStorage(2, 1);

        storage0.start();
        storage1.start();
        storage2.start();

        assertThat(storage0.lastAppliedIndex(), is(10L));
        assertThat(storage0.lastAppliedTerm(), is(15L));
        assertThat(storage0.configuration(), is(new byte[] {1, 2, 3}));
        assertThat(storage0.leaseInfo(), is(leaseInfo0));

        assertThat(storage1.lastAppliedIndex(), is(20L));
        assertThat(storage1.lastAppliedTerm(), is(25L));
        assertThat(storage1.configuration(), is(new byte[] {4, 5, 6}));
        assertThat(storage1.leaseInfo(), is(leaseInfo1));

        assertThat(storage2.lastAppliedIndex(), is(30L));
        assertThat(storage2.lastAppliedTerm(), is(35L));
        assertThat(storage2.configuration(), is(new byte[] {7, 8, 9}));
        assertThat(storage2.leaseInfo(), is(leaseInfo2));
    }

    private static LeaseInfo randomLeaseInfo() {
        return new LeaseInfo(
                ThreadLocalRandom.current().nextLong(),
                UUID.randomUUID(),
                "test"
        );
    }
}
