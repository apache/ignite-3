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

package org.apache.ignite.internal.table.distributed.replication;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.SortedIndexLocker;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.PlacementDriver;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.IgniteException;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** There are tests for partition replica listener. */
public class PartitionReplicaListenerIndexLockingTest extends IgniteAbstractTest {
    private static final int PART_ID = 0;
    private static final UUID TABLE_ID = new UUID(0L, 0L);
    private static final UUID PK_INDEX_ID = new UUID(0L, 1L);
    private static final UUID HASH_INDEX_ID = new UUID(0L, 2L);
    private static final UUID SORTED_INDEX_ID = new UUID(0L, 3L);
    private static final UUID TRANSACTION_ID = Timestamp.nextVersion().toUuid();
    private static final HybridClock CLOCK = new HybridClockImpl();
    private static final LockManager LOCK_MANAGER = new HeapLockManager();
    private static final TablePartitionId PARTITION_ID = new TablePartitionId(TABLE_ID, PART_ID);
    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();
    private static final TestMvPartitionStorage TEST_MV_PARTITION_STORAGE = new TestMvPartitionStorage(PART_ID);

    private static SchemaDescriptor schemaDescriptor;
    private static KvMarshaller<Integer, Integer> kvMarshaller;
    private static Lazy<TableSchemaAwareIndexStorage> pkStorage;
    private static PartitionReplicaListener partitionReplicaListener;
    private static Function<BinaryRow, BinaryTuple> row2HashKeyConverter;
    private static Function<BinaryRow, BinaryTuple> row2SortKeyConverter;

    @BeforeAll
    private static void beforeAll() {
        RaftGroupService mockRaftClient = mock(RaftGroupService.class);

        when(mockRaftClient.refreshAndGetLeaderWithTerm())
                .thenAnswer(invocationOnMock -> CompletableFuture.completedFuture(new LeaderWithTerm(null, 1L)));
        when(mockRaftClient.run(any()))
                .thenAnswer(invocationOnMock -> CompletableFuture.completedFuture(null));

        BinaryTupleSchema tupleSchema = BinaryTupleSchema.create(new Element[]{
                new Element(NativeTypes.INT32, false)
        });

        schemaDescriptor = new SchemaDescriptor(1, new Column[]{
                new Column("id".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
        }, new Column[]{
                new Column("val".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
        });

        row2HashKeyConverter = tableRow -> BinaryConverter.forKey(schemaDescriptor).toTuple(tableRow);

        TableSchemaAwareIndexStorage hashIndexStorage = new TableSchemaAwareIndexStorage(
                PK_INDEX_ID,
                new TestHashIndexStorage(null),
                row2HashKeyConverter
        );
        pkStorage = new Lazy<>(() -> hashIndexStorage);

        IndexLocker pkLocker = new HashIndexLocker(PK_INDEX_ID, true, LOCK_MANAGER, row2HashKeyConverter);
        IndexLocker hashIndexLocker = new HashIndexLocker(HASH_INDEX_ID, false, LOCK_MANAGER, row2HashKeyConverter);

        row2SortKeyConverter = tableRow -> BinaryConverter.forValue(schemaDescriptor).toTuple(tableRow);

        TableSchemaAwareIndexStorage sortedIndexStorage = new TableSchemaAwareIndexStorage(
                SORTED_INDEX_ID,
                new TestSortedIndexStorage(
                        new SortedIndexDescriptor(
                                SORTED_INDEX_ID,
                                List.of(new SortedIndexColumnDescriptor(
                                        "val", NativeTypes.INT32, false, true
                                ))
                        )),
                row2SortKeyConverter
        );

        IndexLocker sortedIndexLocker = new SortedIndexLocker(
                SORTED_INDEX_ID,
                LOCK_MANAGER,
                (SortedIndexStorage) sortedIndexStorage.storage(),
                row2SortKeyConverter
        );

        partitionReplicaListener = new PartitionReplicaListener(
                TEST_MV_PARTITION_STORAGE,
                mockRaftClient,
                mock(TxManager.class),
                LOCK_MANAGER,
                Runnable::run,
                PART_ID,
                TABLE_ID,
                () -> Map.of(
                        pkLocker.id(), pkLocker,
                        hashIndexLocker.id(), hashIndexLocker,
                        sortedIndexLocker.id(), sortedIndexLocker
                ),
                pkStorage,
                () -> Map.of(
                        sortedIndexLocker.id(), sortedIndexStorage,
                        hashIndexLocker.id(), hashIndexStorage
                ),
                CLOCK,
                new PendingComparableValuesTracker<>(CLOCK.now()),
                new TestTxStateStorage(),
                mock(PlacementDriver.class),
                peer -> true
        );

        kvMarshaller = new ReflectionMarshallerFactory().create(schemaDescriptor, Integer.class, Integer.class);
    }

    @BeforeEach
    private void beforeTest() {
        ((TestHashIndexStorage) pkStorage.get().storage()).clear();
        TEST_MV_PARTITION_STORAGE.clear();

        locks().forEach(LOCK_MANAGER::release);
    }

    /** Verifies the mode in which the lock was acquired on the index key for a particular operation. */
    @ParameterizedTest
    @MethodSource("readWriteSingleTestArguments")
    public void testReadWriteSingle(ReadWriteTestArg arg) {
        BinaryRow testBinaryRow = binaryRow(1, 1);

        if (arg.type != RequestType.RW_INSERT) {
            var rowId = new RowId(PART_ID);
            insertRows(List.of(new Pair<>(testBinaryRow, rowId)), Timestamp.nextVersion().toUuid());
        }

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                .groupId(PARTITION_ID)
                .term(1L)
                .commitPartitionId(PARTITION_ID)
                .transactionId(TRANSACTION_ID)
                .binaryRow(testBinaryRow)
                .requestType(arg.type)
                .build());

        await(fut);

        assertThat(
                locks(),
                allOf(
                        hasItem(lockThat(
                                arg.expectedLockOnUniqueHash + " on unique hash index",
                                lock -> PK_INDEX_ID.equals(lock.lockKey().contextId())
                                        && lock.lockMode() == arg.expectedLockOnUniqueHash
                        )),
                        hasItem(lockThat(
                                arg.expectedLockOnNonUniqueHash + " on non unique hash index",
                                lock -> HASH_INDEX_ID.equals(lock.lockKey().contextId())
                                        && lock.lockMode() == arg.expectedLockOnNonUniqueHash
                        )),
                        hasItem(lockThat(
                                arg.expectedLockOnSort + " on sorted index",
                                lock -> SORTED_INDEX_ID.equals(lock.lockKey().contextId())
                                        && lock.lockMode() == arg.expectedLockOnSort
                        ))
                )
        );
    }

    /** Verifies the mode in which the lock was acquired on the index key for a particular operation. */
    @ParameterizedTest
    @MethodSource("readWriteMultiTestArguments")
    public void testReadWriteMulti(ReadWriteTestArg arg) {
        List<BinaryRow> rows = List.of(binaryRow(1, 1), binaryRow(2, 2), binaryRow(3, 3));

        if (arg.type != RequestType.RW_INSERT_ALL) {
            for (BinaryRow row : rows) {
                var rowId = new RowId(PART_ID);
                insertRows(List.of(new Pair<>(row, rowId)), Timestamp.nextVersion().toUuid());
            }
        }

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                .groupId(PARTITION_ID)
                .term(1L)
                .commitPartitionId(PARTITION_ID)
                .transactionId(TRANSACTION_ID)
                .binaryRows(rows)
                .requestType(arg.type)
                .build());

        await(fut);

        for (BinaryRow row : rows) {
            assertThat(
                    locks(),
                    allOf(
                            hasItem(lockThat(
                                    arg.expectedLockOnUniqueHash + " on unique hash index",
                                    lock -> PK_INDEX_ID.equals(lock.lockKey().contextId())
                                            && row2HashKeyConverter.apply(row).byteBuffer().equals(lock.lockKey().key())
                                            && lock.lockMode() == arg.expectedLockOnUniqueHash
                            )),
                            hasItem(lockThat(
                                    arg.expectedLockOnNonUniqueHash + " on non unique hash index",
                                    lock -> HASH_INDEX_ID.equals(lock.lockKey().contextId())
                                            && row2HashKeyConverter.apply(row).byteBuffer().equals(lock.lockKey().key())
                                            && lock.lockMode() == arg.expectedLockOnNonUniqueHash
                            )),
                            hasItem(lockThat(
                                    arg.expectedLockOnSort + " on sorted index",
                                    lock -> SORTED_INDEX_ID.equals(lock.lockKey().contextId())
                                            && row2SortKeyConverter.apply(row).byteBuffer().equals(lock.lockKey().key())
                                            && lock.lockMode() == arg.expectedLockOnSort
                            ))
                    )
            );
        }
    }

    private static Iterable<ReadWriteTestArg> readWriteSingleTestArguments() {
        return List.of(
                new ReadWriteTestArg(RequestType.RW_DELETE, LockMode.X, LockMode.IX, LockMode.IX),
                new ReadWriteTestArg(RequestType.RW_DELETE_EXACT, LockMode.X, LockMode.IX, LockMode.IX),
                new ReadWriteTestArg(RequestType.RW_INSERT, LockMode.X, LockMode.IX, LockMode.X),
                new ReadWriteTestArg(RequestType.RW_UPSERT, LockMode.X, LockMode.IX, LockMode.X),
                new ReadWriteTestArg(RequestType.RW_REPLACE_IF_EXIST, LockMode.X, LockMode.IX, LockMode.X),

                new ReadWriteTestArg(RequestType.RW_GET_AND_DELETE, LockMode.X, LockMode.IX, LockMode.IX),
                new ReadWriteTestArg(RequestType.RW_GET_AND_REPLACE, LockMode.X, LockMode.IX, LockMode.X),
                new ReadWriteTestArg(RequestType.RW_GET_AND_UPSERT, LockMode.X, LockMode.IX, LockMode.X)
        );
    }

    private static Iterable<ReadWriteTestArg> readWriteMultiTestArguments() {
        return List.of(
                new ReadWriteTestArg(RequestType.RW_DELETE_ALL, LockMode.X, LockMode.IX, LockMode.IX),
                new ReadWriteTestArg(RequestType.RW_DELETE_EXACT_ALL, LockMode.X, LockMode.IX, LockMode.IX),
                new ReadWriteTestArg(RequestType.RW_INSERT_ALL, LockMode.X, LockMode.IX, LockMode.X),
                new ReadWriteTestArg(RequestType.RW_UPSERT_ALL, LockMode.X, LockMode.IX, LockMode.X)
        );
    }

    private List<Lock> locks() {
        List<Lock> locks = new ArrayList<>();

        Iterator<Lock> it = LOCK_MANAGER.locks(TRANSACTION_ID);

        while (it.hasNext()) {
            locks.add(it.next());
        }

        return locks;
    }

    private void insertRows(List<Pair<BinaryRow, RowId>> rows, UUID txId) {
        HybridTimestamp commitTs = CLOCK.now();

        for (Pair<BinaryRow, RowId> row : rows) {
            BinaryRow tableRow = row.getFirst();
            RowId rowId = row.getSecond();

            pkStorage.get().put(tableRow, rowId);
            TEST_MV_PARTITION_STORAGE.addWrite(rowId, tableRow, txId, TABLE_ID, PART_ID);
            TEST_MV_PARTITION_STORAGE.commitWrite(rowId, commitTs);
        }
    }

    protected static BinaryRow binaryRow(Integer key, Integer value) {
        try {
            return kvMarshaller.marshal(key, value);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    protected static Integer key(BinaryRow binaryRow) {
        try {
            return kvMarshaller.unmarshalKey(new Row(schemaDescriptor, binaryRow));
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    protected static Integer value(BinaryRow binaryRow) {
        try {
            return kvMarshaller.unmarshalValue(new Row(schemaDescriptor, binaryRow));
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    private static Matcher<Lock> lockThat(String description, Function<Lock, Boolean> checker) {
        return new CustomMatcher<>(description) {
            @Override
            public boolean matches(Object actual) {
                return actual instanceof Lock && checker.apply((Lock) actual) == Boolean.TRUE;
            }
        };
    }

    static class ReadWriteTestArg {
        private final RequestType type;
        private final LockMode expectedLockOnUniqueHash;
        private final LockMode expectedLockOnNonUniqueHash;
        private final LockMode expectedLockOnSort;

        public ReadWriteTestArg(
                RequestType type,
                LockMode expectedLockOnUniqueHash,
                LockMode expectedLockOnNonUniqueHash,
                LockMode expectedLockOnSort
        ) {
            this.type = type;
            this.expectedLockOnUniqueHash = expectedLockOnUniqueHash;
            this.expectedLockOnNonUniqueHash = expectedLockOnNonUniqueHash;
            this.expectedLockOnSort = expectedLockOnSort;
        }

        @Override
        public String toString() {
            return type.toString();
        }
    }
}
