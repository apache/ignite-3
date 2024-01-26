package org.apache.ignite.internal.table.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.storage.util.LockByRowId;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.replicator.TimedBinaryRow;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link StorageUpdateHandler}.
 */
public class StorageUpdateHandlerTest extends BaseMvStoragesTest {
    private static final HybridClock CLOCK = new HybridClockImpl();

    protected static final int PARTITION_ID = 0;

    private static final BinaryTupleSchema TUPLE_SCHEMA = BinaryTupleSchema.createRowSchema(SCHEMA_DESCRIPTOR);

    private static final BinaryTupleSchema PK_INDEX_SCHEMA = BinaryTupleSchema.createKeySchema(SCHEMA_DESCRIPTOR);

    private static final ColumnsExtractor PK_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, PK_INDEX_SCHEMA);

    private static final int[] USER_INDEX_COLS = {
            SCHEMA_DESCRIPTOR.column("INTVAL").schemaIndex(),
            SCHEMA_DESCRIPTOR.column("STRVAL").schemaIndex()
    };

    private static final BinaryTupleSchema USER_INDEX_SCHEMA = BinaryTupleSchema.createSchema(SCHEMA_DESCRIPTOR, USER_INDEX_COLS);

    private static final ColumnsExtractor USER_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, USER_INDEX_SCHEMA);

    private TestHashIndexStorage pkInnerStorage;
    private TestSortedIndexStorage sortedInnerStorage;
    private TestHashIndexStorage hashInnerStorage;
    private TestMvPartitionStorage storage;
    private StorageUpdateHandler storageUpdateHandler;
    private IndexUpdateHandler indexUpdateHandler;
    private LockByRowId lock;

    @InjectConfiguration
    private StorageUpdateConfiguration storageUpdateConfiguration;

    @BeforeEach
    void setUp() {
        int tableId = 1;
        int pkIndexId = 2;
        int sortedIndexId = 3;
        int hashIndexId = 4;

        pkInnerStorage = new TestHashIndexStorage(PARTITION_ID, new StorageHashIndexDescriptor(pkIndexId, List.of(
                new StorageHashIndexColumnDescriptor("INTKEY", NativeTypes.INT32, false),
                new StorageHashIndexColumnDescriptor("STRKEY", NativeTypes.STRING, false)
        )));

        TableSchemaAwareIndexStorage pkStorage = new TableSchemaAwareIndexStorage(
                pkIndexId,
                pkInnerStorage,
                PK_INDEX_BINARY_TUPLE_CONVERTER
        );

        sortedInnerStorage = new TestSortedIndexStorage(PARTITION_ID, new StorageSortedIndexDescriptor(sortedIndexId, List.of(
                new StorageSortedIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false, true),
                new StorageSortedIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false, true)
        )));

        TableSchemaAwareIndexStorage sortedIndexStorage = new TableSchemaAwareIndexStorage(
                sortedIndexId,
                sortedInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER
        );

        hashInnerStorage = new TestHashIndexStorage(PARTITION_ID, new StorageHashIndexDescriptor(hashIndexId, List.of(
                new StorageHashIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false),
                new StorageHashIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false)
        )));

        TableSchemaAwareIndexStorage hashIndexStorage = new TableSchemaAwareIndexStorage(
                hashIndexId,
                hashInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER
        );

        lock = spy(new LockByRowId());
        storage = spy(new TestMvPartitionStorage(PARTITION_ID, lock));

        Map<Integer, TableSchemaAwareIndexStorage> indexes = Map.of(
                pkIndexId, pkStorage,
                sortedIndexId, sortedIndexStorage,
                hashIndexId, hashIndexStorage
        );

        TestPartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(tableId, PARTITION_ID, storage);

        indexUpdateHandler = spy(new IndexUpdateHandler(DummyInternalTableImpl.createTableIndexStoragesSupplier(indexes)));

        storageUpdateHandler = new StorageUpdateHandler(
                PARTITION_ID,
                partitionDataStorage,
                indexUpdateHandler,
                storageUpdateConfiguration
        );
    }

    @Test
    void testUpdateAllBatchedTryLockFailedOnce() {
        UUID txUuid = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));
        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));
        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        TimedBinaryRow tb1 = new TimedBinaryRow(row1, null);
        TimedBinaryRow tb2 = new TimedBinaryRow(row2, null);
        TimedBinaryRow tb3 = new TimedBinaryRow(row3, null);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        UUID id3 = UUID.randomUUID();

        Map<UUID, TimedBinaryRow> rowsToUpdate = Map.of(
                id1, tb1,
                id2, tb2,
                id3, tb3
        );
        // All batches consist of one row.
        storageUpdateConfiguration.batchByteLength().update(0);

        when(lock.tryLock(new RowId(partitionId.partitionId(), id1))).thenReturn(false).thenCallRealMethod();
        when(lock.tryLock(new RowId(partitionId.partitionId(), id2))).thenReturn(false).thenCallRealMethod();
        when(lock.tryLock(new RowId(partitionId.partitionId(), id3))).thenReturn(false).thenCallRealMethod();

        storageUpdateHandler.handleUpdateAll(txUuid, rowsToUpdate, partitionId, true, null, null);

        assertEquals(3, storage.rowsCount());
        // We have three writes to the storage.
        verify(storage, times(3)).addWrite(any(), any(), any(), anyInt(), anyInt());

        // First batch uses lock() instead, second and third get lock after second try
        verify(lock, times(4)).tryLock(any());

        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs);

        assertEquals(3, storage.rowsCount());
        // Those writes resulted in three commits.
        verify(storage, times(3)).commitWrite(any(), any());

        ReadResult result1 = storage.read(new RowId(partitionId.partitionId(), id1), HybridTimestamp.MAX_VALUE);
        assertEquals(row1, result1.binaryRow());

        ReadResult result2 = storage.read(new RowId(partitionId.partitionId(), id2), HybridTimestamp.MAX_VALUE);
        assertEquals(row2, result2.binaryRow());

        ReadResult result3 = storage.read(new RowId(partitionId.partitionId(), id3), HybridTimestamp.MAX_VALUE);
        assertEquals(row3, result3.binaryRow());
    }

}
