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

package org.apache.ignite.internal.storage;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerFactory;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base test for MV storages, contains pojo classes, their descriptor and a marshaller instance.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class BaseMvStoragesTest extends BaseIgniteAbstractTest {
    /** Default reflection marshaller factory. */
    private static final MarshallerFactory MARSHALLER_FACTORY = new ReflectionMarshallerFactory();

    /** Schema descriptor for tests. */
    protected static final SchemaDescriptor SCHEMA_DESCRIPTOR = new SchemaDescriptor(1, new Column[]{
            new Column("INTKEY", NativeTypes.INT32, false),
            new Column("STRKEY", NativeTypes.STRING, false),
    }, new Column[]{
            new Column("INTVAL", NativeTypes.INT32, false),
            new Column("STRVAL", NativeTypes.STRING, false),
    });

    /** Key-value marshaller for tests. */
    private static final KvMarshaller<TestKey, TestValue> KV_MARSHALLER
            = MARSHALLER_FACTORY.create(SCHEMA_DESCRIPTOR, TestKey.class, TestValue.class);

    /** Hybrid clock to generate timestamps. */
    protected static final HybridClock CLOCK = new HybridClockImpl();

    protected static BinaryRow binaryRow(TestKey key, TestValue value) {
        return KV_MARSHALLER.marshal(key, value);
    }

    protected static IndexRow indexRow(StorageIndexDescriptor indexDescriptor, BinaryRow binaryRow, RowId rowId) {
        int[] columnIndexes = indexDescriptor.columns().stream()
                .mapToInt(indexColumnDescriptor -> {
                    Column column = SCHEMA_DESCRIPTOR.column(indexColumnDescriptor.name());

                    assertNotNull(column, column.name());

                    return column.positionInRow();
                })
                .toArray();

        ColumnsExtractor converter = BinaryRowConverter.columnsExtractor(SCHEMA_DESCRIPTOR, columnIndexes);
        return new IndexRowImpl(converter.extractColumns(binaryRow), rowId);
    }

    protected static TestKey key(BinaryRow binaryRow) {
        return KV_MARSHALLER.unmarshalKey(Row.wrapBinaryRow(SCHEMA_DESCRIPTOR, binaryRow));
    }

    @Nullable
    protected static TestValue value(BinaryRow binaryRow) {
        return KV_MARSHALLER.unmarshalValue(Row.wrapBinaryRow(SCHEMA_DESCRIPTOR, binaryRow));
    }

    protected static @Nullable IgniteBiTuple<TestKey, TestValue> unwrap(@Nullable BinaryRow binaryRow) {
        if (binaryRow == null) {
            return null;
        }

        return new IgniteBiTuple<>(key(binaryRow), value(binaryRow));
    }

    protected static @Nullable IgniteBiTuple<TestKey, TestValue> unwrap(@Nullable ReadResult readResult) {
        if (readResult == null) {
            return null;
        }

        BinaryRow binaryRow = readResult.binaryRow();

        if (binaryRow == null) {
            return null;
        }

        return new IgniteBiTuple<>(key(binaryRow), value(binaryRow));
    }

    protected static List<IgniteBiTuple<TestKey, TestValue>> drainToList(Cursor<ReadResult> cursor) {
        try (cursor) {
            return cursor.stream().map(BaseMvStoragesTest::unwrap).collect(Collectors.toList());
        }
    }

    /**
     * Test pojo key.
     */
    protected static class TestKey {
        @IgniteToStringInclude
        public int intKey;

        @IgniteToStringInclude
        public String strKey;

        public TestKey() {
        }

        public TestKey(int intKey, String strKey) {
            this.intKey = intKey;
            this.strKey = strKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestKey testKey = (TestKey) o;
            return intKey == testKey.intKey && Objects.equals(strKey, testKey.strKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intKey, strKey);
        }

        @Override
        public String toString() {
            return S.toString(TestKey.class, this);
        }
    }

    /**
     * Test pojo value.
     */
    protected static class TestValue implements Comparable<TestValue> {
        @IgniteToStringInclude
        public Integer intVal;

        @IgniteToStringInclude
        public String strVal;

        public TestValue() {
        }

        public TestValue(Integer intVal, String strVal) {
            this.intVal = intVal;
            this.strVal = strVal;
        }

        @Override
        public int compareTo(TestValue o) {
            int cmp = Integer.compare(intVal, o.intVal);

            return cmp != 0 ? cmp : strVal.compareTo(o.strVal);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestValue testValue = (TestValue) o;
            return Objects.equals(intVal, testValue.intVal) && Objects.equals(strVal, testValue.strVal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intVal, strVal);
        }

        @Override
        public String toString() {
            return S.toString(TestValue.class, this);
        }
    }

    /**
     * Retrieves or creates a multi-versioned partition storage.
     */
    public static MvPartitionStorage getOrCreateMvPartition(MvTableStorage tableStorage, int partitionId) {
        MvPartitionStorage mvPartition = tableStorage.getMvPartition(partitionId);

        if (mvPartition != null) {
            return mvPartition;
        }

        CompletableFuture<MvPartitionStorage> createMvPartitionStorageFuture = tableStorage.createMvPartition(partitionId);

        assertThat(createMvPartitionStorageFuture, willCompleteSuccessfully());

        return createMvPartitionStorageFuture.join();
    }

    /**
     * Creates a new transaction id.
     */
    public static UUID newTransactionId() {
        return TransactionIds.transactionId(CLOCK.now(), 0);
    }
}
