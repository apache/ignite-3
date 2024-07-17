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

package org.apache.ignite.internal.storage.rocksdb;

import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.comparatorFromCfName;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexCfName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbBinaryTupleComparator;
import org.apache.ignite.internal.testframework.VariableSource;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

/**
 * Unit test for {@link ColumnFamilyUtils}.
 */
public class ColumnFamilyUtilsTest {
    @SuppressWarnings("unused")
    public static final List<NativeType> ALL_TYPES = SchemaTestUtils.ALL_TYPES;

    @ParameterizedTest
    @VariableSource("ALL_TYPES")
    void testSortedIndexCfNameSingleType(NativeType nativeType) {
        var descriptor = new StorageSortedIndexColumnDescriptor("<unused>", nativeType, false, false);

        assertArrayEquals(name(nativeType.spec().ordinal(), 0), sortedIndexCfName(List.of(descriptor)));
    }

    @Test
    void testSortedIndexCfNameFlags() {
        List<StorageSortedIndexColumnDescriptor> descriptors = List.of(
                new StorageSortedIndexColumnDescriptor("<unused>", NativeTypes.INT64, false, false),
                new StorageSortedIndexColumnDescriptor("<unused>", NativeTypes.INT32, true, false),
                new StorageSortedIndexColumnDescriptor("<unused>", NativeTypes.INT16, false, true),
                new StorageSortedIndexColumnDescriptor("<unused>", NativeTypes.INT8, true, true)
        );

        assertArrayEquals(name(3, 0, 2, 1, 1, 2, 0, 3), sortedIndexCfName(descriptors));
    }

    @Test
    void testComparatorFromCfName() {
        RocksDbBinaryTupleComparator comparator = comparatorFromCfName(name(3, 0, 2, 1, 1, 2, 0, 3));

        // I am sorry, this is for a single test only.
        List<StorageSortedIndexColumnDescriptor> columns = getFieldValue(comparator, "comparator", "columns");

        assertEquals(NativeTypes.INT64, columns.get(0).type());
        assertEquals(NativeTypes.INT32, columns.get(1).type());
        assertEquals(NativeTypes.INT16, columns.get(2).type());
        assertEquals(NativeTypes.INT8, columns.get(3).type());

        assertFalse(columns.get(0).nullable());
        assertTrue(columns.get(1).nullable());
        assertFalse(columns.get(2).nullable());
        assertTrue(columns.get(3).nullable());

        assertFalse(columns.get(0).asc());
        assertFalse(columns.get(1).asc());
        assertTrue(columns.get(2).asc());
        assertTrue(columns.get(3).asc());
    }

    private static byte[] name(int... bytes) {
        var buf = ByteBuffer.allocate("cf-sorted-".length() + bytes.length).put("cf-sorted-".getBytes(StandardCharsets.UTF_8));

        for (int val : bytes) {
            buf.put((byte) val);
        }

        return buf.array();
    }
}
