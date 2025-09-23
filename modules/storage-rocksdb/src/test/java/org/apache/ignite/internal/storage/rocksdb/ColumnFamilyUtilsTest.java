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

import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_FIRST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.DESC_NULLS_FIRST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.DESC_NULLS_LAST;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.comparatorFromCfName;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexCfName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbBinaryTupleComparator;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit test for {@link ColumnFamilyUtils}.
 */
public class ColumnFamilyUtilsTest {
    private static List<NativeType> allNativeTypes() {
        return SchemaTestUtils.ALL_TYPES;
    }

    @ParameterizedTest
    @MethodSource("allNativeTypes")
    void testSortedIndexCfNameSingleType(NativeType nativeType) {
        var descriptor = new StorageSortedIndexColumnDescriptor("<unused>", nativeType, false, false, false);

        assertArrayEquals(name(nativeType.spec().id(), 0), sortedIndexCfName(List.of(descriptor)));
    }

    @Test
    void testSortedIndexCfNameFlags() {
        List<StorageSortedIndexColumnDescriptor> descriptors = List.of(
                new StorageSortedIndexColumnDescriptor("<unused>", NativeTypes.INT64, false, false, false),
                new StorageSortedIndexColumnDescriptor("<unused>", NativeTypes.INT32, true, false, true),
                new StorageSortedIndexColumnDescriptor("<unused>", NativeTypes.INT16, false, true, false),
                new StorageSortedIndexColumnDescriptor("<unused>", NativeTypes.INT8, true, true, true)
        );

        assertArrayEquals(name(ColumnType.INT64.id(), 0,
                ColumnType.INT32.id(), 5, ColumnType.INT16.id(), 2, ColumnType.INT8.id(), 7), sortedIndexCfName(descriptors));
    }

    @Test
    void testComparatorFromCfName() {
        RocksDbBinaryTupleComparator comparator = comparatorFromCfName(
                name(ColumnType.INT64.id(), 0, ColumnType.INT32.id(), 5, ColumnType.INT16.id(), 2, ColumnType.INT8.id(), 7));

        List<NativeType> expectedTypes = List.of(NativeTypes.INT64, NativeTypes.INT32, NativeTypes.INT16, NativeTypes.INT8);
        List<CatalogColumnCollation> expectedCollations = List.of(DESC_NULLS_LAST, DESC_NULLS_FIRST, ASC_NULLS_LAST, ASC_NULLS_FIRST);

        // I am sorry, this is for a single test only.
        List<NativeType> columnTypes = getFieldValue(comparator, "comparator", "columnTypes");
        List<CatalogColumnCollation> columnCollations = getFieldValue(comparator, "comparator", "columnCollations");

        assertEquals(expectedTypes, columnTypes);
        assertEquals(expectedCollations, columnCollations);
    }

    private static byte[] name(int... bytes) {
        var buf = ByteBuffer.allocate("cf-sorted-".length() + bytes.length).put("cf-sorted-".getBytes(StandardCharsets.UTF_8));

        for (int val : bytes) {
            buf.put((byte) val);
        }

        return buf.array();
    }
}
