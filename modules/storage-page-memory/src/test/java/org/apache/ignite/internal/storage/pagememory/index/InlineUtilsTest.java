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

package org.apache.ignite.internal.storage.pagememory.index;

import static org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo.CHILD_LINK_SIZE;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.BIG_NUMBER_INLINE_SIZE;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.MAX_BINARY_TUPLE_INLINE_SIZE;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.MAX_VARLEN_INLINE_SIZE;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.binaryTupleInlineSize;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.inlineSize;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.innerNodePayloadSize;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.leafNodePayloadSize;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.optimizeItemSize;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor.StorageColumnDescriptor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * For {@link InlineUtils} testing.
 */
public class InlineUtilsTest extends BaseIgniteAbstractTest {
    @Test
    void testInlineSizeForNativeType() {
        EnumSet<NativeTypeSpec> nativeTypeSpecs = EnumSet.allOf(NativeTypeSpec.class);

        // Fixed length type checking.

        NativeType nativeType = NativeTypes.BOOLEAN;

        assertEquals(1, inlineSize(nativeType));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(1, inlineSize(nativeType = NativeTypes.INT8));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(2, inlineSize(nativeType = NativeTypes.INT16));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(4, inlineSize(nativeType = NativeTypes.INT32));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(8, inlineSize(nativeType = NativeTypes.INT64));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(4, inlineSize(nativeType = NativeTypes.FLOAT));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(8, inlineSize(nativeType = NativeTypes.DOUBLE));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(16, inlineSize(nativeType = NativeTypes.UUID));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(1, inlineSize(nativeType = NativeTypes.bitmaskOf(8)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(3, inlineSize(nativeType = NativeTypes.DATE));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(4, inlineSize(nativeType = NativeTypes.time(0)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(7, inlineSize(nativeType = NativeTypes.datetime(3)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(12, inlineSize(nativeType = NativeTypes.timestamp(3)));
        nativeTypeSpecs.remove(nativeType.spec());

        // Variable length type checking.

        assertEquals(2 + BIG_NUMBER_INLINE_SIZE, inlineSize(nativeType = NativeTypes.decimalOf(1, 1)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(2 + BIG_NUMBER_INLINE_SIZE, inlineSize(nativeType = NativeTypes.decimalOf(100, 1)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(7, inlineSize(nativeType = NativeTypes.stringOf(7)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(MAX_VARLEN_INLINE_SIZE, inlineSize(nativeType = NativeTypes.stringOf(256)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(MAX_VARLEN_INLINE_SIZE, inlineSize(nativeType = NativeTypes.stringOf(Integer.MAX_VALUE)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(9, inlineSize(nativeType = NativeTypes.blobOf(9)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(MAX_VARLEN_INLINE_SIZE, inlineSize(nativeType = NativeTypes.blobOf(256)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(MAX_VARLEN_INLINE_SIZE, inlineSize(nativeType = NativeTypes.blobOf(Integer.MAX_VALUE)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(BIG_NUMBER_INLINE_SIZE, inlineSize(nativeType = NativeTypes.numberOf(1)));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(BIG_NUMBER_INLINE_SIZE, inlineSize(nativeType = NativeTypes.numberOf(100)));
        nativeTypeSpecs.remove(nativeType.spec());

        // Let's check that all types have been checked.
        assertThat(nativeTypeSpecs, empty());
    }

    @Test
    void testBinaryTupleInlineSize() {
        StorageIndexDescriptor indexDescriptor = testIndexDescriptor(testColumnDescriptor(NativeTypes.INT8, false));

        assertEquals(
                BinaryTupleCommon.HEADER_SIZE + 1 + NativeTypes.INT8.sizeInBytes(), // Without a nullMap card.
                binaryTupleInlineSize(indexDescriptor)
        );

        indexDescriptor = testIndexDescriptor(testColumnDescriptor(NativeTypes.INT32, true));

        assertEquals(
                BinaryTupleCommon.HEADER_SIZE + 1 + NativeTypes.INT32.sizeInBytes(), // With a nullMap card.
                binaryTupleInlineSize(indexDescriptor)
        );

        // Let's check the 2-byte entry size for the BinaryTuple offset table.

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.stringOf(MAX_VARLEN_INLINE_SIZE), false),
                testColumnDescriptor(NativeTypes.stringOf(MAX_VARLEN_INLINE_SIZE), false),
                testColumnDescriptor(NativeTypes.stringOf(MAX_VARLEN_INLINE_SIZE), false),
                testColumnDescriptor(NativeTypes.stringOf(MAX_VARLEN_INLINE_SIZE), false)
        );

        assertEquals(
                BinaryTupleCommon.HEADER_SIZE + 4 * 2 + 4 * MAX_VARLEN_INLINE_SIZE, // Without a nullMap card.
                binaryTupleInlineSize(indexDescriptor)
        );

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.stringOf(MAX_VARLEN_INLINE_SIZE), false),
                testColumnDescriptor(NativeTypes.stringOf(MAX_VARLEN_INLINE_SIZE), true),
                testColumnDescriptor(NativeTypes.stringOf(MAX_VARLEN_INLINE_SIZE), false),
                testColumnDescriptor(NativeTypes.stringOf(MAX_VARLEN_INLINE_SIZE), false)
        );

        assertEquals(
                BinaryTupleCommon.HEADER_SIZE + 4 * 2 + 4 * MAX_VARLEN_INLINE_SIZE, // With a nullMap card.
                binaryTupleInlineSize(indexDescriptor)
        );

        // Let's check that it does not exceed the MAX_BINARY_TUPLE_INLINE_SIZE.

        StorageColumnDescriptor[] columnDescriptors = IntStream.range(0, MAX_BINARY_TUPLE_INLINE_SIZE / MAX_VARLEN_INLINE_SIZE)
                .mapToObj(i -> NativeTypes.stringOf(MAX_VARLEN_INLINE_SIZE))
                .map(nativeType -> testColumnDescriptor(nativeType, false))
                .toArray(StorageColumnDescriptor[]::new);

        assertEquals(
                MAX_BINARY_TUPLE_INLINE_SIZE, // Without a nullMap card.
                binaryTupleInlineSize(testIndexDescriptor(columnDescriptors))
        );

        columnDescriptors = IntStream.range(0, MAX_BINARY_TUPLE_INLINE_SIZE / MAX_VARLEN_INLINE_SIZE)
                .mapToObj(i -> NativeTypes.stringOf(MAX_VARLEN_INLINE_SIZE))
                .map(nativeType -> testColumnDescriptor(nativeType, true))
                .toArray(StorageColumnDescriptor[]::new);

        assertEquals(
                MAX_BINARY_TUPLE_INLINE_SIZE, // With a nullMap card.
                binaryTupleInlineSize(testIndexDescriptor(columnDescriptors))
        );
    }

    @Test
    void testInnerNodePayloadSize() {
        int pageSize = 1024;

        assertEquals(pageSize - BplusInnerIo.HEADER_SIZE, innerNodePayloadSize(pageSize));

        pageSize = 128;

        assertEquals(pageSize - BplusInnerIo.HEADER_SIZE, innerNodePayloadSize(pageSize));
    }

    @Test
    void testLeafNodePayloadSize() {
        int pageSize = 1024;

        assertEquals(pageSize - BplusLeafIo.HEADER_SIZE, leafNodePayloadSize(pageSize));

        pageSize = 128;

        assertEquals(pageSize - BplusLeafIo.HEADER_SIZE, leafNodePayloadSize(pageSize));
    }

    @Test
    void testBinaryTupleInlineSizeForBplusTree() {
        int pageSize = 1024;
        int itemHeaderSize = 6;

        // Let's check without variable length columns.

        StorageIndexDescriptor indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.INT64, false),
                testColumnDescriptor(NativeTypes.UUID, false)
        );

        assertEquals(
                BinaryTupleCommon.HEADER_SIZE + 2 + NativeTypes.INT64.sizeInBytes() + NativeTypes.UUID.sizeInBytes(),
                binaryTupleInlineSize(pageSize, itemHeaderSize, indexDescriptor)
        );

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.INT64, false),
                testColumnDescriptor(NativeTypes.UUID, false),
                testColumnDescriptor(NativeTypes.UUID, false),
                testColumnDescriptor(NativeTypes.UUID, false),
                testColumnDescriptor(NativeTypes.UUID, true)
        );

        assertEquals(
                (((innerNodePayloadSize(128) - CHILD_LINK_SIZE) / 2) - CHILD_LINK_SIZE) - itemHeaderSize,
                binaryTupleInlineSize(128, itemHeaderSize, indexDescriptor)
        );

        // Let's check without variable length columns.

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.stringOf(32), true)
        );

        assertEquals(
                (((innerNodePayloadSize(128) - CHILD_LINK_SIZE) / 2) - CHILD_LINK_SIZE) - itemHeaderSize,
                binaryTupleInlineSize(128, itemHeaderSize, indexDescriptor)
        );

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.INT64, false),
                testColumnDescriptor(NativeTypes.UUID, false),
                testColumnDescriptor(NativeTypes.UUID, false),
                testColumnDescriptor(NativeTypes.UUID, false),
                testColumnDescriptor(NativeTypes.stringOf(33), true)
        );

        assertEquals(
                BinaryTupleCommon.HEADER_SIZE + 5 + NativeTypes.INT64.sizeInBytes() + 3 * NativeTypes.UUID.sizeInBytes() + 33 + 6,
                binaryTupleInlineSize(pageSize, itemHeaderSize, indexDescriptor)
        );
    }

    @Test
    void testOptimizeItemSize() {
        assertEquals(100, optimizeItemSize(1000, 100));

        assertEquals(333, optimizeItemSize(1000, 330));
    }

    private static StorageIndexDescriptor testIndexDescriptor(StorageColumnDescriptor... columnDescriptors) {
        StorageIndexDescriptor indexDescriptor = mock(StorageIndexDescriptor.class);

        when(indexDescriptor.columns()).then(answer -> List.of(columnDescriptors));

        return indexDescriptor;
    }

    private static StorageColumnDescriptor testColumnDescriptor(NativeType nativeType, boolean nullable) {
        StorageColumnDescriptor columnDescriptor = mock(StorageColumnDescriptor.class);

        when(columnDescriptor.type()).thenReturn(nativeType);
        when(columnDescriptor.nullable()).thenReturn(nullable);

        return columnDescriptor;
    }
}
