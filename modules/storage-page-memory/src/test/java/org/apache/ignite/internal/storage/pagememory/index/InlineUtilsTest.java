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

import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.MAX_BINARY_TUPLE_INLINE_SIZE;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.binaryTupleBplusIoInlineSize;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.binaryTupleInlineSize;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.inlineSize;
import static org.apache.ignite.internal.util.Constants.KiB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.List;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.storage.index.IndexDescriptor;
import org.apache.ignite.internal.storage.index.IndexDescriptor.ColumnDescriptor;
import org.junit.jupiter.api.Test;

/**
 * For {@link InlineUtils} testing.
 */
public class InlineUtilsTest {
    @Test
    void testInlineSizeForNativeType() {
        EnumSet<NativeTypeSpec> nativeTypeSpecs = EnumSet.allOf(NativeTypeSpec.class);

        // Fixed length type checking.

        NativeType nativeType = NativeTypes.INT8;

        int maxVarlenInlineSize = 10;

        assertEquals(1, inlineSize(nativeType, maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(2, inlineSize(nativeType = NativeTypes.INT16, maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(4, inlineSize(nativeType = NativeTypes.INT32, maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(8, inlineSize(nativeType = NativeTypes.INT64, maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(4, inlineSize(nativeType = NativeTypes.FLOAT, maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(8, inlineSize(nativeType = NativeTypes.DOUBLE, maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(16, inlineSize(nativeType = NativeTypes.UUID, maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(1, inlineSize(nativeType = NativeTypes.bitmaskOf(8), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(3, inlineSize(nativeType = NativeTypes.DATE, maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(4, inlineSize(nativeType = NativeTypes.time(), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(9, inlineSize(nativeType = NativeTypes.datetime(), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(12, inlineSize(nativeType = NativeTypes.timestamp(), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        // Variable length type checking.

        assertEquals(4, inlineSize(nativeType = NativeTypes.decimalOf(1, 1), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(4, inlineSize(nativeType = NativeTypes.decimalOf(100, 1), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(7, inlineSize(nativeType = NativeTypes.stringOf(7), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(maxVarlenInlineSize, inlineSize(nativeType = NativeTypes.stringOf(Integer.MAX_VALUE), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(9, inlineSize(nativeType = NativeTypes.blobOf(9), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(maxVarlenInlineSize, inlineSize(nativeType = NativeTypes.blobOf(Integer.MAX_VALUE), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(4, inlineSize(nativeType = NativeTypes.numberOf(1), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        assertEquals(4, inlineSize(nativeType = NativeTypes.numberOf(100), maxVarlenInlineSize));
        nativeTypeSpecs.remove(nativeType.spec());

        // Let's check that all types have been checked.
        assertThat(nativeTypeSpecs, empty());
    }

    @Test
    void testInlineSizeForBinaryTuple() {
        IndexDescriptor indexDescriptor = testIndexDescriptor();

        int maxVarlenInlineSize = 10;

        assertEquals(1, binaryTupleInlineSize(indexDescriptor, maxVarlenInlineSize));

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.INT8, false),
                testColumnDescriptor(NativeTypes.UUID, false)
        );

        assertEquals(20, binaryTupleInlineSize(indexDescriptor, maxVarlenInlineSize));

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.INT8, true),
                testColumnDescriptor(NativeTypes.UUID, false)
        );

        assertEquals(21, binaryTupleInlineSize(indexDescriptor, maxVarlenInlineSize));

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.INT8, true),
                testColumnDescriptor(NativeTypes.stringOf(100), false),
                testColumnDescriptor(NativeTypes.INT64, true)
        );

        assertEquals(27, binaryTupleInlineSize(indexDescriptor, maxVarlenInlineSize));

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.stringOf(MAX_BINARY_TUPLE_INLINE_SIZE), false),
                testColumnDescriptor(NativeTypes.stringOf(MAX_BINARY_TUPLE_INLINE_SIZE), false)
        );

        assertEquals(MAX_BINARY_TUPLE_INLINE_SIZE, binaryTupleInlineSize(indexDescriptor, 4 * KiB));

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.stringOf(MAX_BINARY_TUPLE_INLINE_SIZE), true),
                testColumnDescriptor(NativeTypes.stringOf(MAX_BINARY_TUPLE_INLINE_SIZE), false)
        );

        assertEquals(MAX_BINARY_TUPLE_INLINE_SIZE, binaryTupleInlineSize(indexDescriptor, 4 * KiB));
    }

    @Test
    void testBinaryTupleBplusIoInlineSize() {
        IndexDescriptor indexDescriptor = testIndexDescriptor(testColumnDescriptor(NativeTypes.INT8, true));

        assertEquals(4, binaryTupleBplusIoInlineSize(100, 5, indexDescriptor, 10));

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.INT8, true),
                testColumnDescriptor(NativeTypes.stringOf(5), true)
        );

        assertEquals(8, binaryTupleBplusIoInlineSize(100, 5, indexDescriptor, 10));

        assertEquals(12, binaryTupleBplusIoInlineSize(200, 5, indexDescriptor, 10));

        indexDescriptor = testIndexDescriptor(
                testColumnDescriptor(NativeTypes.stringOf(64), true),
                testColumnDescriptor(NativeTypes.stringOf(49), true)
        );

        assertEquals(122, binaryTupleBplusIoInlineSize(462, 5, indexDescriptor, 64));
    }

    private static IndexDescriptor testIndexDescriptor(ColumnDescriptor... columnDescriptors) {
        IndexDescriptor indexDescriptor = mock(IndexDescriptor.class);

        when(indexDescriptor.columns()).then(answer -> List.of(columnDescriptors));

        return indexDescriptor;
    }

    private static ColumnDescriptor testColumnDescriptor(NativeType nativeType, boolean nullable) {
        ColumnDescriptor columnDescriptor = mock(ColumnDescriptor.class);

        when(columnDescriptor.type()).thenReturn(nativeType);
        when(columnDescriptor.nullable()).thenReturn(nullable);

        return columnDescriptor;
    }
}
