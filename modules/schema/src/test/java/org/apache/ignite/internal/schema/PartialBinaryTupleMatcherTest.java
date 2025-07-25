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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.PREFIX_FLAG;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.isFlagSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/** Tests for {@link PartialBinaryTupleMatcher}. */
public class PartialBinaryTupleMatcherTest extends BinaryTupleComparatorBaseTest {
    @Override
    protected Comparator<ByteBuffer> newComparator(List<CatalogColumnCollation> columnCollations, List<NativeType> columnTypes) {
        PartialBinaryTupleMatcher matcher = new PartialBinaryTupleMatcher(columnCollations, columnTypes);

        return (l, r) -> isFlagSet(l, PREFIX_FLAG) ? -matcher.match(r, l) : matcher.match(l, r);
    }

    @Test
    public void partialComparatorAsciiTest() {
        PartialBinaryTupleMatcher partialBinaryTupleMatcher = new PartialBinaryTupleMatcher(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.STRING)
        );

        ByteBuffer tupleReference = new BinaryTupleBuilder(1)
                .appendString("qwertyuiop".repeat(10))
                .build();

        ByteBuffer tupleWithEmptyStringReference = new BinaryTupleBuilder(1)
                .appendString("")
                .build();

        ByteBuffer tuple1 = new BinaryTupleBuilder(1)
                .appendString("qwertyuiop")
                .build();

        ByteBuffer tuple2 = new BinaryTupleBuilder(1)
                .appendString("rxfsuzvjpq".repeat(2))
                .build();

        ByteBuffer tuple3 = new BinaryTupleBuilder(1)
                .appendString("qwertyuiop".repeat(15))
                .build();

        assertTrue(partialBinaryTupleMatcher.match(tuple1, tupleReference) < 0);
        assertTrue(partialBinaryTupleMatcher.match(tuple2, tupleReference) > 0);
        assertTrue(partialBinaryTupleMatcher.match(tuple3, tupleReference) > 0);
        assertEquals(0, partialBinaryTupleMatcher.match(tuple1.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(partialBinaryTupleMatcher.match(tuple2.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
        assertTrue(partialBinaryTupleMatcher.match(
                tuple2.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN),
                tupleWithEmptyStringReference
        ) > 0);
        assertEquals(0, partialBinaryTupleMatcher.match(tuple3.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(partialBinaryTupleMatcher.match(tuple3.limit(120).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
        assertTrue(partialBinaryTupleMatcher.match(
                tuple3.limit(120).slice().order(ByteOrder.LITTLE_ENDIAN),
                tupleWithEmptyStringReference
        ) > 0);
    }

    @Test
    public void partialComparatorUnicodeTest() {
        PartialBinaryTupleMatcher partialBinaryTupleMatcher = new PartialBinaryTupleMatcher(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.STRING)
        );

        ByteBuffer tupleReference = new BinaryTupleBuilder(1)
                .appendString("йцукенгшщз".repeat(10))
                .build();

        ByteBuffer tuple1 = new BinaryTupleBuilder(1)
                .appendString("йцукенгшщз")
                .build();

        ByteBuffer tuple2 = new BinaryTupleBuilder(1)
                .appendString("кчфлёодщъи".repeat(2))
                .build();

        ByteBuffer tuple3 = new BinaryTupleBuilder(1)
                .appendString("йцукенгшщз".repeat(15))
                .build();

        assertTrue(partialBinaryTupleMatcher.match(tuple1, tupleReference) < 0);
        assertTrue(partialBinaryTupleMatcher.match(tuple2, tupleReference) > 0);
        assertTrue(partialBinaryTupleMatcher.match(tuple3, tupleReference) > 0);
        assertEquals(0, partialBinaryTupleMatcher.match(tuple1.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(partialBinaryTupleMatcher.match(tuple2.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
        assertEquals(0, partialBinaryTupleMatcher.match(tuple3.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(partialBinaryTupleMatcher.match(tuple3.limit(220).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
    }

    @Test
    public void partialComparatorBytesTest() {
        PartialBinaryTupleMatcher partialBinaryTupleMatcher = new PartialBinaryTupleMatcher(
                List.of(CatalogColumnCollation.ASC_NULLS_LAST),
                List.of(NativeTypes.BYTES)
        );

        byte[] bytes = new byte[150];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i % 10);
        }

        ByteBuffer tupleReference = new BinaryTupleBuilder(1)
                .appendBytes(Arrays.copyOfRange(bytes, 0, 100))
                .build();

        ByteBuffer tupleWithEmptyArrayReference = new BinaryTupleBuilder(1)
                .appendBytes(new byte[0])
                .build();

        ByteBuffer tuple1 = new BinaryTupleBuilder(1)
                .appendBytes(Arrays.copyOfRange(bytes, 0, 10))
                .build();

        ByteBuffer tuple2 = new BinaryTupleBuilder(1)
                .appendBytes(Arrays.copyOfRange(bytes, 1, 21))
                .build();

        ByteBuffer tuple3 = new BinaryTupleBuilder(1)
                .appendBytes(bytes)
                .build();

        assertTrue(partialBinaryTupleMatcher.match(tuple1, tupleReference) < 0);
        assertTrue(partialBinaryTupleMatcher.match(tuple2, tupleReference) > 0);
        assertTrue(partialBinaryTupleMatcher.match(tuple3, tupleReference) > 0);
        assertEquals(0, partialBinaryTupleMatcher.match(tuple1.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(partialBinaryTupleMatcher.match(tuple2.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
        assertTrue(partialBinaryTupleMatcher.match(
                tuple2.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN),
                tupleWithEmptyArrayReference
        ) > 0);
        assertEquals(0, partialBinaryTupleMatcher.match(tuple3.limit(8).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference));
        assertTrue(partialBinaryTupleMatcher.match(tuple3.limit(120).slice().order(ByteOrder.LITTLE_ENDIAN), tupleReference) > 0);
        assertTrue(partialBinaryTupleMatcher.match(
                tuple3.limit(120).slice().order(ByteOrder.LITTLE_ENDIAN),
                tupleWithEmptyArrayReference
        ) > 0);
    }
}
