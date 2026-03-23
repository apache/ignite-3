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

package org.apache.ignite.internal.storage.pagememory.index.sorted.comparator;

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.PREFIX_FLAG;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.isFlagSet;
import static org.apache.ignite.internal.storage.pagememory.index.sorted.comparator.JitComparatorGenerator.createComparator;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.schema.BinaryTupleComparatorBaseTest;
import org.apache.ignite.internal.schema.UnsafeByteBufferAccessor;
import org.apache.ignite.internal.type.NativeType;

/**
 * Tests for {@link JitComparator} that use {@link JitComparatorGenerator} to generate a comparator.
 */
public class JitComparatorTest extends BinaryTupleComparatorBaseTest {
    @Override
    protected Comparator<ByteBuffer> newComparator(List<CatalogColumnCollation> columnCollations, List<NativeType> columnTypes) {
        JitComparator comparator = createComparator(JitComparatorOptions.builder()
                .columnCollations(columnCollations)
                .columnTypes(columnTypes)
                .nullableFlags(Collections.nCopies(columnTypes.size(), true))
                .supportPrefixes(true)
                .supportPartialComparison(true)
                .build()
        );

        return (left, right) -> {
            UnsafeByteBufferAccessor leftAccessor = new UnsafeByteBufferAccessor(left);
            UnsafeByteBufferAccessor rightAccessor = new UnsafeByteBufferAccessor(right);

            // It is not allowed to pass prefix as a second tuple in JitComparator, for this reason we reverse the order of arguments and
            // the result if the second tuple is a prefix.
            return isFlagSet(right, PREFIX_FLAG)
                    ? -comparator.compare(rightAccessor, right.capacity(), leftAccessor, left.capacity())
                    : comparator.compare(leftAccessor, left.capacity(), rightAccessor, right.capacity());
        };
    }

    @Override
    protected boolean supportsPartialComparison() {
        return true;
    }
}
