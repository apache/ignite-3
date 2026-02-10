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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.type.NativeType;

/** Tests for {@link PartialBinaryTupleMatcher}. */
public class PartialBinaryTupleMatcherTest extends BinaryTupleComparatorBaseTest {
    @Override
    protected Comparator<ByteBuffer> newComparator(List<CatalogColumnCollation> columnCollations, List<NativeType> columnTypes) {
        PartialBinaryTupleMatcher matcher = new PartialBinaryTupleMatcher(columnCollations, columnTypes);

        return (l, r) -> isFlagSet(l, PREFIX_FLAG) ? -matcher.match(r, l) : matcher.match(l, r);
    }

    @Override
    protected boolean supportsPartialComparison() {
        return true;
    }
}
