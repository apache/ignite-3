/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.index.impl;

import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowDeserializer;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;

/**
 * {@link IndexRowDeserializer} implementation that uses {@link BinaryTuple} infrastructure for deserialization purposes.
 */
class BinaryTupleRowDeserializer implements IndexRowDeserializer {
    private final SortedIndexDescriptor descriptor;

    BinaryTupleRowDeserializer(SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public Object[] deserializeColumns(IndexRow indexRow) {
        BinaryTuple tuple = indexRow.indexColumns();

        assert tuple.count() == descriptor.indexColumns().size();

        var result = new Object[descriptor.indexColumns().size()];

        for (int i = 0; i < result.length; i++) {
            NativeTypeSpec typeSpec = descriptor.indexColumns().get(i).type().spec();

            result[i] = typeSpec.objectValue(tuple, i);
        }

        return result;
    }
}
