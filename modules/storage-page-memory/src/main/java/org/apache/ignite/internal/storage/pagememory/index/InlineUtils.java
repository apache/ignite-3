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

import static org.apache.ignite.internal.util.Constants.KiB;

import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;

/**
 * Будет перенесено из мастера.
 */
public class InlineUtils {
    /** Maximum inline size for a {@link BinaryTuple}, in bytes. */
    public static final int MAX_BINARY_TUPLE_INLINE_SIZE = 2 * KiB;

    /**
     * Будет перенесено из мастера.
     */
    public static int binaryTupleInlineSize(int pageSize, int itemHeaderSize, HashIndexDescriptor indexDescriptor) {
        return MAX_BINARY_TUPLE_INLINE_SIZE;
    }
}
