/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.internal.index;

import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.util.Cursor;

/**
 * An object describing an abstract index.
 *
 * <p>Provides access to the indexed data as well as all information about index itself.
 */
public interface Index<DescriptorT extends IndexDescriptor> {
    /** Returns identifier of the index. */
    UUID id();

    /** Returns name of the index. */
    String name();

    UUID tableId();

    DescriptorT descriptor();

    /** Returns cursor for the values corresponding to the given key. */
    Cursor<BinaryTuple> scan(BinaryTuple key, BitSet columns);
}
