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

package org.apache.ignite.internal.storage.basic;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.SearchRow;

/**
 * Adapter that converts a {@link BinaryRow} into a {@link SearchRow}.
 */
public class BinarySearchRow implements SearchRow {
    /** Key array. */
    private final ByteBuffer key;

    /**
     * The constructor.
     *
     * @param row The search row.
     */
    public BinarySearchRow(BinaryRow row) {
        key = row.keySlice();
    }

    @Override
    public byte[] keyBytes() {
        // TODO asch IGNITE-15934 can reuse thread local byte buffer
        byte[] bytes = new byte[key.limit()];

        key.rewind().get(bytes);

        return bytes;
    }

    @Override
    public ByteBuffer key() {
        return key.rewind();
    }
}
