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

package org.apache.ignite.internal.storage;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;

public class DummyTableRowImpl implements TableRow {
    //TODO: Replace with Tuple layout constants.
    private final int SCHEMA_VERSION_OFFSET = 0;
    private final int FLAGS_OFFSET = SCHEMA_VERSION_OFFSET + 2;
    private final int KEY_HASH_OFFSET = FLAGS_OFFSET + 2;
    private final int KEY_OFFSET = KEY_HASH_OFFSET + 4;


    // TODO: Wrap tuple.
    private final byte[] bytes;

    public DummyTableRowImpl(byte[] rowBytes) {
        bytes = rowBytes;
    }

    @Override public ByteBuffer getKeyBytes() {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);

        final int keyLen = buf.getInt(KEY_HASH_OFFSET);

        return buf.limit(keyLen).position(KEY_OFFSET).slice();
    }
}
