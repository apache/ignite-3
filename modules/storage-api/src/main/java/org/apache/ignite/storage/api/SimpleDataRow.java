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

package org.apache.ignite.storage.api;

import java.nio.ByteBuffer;

public class SimpleDataRow implements DataRow {
    private final ByteBuffer key;

    private final ByteBuffer value;

    public SimpleDataRow(ByteBuffer key, ByteBuffer value) {
        assert key.isReadOnly();
        assert value.isReadOnly();

        this.key = key;
        this.value = value;
    }

    @Override public ByteBuffer key() {
        return key;
    }

    @Override public ByteBuffer value() {
        return value;
    }

    @Override public int hash() {
        return key.hashCode();
    }
}
