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

package org.apache.ignite.internal.metastorage.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.server.Value;
import org.jetbrains.annotations.Nullable;

/** Implementation of the {@link Entry}. */
public final class EntryImpl implements Entry {
    private static final long serialVersionUID = 3636551347117181271L;

    private final byte[] key;

    private final byte @Nullable [] value;

    private final long revision;

    private final @Nullable HybridTimestamp timestamp;

    /** Constructor. */
    public EntryImpl(byte[] key, byte @Nullable [] value, long revision, @Nullable HybridTimestamp timestamp) {
        this.key = key;
        this.value = value;
        this.revision = revision;
        this.timestamp = timestamp;
    }

    @Override
    public byte[] key() {
        return key;
    }

    @Override
    public byte @Nullable [] value() {
        return value;
    }

    @Override
    public long revision() {
        return revision;
    }

    @Override
    public @Nullable HybridTimestamp timestamp() {
        return timestamp;
    }

    @Override
    public boolean tombstone() {
        return value == null && timestamp != null;
    }

    /** Creates an instance of tombstone entry. */
    public static Entry tombstone(byte[] key, long revision, HybridTimestamp timestamp) {
        return new EntryImpl(key, null, revision, timestamp);
    }

    @Override
    public boolean empty() {
        return timestamp == null;
    }

    /** Creates an instance of empty entry for a given key. */
    public static Entry empty(byte[] key) {
        return new EntryImpl(key, null, 0, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EntryImpl entry = (EntryImpl) o;

        if (revision != entry.revision) {
            return false;
        }

        if (!Objects.equals(timestamp, entry.timestamp)) {
            return false;
        }

        if (!Arrays.equals(key, entry.key)) {
            return false;
        }

        return Arrays.equals(value, entry.value);
    }

    @Override
    public int hashCode() {
        int res = Arrays.hashCode(key);

        res = 31 * res + Arrays.hashCode(value);

        res = 31 * res + (int) (revision ^ (revision >>> 32));

        res = 31 * res + Objects.hashCode(timestamp);

        return res;
    }

    @Override
    public String toString() {
        return "EntryImpl{"
                + "key=" + new String(key, UTF_8)
                + ", value=" + Arrays.toString(value)
                + ", revision=" + revision
                + ", timestamp=" + timestamp
                + '}';
    }

    /** Converts to {@link EntryImpl}. */
    public static Entry toEntry(byte[] key, long revision, Value value) {
        if (value.tombstone()) {
            return tombstone(key, revision, value.operationTimestamp());
        }

        return new EntryImpl(key, value.bytes(), revision, value.operationTimestamp());
    }
}
