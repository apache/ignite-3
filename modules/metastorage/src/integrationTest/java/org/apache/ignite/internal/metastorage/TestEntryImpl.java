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

package org.apache.ignite.internal.metastorage;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/** Implementation for equality checking only. */
public class TestEntryImpl implements Entry {
    private static final long serialVersionUID = 4106515935101768387L;

    /** Special value that means to ignore the {@link #timestamp()} check when checking for equality. */
    public static final HybridTimestamp ANY_TIMESTAMP = new HybridTimestamp(1L, 1);

    private final byte[] key;

    private final byte @Nullable [] value;

    private final long revision;

    private final long updateCounter;

    private final @Nullable HybridTimestamp timestamp;

    /** Constructor. */
    public TestEntryImpl(byte[] key, byte @Nullable [] value, long revision, long updateCounter, @Nullable HybridTimestamp timestamp) {
        this.key = key;
        this.value = value;
        this.revision = revision;
        this.updateCounter = updateCounter;
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
    public long updateCounter() {
        return updateCounter;
    }

    @Override
    public boolean empty() {
        throw new UnsupportedOperationException("Implementation for equality checking only");
    }

    @Override
    public boolean tombstone() {
        throw new UnsupportedOperationException("Implementation for equality checking only");
    }

    @Override
    public @Nullable HybridTimestamp timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Entry)) {
            return false;
        }

        Entry entry = (Entry) o;

        if (revision != entry.revision()) {
            return false;
        }

        if (updateCounter != entry.updateCounter()) {
            return false;
        }

        if (timestamp != ANY_TIMESTAMP && entry.timestamp() != ANY_TIMESTAMP) {
            if (!Objects.equals(timestamp, entry.timestamp())) {
                return false;
            }
        }

        if (!Arrays.equals(key, entry.key())) {
            return false;
        }

        return Arrays.equals(value, entry.value());
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Implementation for equality checking only");
    }

    @Override
    public String toString() {
        return "TestEntryImpl{"
                + "key=" + new String(key, UTF_8)
                + ", value=" + Arrays.toString(value)
                + ", revision=" + revision
                + ", updateCounter=" + updateCounter
                + ", timestamp=" + timestamp
                + '}';
    }
}
