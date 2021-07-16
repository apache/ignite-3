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

package org.apache.ignite.internal.metastorage.server.persistence;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.metastorage.server.Value;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;

/**
 * Utility class for {@link RocksDBKeyValueStorage}.
 */
class RocksStorageByteUtils {
    /**
     * Adds a revision to a key.
     *
     * @param revision Revision.
     * @param key Key.
     * @return Key with a revision.
     */
    static byte[] keyToRocksKey(long revision, byte[] key) {
        byte[] buffer = new byte[Long.BYTES + key.length];

        ByteUtils.toBytes(revision, buffer, 0, Long.BYTES);

        System.arraycopy(key, 0, buffer, Long.BYTES, key.length);

        return buffer;
    }

    /**
     * Gets a key from a key with revision.
     *
     * @param rocksKey Key with a revision.
     * @return Key without a revision.
     */
    static byte[] rocksKeyToBytes(byte[] rocksKey) {
        return Arrays.copyOfRange(rocksKey, Long.BYTES, rocksKey.length);
    }

    /**
     * Builds a value from a byte array.
     *
     * @param valueBytes Value byte array.
     * @return Value.
     */
    static Value bytesToValue(byte[] valueBytes) {
        ByteBuffer buf = ByteBuffer.wrap(valueBytes);

        // Read an update counter (8-byte long) from the entry.
        long updateCounter = buf.getLong();

        // Read a has-value flag (1 byte) from the entry.
        boolean hasValue = buf.get() != 0;

        byte[] val;
        if (hasValue) {
            // Read the value.
            val = new byte[buf.remaining()];
            buf.get(val);
        }
        else
            // There is no value, mark it as a tombstone.
            val = TOMBSTONE;

        return new Value(val, updateCounter);
    }

    /**
     * Adds an update counter and a tombstone flag to a value.
     * @param value Value byte array.
     * @param updateCounter Update counter.
     * @return Value with an update counter and a tombstone.
     */
    static byte[] valueToBytes(byte[] value, long updateCounter) {
        byte[] bytes = new byte[Long.BYTES + Byte.BYTES + value.length];

        ByteUtils.toBytes(updateCounter, bytes, 0, Long.BYTES);

        bytes[Long.BYTES] = (byte) (value == TOMBSTONE ? 0 : 1);

        System.arraycopy(value, 0, bytes, Long.BYTES + Byte.BYTES, value.length);

        return bytes;
    }

    /**
     * Iterates over the given iterator passing key-values pairs to the given consumer and checks the iterator's status
     * afterwards.
     *
     * @param iterator Iterator.
     * @param consumer Consumer of key-value pairs.
     * @throws RocksDBException If failed.
     */
    static void forEach(RocksIterator iterator, RocksBiConsumer consumer) throws RocksDBException {
        for (; iterator.isValid(); iterator.next())
            consumer.accept(iterator.key(), iterator.value());

        checkIterator(iterator);
    }

    /**
     * Checks the status of the iterator and throw an exception if it is not correct.
     *
     * @param it RocksDB iterator.
     * @throws IgniteInternalException if the iterator has an incorrect status.
     */
    static void checkIterator(RocksIterator it) {
        try {
            it.status();
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
    }

    /**
     * BiConsumer that can throw {@link RocksDBException}.
     */
    interface RocksBiConsumer {
        /**
         * Accepts the key and the value of the entry.
         *
         * @param key Key.
         * @param value Value.
         * @throws RocksDBException If failed to process the key-value pair.
         */
        void accept(byte[] key, byte[] value) throws RocksDBException;
    }
}
