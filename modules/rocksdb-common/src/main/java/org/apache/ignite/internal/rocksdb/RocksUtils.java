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

package org.apache.ignite.internal.rocksdb;

import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * RocksDB utility functions.
 */
public class RocksUtils {
    /**
     * Iterates over the given iterator passing key-value pairs to the given consumer and checks the iterator's status afterwards.
     *
     * @param iterator Iterator.
     * @param consumer Consumer of key-value pairs.
     * @throws RocksDBException If failed.
     */
    public static void forEach(RocksIterator iterator, RocksBiConsumer consumer) throws RocksDBException {
        for (; iterator.isValid(); iterator.next()) {
            consumer.accept(iterator.key(), iterator.value());
        }

        checkIterator(iterator);
    }

    /**
     * Iterates over the given iterator testing key-value pairs with the given predicate and checks the iterator's status afterwards.
     *
     * @param iterator Iterator.
     * @param consumer Consumer of key-value pairs.
     * @return {@code true} if a matching key-value pair has been found, {@code false} otherwise.
     * @throws RocksDBException If failed.
     */
    public static boolean find(RocksIterator iterator, RocksBiPredicate consumer) throws RocksDBException {
        for (; iterator.isValid(); iterator.next()) {
            boolean result = consumer.test(iterator.key(), iterator.value());

            if (result) {
                return true;
            }
        }

        checkIterator(iterator);

        return false;
    }

    /**
     * Checks the status of the iterator and throws an exception if it is not correct.
     *
     * @param it RocksDB iterator.
     * @throws IgniteInternalException if the iterator has an incorrect status.
     */
    public static void checkIterator(RocksIterator it) {
        try {
            it.status();
        } catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
    }

    /**
     * Creates a byte array that can be used as an upper bound when iterating over RocksDB content.
     *
     * <p>This method tries to increment the least significant byte (in BE order) that is not equal to 0xFF (bytes are treated as
     * unsigned values).
     *
     * @param rangeStart Start of a range of keys (prefix) in RocksDB.
     * @return End of a range of keys in RocksDB or {@code null} if all bytes of the prefix are equal to 0xFF.
     */
    public static byte @Nullable [] rangeEnd(byte[] rangeStart) {
        byte[] rangeEnd = rangeStart.clone();

        int i = rangeStart.length - 1;

        // Cycle through all bytes that are equal to 0xFF
        while (i >= 0 && rangeStart[i] == -1) {
            rangeEnd[i] = 0;

            i--;
        }

        if (i == -1) {
            // All bytes are equal to 0xFF, no upper bound should be used
            return null;
        } else {
            rangeEnd[i] += 1;

            return rangeEnd;
        }
    }
}
