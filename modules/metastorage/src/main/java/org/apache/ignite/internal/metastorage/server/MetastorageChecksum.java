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

package org.apache.ignite.internal.metastorage.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.Checksum;
import org.apache.ignite.internal.metastorage.command.RemoveByPrefixCommand;
import org.apache.ignite.raft.jraft.util.CRC64;

/**
 * Checksum calculation logic for the Metastorage.
 *
 * <p>An object of this class has the following state:
 *
 * <ul>
 *     <li>Last revision checksum (0 if the Metastorage has no revisions yet)</li>
 *     <li>Current round checksum state (accumulated during the round)</li>
 * </ul>
 *
 * <p>A round per revision is performed. For simple (non-invoke) commands, the whole round is performed in one call (see
 * {@link #wholePut(byte[], byte[])} and so on). For compound commands (invokes), a round is started
 * ({@link #prepareForInvoke(boolean, int, byte[])}) and finished ({@link #roundValue()}) explicitly, and the checksum is updated in it
 * per each operation that actually gets executed ({@link #appendPutAsPart(byte[], byte[])} and so on).
 *
 * <p>During a round (including its finish), only the current round checksum state is updated. To update the last revision checksum, call
 * {@link #commitRound(long)}.
 */
public class MetastorageChecksum {
    private long lastChecksum;

    private final Checksum checksum = new CRC64();

    /** Constructor. */
    public MetastorageChecksum(long lastChecksum) {
        this.lastChecksum = lastChecksum;
    }

    /**
     * Calculates a checksum for a PUT command.
     *
     * @param key Key.
     * @param value Value.
     */
    public long wholePut(byte[] key, byte[] value) {
        return checksumWholeOperation(Op.PUT, () -> updateForPut(key, value));
    }

    private void updateForPut(byte[] key, byte[] value) {
        updateWithBytes(key);
        updateWithBytes(value);
    }

    private long checksumWholeOperation(Op operation, Updater updater) {
        prepareRound(operation);

        updater.update();

        return roundValue();
    }

    private void prepareRound(Op operation) {
        checksum.reset();
        updateWithLong(lastChecksum);
        checksum.update(operation.code);
    }

    private void updateWithBytes(byte[] bytes) {
        updateWithInt(bytes.length);
        checksum.update(bytes);
    }

    private void updateWithInt(int value) {
        for (int i = 0; i < Integer.BYTES; i++) {
            checksum.update((byte) (value >> (Integer.SIZE - 8)));
            value <<= 8;
        }
    }

    private void updateWithLong(long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            checksum.update((byte) (value >> (Long.SIZE - 8)));
            value <<= 8;
        }
    }

    /**
     * Calculates a checksum for a PUT_ALL command.
     *
     * @param keys Keys.
     * @param values Values.
     */
    public long wholePutAll(List<byte[]> keys, List<byte[]> values) {
        return checksumWholeOperation(Op.PUT_ALL, () -> updateForPutAll(keys, values));
    }

    private void updateForPutAll(List<byte[]> keys, List<byte[]> values) {
        updateWithInt(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            updateForPut(keys.get(i), values.get(i));
        }
    }

    /**
     * Calculates a checksum for a REMOVE command.
     *
     * @param key Key.
     */
    public long wholeRemove(byte[] key) {
        return checksumWholeOperation(Op.REMOVE, () -> updateForRemove(key));
    }

    private void updateForRemove(byte[] key) {
        updateWithBytes(key);
    }

    /**
     * Calculates a checksum for a REMOVE command.
     *
     * @param keys Keys.
     */
    public long wholeRemoveAll(List<byte[]> keys) {
        // Sort keys to get stable checksums independent from the order of keys (as the effect to the storage is the same even if
        // key order is different).
        List<byte[]> sortedKeys = new ArrayList<>(keys);
        sortedKeys.sort(Arrays::compare);

        return checksumWholeOperation(Op.REMOVE_ALL, () -> updateForRemoveAll(sortedKeys));
    }

    private void updateForRemoveAll(List<byte[]> keys) {
        updateWithInt(keys.size());
        for (byte[] key : keys) {
            updateForRemove(key);
        }
    }

    /**
     * Calculates a checksum for a {@link RemoveByPrefixCommand}.
     *
     * @param prefix prefix.
     */
    public long wholeRemoveByPrefix(byte[] prefix) {
        return checksumWholeOperation(Op.REMOVE_BY_PREFIX, () -> updateWithBytes(prefix));
    }

    /**
     * Initiates a round for an invocation command.
     *
     * @param multiInvoke Whether this is a multi-invoke.
     * @param opCount Number of operations that get executed.
     * @param updateResult Update result.
     */
    public void prepareForInvoke(boolean multiInvoke, int opCount, byte[] updateResult) {
        prepareRound(multiInvoke ? Op.MULTI_INVOKE : Op.SINGLE_INVOKE);
        updateWithBytes(updateResult);
        updateWithInt(opCount);
    }

    /**
     * Appends a PUT command as a part of an invocation command.
     *
     * @param key Key.
     * @param value Value.
     */
    public void appendPutAsPart(byte[] key, byte[] value) {
        appendPartOfCompound(Op.PUT, () -> updateForPut(key, value));
    }

    /**
     * Appends a REMOVE command as a part of an invocation command.
     *
     * @param key Key.
     */
    public void appendRemoveAsPart(byte[] key) {
        appendPartOfCompound(Op.REMOVE, () -> updateForRemove(key));
    }

    private void appendPartOfCompound(Op operation, Updater updater) {
        checksum.update(operation.code);
        updater.update();
    }

    /**
     * Saves the new checksum as the last checksum.
     *
     * @param newChecksum New checksum.
     */
    public void commitRound(long newChecksum) {
        lastChecksum = newChecksum;
    }

    /**
     * Returns current round checksum value.
     */
    public long roundValue() {
        return checksum.getValue();
    }

    private enum Op {
        PUT(1),
        PUT_ALL(2),
        REMOVE(3),
        REMOVE_ALL(4),
        SINGLE_INVOKE(5),
        MULTI_INVOKE(6),
        REMOVE_BY_PREFIX(7);

        private final int code;

        Op(int code) {
            this.code = code;
        }
    }

    @FunctionalInterface
    private interface Updater {
        void update();
    }
}
