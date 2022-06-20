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

package org.apache.ignite.internal.tx;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.lang.IgniteException;

/**
 * Utility to create a transaction ID.
 */
public class TxUtils {
    /** Epoch start for the generation purposes. */
    private static final long EPOCH = LocalDateTime.of(2021, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();

    /** A max value for a counter before rollover. */
    public static final short MAX_CNT = Short.MAX_VALUE;

    /** Local time. */
    private static long localTime;

    /** The counter. */
    private static long cntr;

    /** Local node id. */
    private static long localNodeId = getLocalNodeId();

    /**
     * Waits until a time will pass after a timestamp.
     *
     * @param lastTimestamp The timestamp.
     * @return New timestamp.
     */
    private static long waitForNextMillisecond(long lastTimestamp) {
        long timestamp;

        do {
            timestamp = Clock.systemUTC().instant().toEpochMilli() - EPOCH;
        } while (timestamp <= lastTimestamp); // Wall clock can go backward.

        return timestamp;
    }

    /**
     * A transaction id is used to order transactions and perform conflict resolution.
     * TODO https://issues.apache.org/jira/browse/IGNITE-15129
     *
     * <p>The transaction id has the following structure:
     *
     * <p>Epoch time(48 bit), Local counter (16 bit), Local node id (48 bits), Reserved (16 bits)
     */
    public static synchronized UUID newTxId() {
        long timestamp = Clock.systemUTC().instant().toEpochMilli() - EPOCH;

        long newTime = Math.max(localTime, timestamp);

        if (newTime == localTime) {
            cntr = (cntr + 1) & 0xFFFF;

            if (cntr == 0) {
                newTime = waitForNextMillisecond(newTime);
            }
        } else {
            cntr = 0;
        }

        localTime = newTime;

        // Will overflow in a late future.
        return new UUID(newTime << 16 | cntr, localNodeId);
    }

    /**
     * Generates a local node id.
     *
     * @return Local node id as a long value.
     */
    private static long getLocalNodeId() {
        try {
            InetAddress localHost = InetAddress.getLocalHost();

            NetworkInterface iface = NetworkInterface.getByInetAddress(localHost);

            byte[] bytes = null;

            if (iface != null) {
                bytes = iface.getHardwareAddress();
            }

            if (bytes == null) {
                ThreadLocalRandom.current().nextBytes(bytes = new byte[Byte.SIZE]);
            }

            ByteBuffer buffer = ByteBuffer.allocate(Byte.SIZE);
            buffer.put(bytes);

            for (int i = bytes.length; i < Byte.SIZE; i++) {
                buffer.put((byte) 0);
            }

            buffer.flip();

            return buffer.getLong();
        } catch (Exception e) {
            throw new IgniteException("Failed to get local node id", e);
        }
    }
}
