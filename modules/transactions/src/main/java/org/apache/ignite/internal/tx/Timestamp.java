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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Enumeration;
import java.util.UUID;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.NotNull;

/**
 * The timestamp. Has the following structure:
 * <p>
 * Local Time(48 bit) Local counter (16 bit) Local node id (48 bits) Not used (16 bits)
 */
public class Timestamp implements Comparable<Timestamp>, Serializable {
    /** */
    private static final long serialVersionUID = 1L;

    /** Epoch start for the generation purposes. */
    private static final long EPOCH = LocalDateTime.of(2021, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();

    /** */
    public static final short MAX_CNT = Short.MAX_VALUE;

    /** */
    private static long localTime;

    /** */
    private static long cntr;

    /** */
    private static long localNodeId = getLocalNodeId();

    /** */
    private final long timestamp;

    /** */
    private final long nodeId;

    /**
     * @param timestamp The timestamp.
     * @param nodeId Node id.
     */
    public Timestamp(long timestamp, long nodeId) {
        this.timestamp = timestamp;
        this.nodeId = nodeId;
    }

    /**
     * @param other Other version.
     * @return Comparison result.
     */
    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull Timestamp other) {
        return (this.timestamp < other.timestamp ? -1 :
            (this.timestamp > other.timestamp ? 1 :
                (this.nodeId < other.nodeId ? -1 :
                    (this.nodeId > other.nodeId ? 1 :
                        0))));
    }

    /**
     * @return The timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return Local node id.
     */
    public long getNodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (!(o instanceof Timestamp)) return false;

        return compareTo((Timestamp) o) == 0;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        long hilo = timestamp ^ nodeId;
        return ((int)(hilo >> 32)) ^ (int) hilo;
    }

    /**
     * TODO https://issues.apache.org/jira/browse/IGNITE-15129
     * @return Next timestamp (monotonically increasing).
     */
    public static synchronized Timestamp nextVersion() {
        long timestamp = Clock.systemUTC().instant().toEpochMilli() - EPOCH;

        long newTime = Math.max(localTime, timestamp);

        if (newTime == localTime) {
            cntr = (cntr + 1) & 0xFFFF;

            if (cntr == 0)
                newTime = waitForNextMillisecond(newTime);
        }
        else
            cntr = 0;

        localTime = newTime;

        // Will overflow in a late future.
        return new Timestamp(newTime << 16 | cntr, localNodeId);
    }

    /**
     * @param lastTimestamp Wait until this timestamp will pass.
     * @return New timestamp.
     */
    private static long waitForNextMillisecond(long lastTimestamp) {
        long timestamp;

        do {
            timestamp = Clock.systemUTC().instant().toEpochMilli() - EPOCH;
        }
        while(timestamp <= lastTimestamp); // Wall clock can go backward.

        return timestamp;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return new UUID(timestamp, nodeId).toString();
    }

    /**
     * @return Local node id as long value.
     */
    private static long getLocalNodeId() {
        try {
            byte[] hardwareAddress = null;

            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

            while (networkInterfaces.hasMoreElements() && hardwareAddress == null) {
                Enumeration<InetAddress> inetAddrs = networkInterfaces.nextElement().getInetAddresses();

                while (inetAddrs.hasMoreElements() && hardwareAddress == null) {
                    InetAddress addr = inetAddrs.nextElement();

                    hardwareAddress = NetworkInterface.getByInetAddress(addr).getHardwareAddress();
                }
            }

            ByteBuffer buffer = ByteBuffer.allocate(Byte.SIZE);
            buffer.put(hardwareAddress);

            for (int i = hardwareAddress.length; i < Byte.SIZE; i++)
                buffer.put((byte) 0);

            buffer.flip();

            return buffer.getLong();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to get local node id", e);
        }
    }
}
