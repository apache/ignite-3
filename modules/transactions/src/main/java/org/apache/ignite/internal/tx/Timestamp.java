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
        int res = Long.compare(timestamp >> 16 << 16, other.timestamp >> 16 << 16);

        if (res == 0) {
            res = Long.compare(timestamp << 48 >> 48, other.timestamp << 48 >> 48);

            return res == 0 ? Long.compare(nodeId, other.nodeId) : res;
        }
        else
            return res;
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
        long localTimeCpy = localTime;

        // Truncate nanotime to 48 bits.
        localTime = Math.max(localTimeCpy, Clock.systemUTC().instant().toEpochMilli() >> 16 << 16);

        if (localTimeCpy == localTime)
            cntr++;
        else
            cntr = 0;

        return new Timestamp(localTime | cntr, localNodeId);
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
            InetAddress localHost = InetAddress.getLocalHost();

            NetworkInterface iface = NetworkInterface.getByInetAddress(localHost);

            byte[] bytes = iface.getHardwareAddress();

            ByteBuffer buffer = ByteBuffer.allocate(Byte.SIZE);
            buffer.put(bytes);

            for (int i = bytes.length; i < Byte.SIZE; i++)
                buffer.put((byte) 0);

            buffer.flip();

            return buffer.getLong();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to get local node id", e);
        }
    }
}
