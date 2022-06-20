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
import java.util.UUID;
import org.jetbrains.annotations.NotNull;

/**
 * A timestamp implementation. Timestamps are used to order transactions.
 * Temporary solution until the implementation of the HLC is done.
 * TODO https://issues.apache.org/jira/browse/IGNITE-15129
 *
 * <p>The timestamp has the following structure:
 *
 * <p>Epoch time(48 bit), Local counter (16 bit), Local node id (48 bits), Reserved (16 bits)
 */
public class Timestamp implements Comparable<Timestamp>, Serializable {
    /** Serial version. */
    private static final long serialVersionUID = 1L;

    /** The offset and counter part of a timestamp. */
    private final long timestamp;

    /** The node id part of a timestamp. */
    private final long nodeId;

    /**
     * The constructor.
     *
     * @param timestamp The timestamp.
     * @param nodeId    Node id.
     */
    public Timestamp(long timestamp, long nodeId) {
        this.timestamp = timestamp;
        this.nodeId = nodeId;
    }

    /**
     * The constructor.
     *
     * @param txId Transaction id.
     */
    public Timestamp(UUID txId) {
        this.timestamp = txId.getMostSignificantBits();
        this.nodeId = txId.getLeastSignificantBits();
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(@NotNull Timestamp other) {
        return (this.timestamp < other.timestamp ? -1 : (this.timestamp > other.timestamp ? 1 :
                Long.compare(this.nodeId, other.nodeId)));
    }

    /**
     * Returns {@code true} if this is before or equal to another timestamp.
     *
     * @param that another timestamp
     * @return {@code true} if this is before or equal to another timestamp
     */
    public boolean beforeOrEquals(Timestamp that) {
        return this.compareTo(that) <= 0;
    }

    /**
     * Returns a timestamp.
     *
     * @return The timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Return a node id.
     *
     * @return Local node id.
     */
    public long getNodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Timestamp)) {
            return false;
        }

        return compareTo((Timestamp) o) == 0;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        long hilo = timestamp ^ nodeId;
        return ((int) (hilo >> 32)) ^ (int) hilo;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return new UUID(timestamp, nodeId).toString();
    }
}
