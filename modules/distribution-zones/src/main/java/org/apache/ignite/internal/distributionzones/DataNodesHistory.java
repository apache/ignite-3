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

package org.apache.ignite.internal.distributionzones;

import static java.util.Collections.emptySet;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * Data nodes history. Is actually a map of timestamps to sets of nodes with their attributes.
 */
public class DataNodesHistory {
    @IgniteToStringInclude
    private final NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history;

    public DataNodesHistory() {
        this(new TreeMap<>());
    }

    private DataNodesHistory(NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history) {
        this.history = history;
    }

    /**
     * Copies existing history and adds a new history entry.
     *
     * @param timestamp Timestamp.
     * @param nodes Nodes.
     * @return New data nodes history.
     */
    public DataNodesHistory addHistoryEntry(HybridTimestamp timestamp, Set<NodeWithAttributes> nodes) {
        DataNodesHistory dataNodesHistory = new DataNodesHistory(new TreeMap<>(this.history));
        dataNodesHistory.history.put(timestamp, nodes);
        return dataNodesHistory;
    }

    /**
     * Copies existing history and compacts it to the given timestamp inclusively. Leaves at least one entry.
     *
     * @param toTimestamp Minimal timestamp to leave in the history.
     * @return New data nodes history.
     */
    public DataNodesHistory compactIfNeeded(HybridTimestamp toTimestamp) {
        DataNodesHistory compacted = new DataNodesHistory(new TreeMap<>(this.history.tailMap(toTimestamp, true)));

        if (compacted.isEmpty()) {
            Map.Entry<HybridTimestamp, Set<NodeWithAttributes>> lastEntry = this.history.lastEntry();

            if (lastEntry != null) {
                compacted = compacted.addHistoryEntry(lastEntry.getKey(), lastEntry.getValue());
            }
        }

        return compacted;
    }

    /**
     * Checks that the exact timestamp is present in history.
     *
     * @param timestamp Timestamp.
     * @return {@code true} if the exact timestamp is present in history.
     */
    public boolean entryIsPresentAtExactTimestamp(HybridTimestamp timestamp) {
        return history.containsKey(timestamp);
    }

    /**
     * Returns the size of the history.
     *
     * @return Size of the history.
     */
    public int size() {
        return history.size();
    }

    /**
     * Checks that the history is empty.
     *
     * @return {@code true} if history is empty.
     */
    public boolean isEmpty() {
        return history.isEmpty();
    }

    /**
     * Returns data nodes for timestamp, or empty set.
     *
     * @param timestamp Timestamp.
     * @return Data nodes for timestamp.
     */
    public DataNodesHistoryEntry dataNodesForTimestamp(HybridTimestamp timestamp) {
        Map.Entry<HybridTimestamp, Set<NodeWithAttributes>> entry = history.floorEntry(timestamp);

        if (entry == null) {
            return new DataNodesHistoryEntry(HybridTimestamp.MIN_VALUE, emptySet());
        }

        return new DataNodesHistoryEntry(entry.getKey(), entry.getValue());
    }

    @Override
    public String toString() {
        return S.toString(DataNodesHistory.class, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataNodesHistory history1 = (DataNodesHistory) o;
        return Objects.equals(history, history1.history);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(history);
    }

    /**
     * Data nodes history serializer.
     */
    public static class DataNodesHistorySerializer extends VersionedSerializer<DataNodesHistory> {
        private static final DataNodesHistorySerializer INSTANCE = new DataNodesHistorySerializer();

        @Override
        protected void writeExternalData(DataNodesHistory object, IgniteDataOutput out) throws IOException {
            // Using new TreeSet() because the order of elements is important: the serialized representation is compared during
            // MetaStorageManager#invoke().
            out.writeMap(
                    object.history,
                    (k, out0) -> out0.writeLong(k.longValue()),
                    (v, out0) -> out0.writeCollection(new TreeSet<>(v), NodeWithAttributesSerializer.INSTANCE::writeExternal)
            );
        }

        @Override
        protected DataNodesHistory readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
            NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history = in.readMap(
                    unused -> new TreeMap<>(),
                    in0 -> HybridTimestamp.hybridTimestamp(in0.readLong()),
                    in0 -> in0.readCollection(
                            unused -> new TreeSet<>(),
                            NodeWithAttributesSerializer.INSTANCE::readExternal
                    )
            );

            return new DataNodesHistory(history);
        }

        public static byte[] serialize(DataNodesHistory dataNodesHistory) {
            return VersionedSerialization.toBytes(dataNodesHistory, INSTANCE);
        }

        public static DataNodesHistory deserialize(byte[] bytes) {
            return VersionedSerialization.fromBytes(bytes, INSTANCE);
        }
    }
}
