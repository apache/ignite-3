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
import static org.apache.ignite.internal.lang.Pair.pair;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.Pair;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * Data nodes history.
 */
public class DataNodesHistory {
    private final NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history;

    public DataNodesHistory() {
        this(new TreeMap<>());
    }

    public DataNodesHistory(NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history) {
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
        DataNodesHistory dataNodesHistory = new DataNodesHistory(this.history);
        dataNodesHistory.history.put(timestamp, nodes);
        return dataNodesHistory;
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
     * Returns data nodes for timestamp, or empty set.
     *
     * @param timestamp Timestamp.
     * @return Data nodes for timestamp, or empty set.
     */
    public Pair<HybridTimestamp, Set<NodeWithAttributes>> dataNodesForTimestamp(HybridTimestamp timestamp) {
        Map.Entry<HybridTimestamp, Set<NodeWithAttributes>> entry = history.floorEntry(timestamp);

        if (entry == null) {
            return pair(HybridTimestamp.MIN_VALUE, emptySet());
        }

        return pair(entry.getKey(), entry.getValue());
    }

    @Override
    public String toString() {
        return "DataNodesHistory [history=" + history + "].";
    }

    /**
     * Data nodes history serializer.
     */
    public static class DataNodesHistorySerializer extends VersionedSerializer<DataNodesHistory> {
        private static final DataNodesHistorySerializer INSTANCE = new DataNodesHistorySerializer();

        @Override
        protected void writeExternalData(DataNodesHistory object, IgniteDataOutput out) throws IOException {
            out.writeMap(
                    object.history,
                    (k, out0) -> out0.writeLong(k.longValue()),
                    (v, out0) -> out0.writeCollection(v, NodeWithAttributesSerializer.INSTANCE::writeExternal)
            );
        }

        @Override
        protected DataNodesHistory readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
            NavigableMap<HybridTimestamp, Set<NodeWithAttributes>> history = in.readMap(
                    TreeMap::new,
                    in0 -> HybridTimestamp.hybridTimestamp(in0.readLong()),
                    in0 -> in0.readCollection(
                            HashSet::new,
                            NodeWithAttributesSerializer.INSTANCE::readExternal
                    )
            );

            return new DataNodesHistory(history);
        }

        static byte[] serialize(DataNodesHistory dataNodesHistory) {
            return VersionedSerialization.toBytes(dataNodesHistory, INSTANCE);
        }

        static DataNodesHistory deserialize(byte[] bytes) {
            return VersionedSerialization.fromBytes(bytes, INSTANCE);
        }
    }
}
