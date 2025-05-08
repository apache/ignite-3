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

import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Class representing a single entry in the history of data nodes.
 */
public class DataNodesHistoryEntry {
    private final HybridTimestamp timestamp;

    @IgniteToStringInclude
    private final Set<NodeWithAttributes> dataNodes;

    /**
     * Constructor.
     *
     * @param timestamp Timestamp of the history entry.
     * @param dataNodes Set of data nodes at the time of the history entry.
     */
    public DataNodesHistoryEntry(HybridTimestamp timestamp, Set<NodeWithAttributes> dataNodes) {
        this.timestamp = timestamp;
        this.dataNodes = dataNodes;
    }

    /**
     * Timestamp of the history entry.
     *
     * @return Timestamp of the history entry.
     */
    public HybridTimestamp timestamp() {
        return timestamp;
    }

    /**
     * Set of data nodes at the time of the history entry.
     *
     * @return Set of data nodes at the time of the history entry.
     */
    public Set<NodeWithAttributes> dataNodes() {
        return dataNodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataNodesHistoryEntry that = (DataNodesHistoryEntry) o;
        return Objects.equals(timestamp, that.timestamp) && Objects.equals(dataNodes, that.dataNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, dataNodes);
    }

    @Override
    public String toString() {
        return S.toString(DataNodesHistoryEntry.class, this);
    }
}
