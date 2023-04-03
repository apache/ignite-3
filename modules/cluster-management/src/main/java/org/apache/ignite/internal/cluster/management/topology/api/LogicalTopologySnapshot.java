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

package org.apache.ignite.internal.cluster.management.topology.api;

import static java.util.Collections.emptySet;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * A snapshot of a logical topology as seen locally. Includes nodes participating in the logical topology and the version
 * of the topology (that gets incremented on each update to the topology).
 *
 * <p>Instances of this class are immutable.
 */
public class LogicalTopologySnapshot implements Serializable {
    private static final long serialVersionUID = 0L;

    /** Initial 'topology' for an empty cluster (before any node has joined). */
    public static final LogicalTopologySnapshot INITIAL = new LogicalTopologySnapshot(0, emptySet());

    private final long version;

    @IgniteToStringInclude
    private final Set<LogicalNode> nodes;

    public LogicalTopologySnapshot(long version, Collection<LogicalNode> nodes) {
        this.version = version;
        this.nodes = Set.copyOf(nodes);
    }

    /**
     * Returns the version of this logical topology (which is incremented on each change to the topology).
     */
    public long version() {
        return version;
    }

    /**
     * Returns the nodes that comprise the logical topology.
     */
    public Set<LogicalNode> nodes() {
        return nodes;
    }

    @Override
    public String toString() {
        return S.toString(LogicalTopologySnapshot.class, this);
    }
}
