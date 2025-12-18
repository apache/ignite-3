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

package org.apache.ignite.internal.table.distributed.disaster;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;

/** Container for LocalTablePartitionState to node name map. */
// TODO remove
public class LocalTablePartitionStateByNode {
    @IgniteToStringInclude
    private final Map<String, LocalTablePartitionState> map;

    LocalTablePartitionStateByNode(Map<String, LocalTablePartitionState> map) {
        this.map = Map.copyOf(map);
    }

    /** Returns collection of local partition states. */
    public Collection<LocalTablePartitionState> values() {
        return map.values();
    }

    /** Returns set of map entries. */
    public Set<Entry<String, LocalTablePartitionState>> entrySet() {
        return map.entrySet();
    }

    /** Returns set of node names. */
    public Set<String> keySet() {
        return map.keySet();
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
