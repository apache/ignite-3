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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.ignite.internal.table.distributed.disaster.messages.LocalPartitionStateMessage;

/** Container for LocalPartitionStateMessage to node name map. */
public class LocalPartitionStateMessageByNode {
    private final Map<String, LocalPartitionStateMessage> map;

    public LocalPartitionStateMessageByNode(LocalPartitionStateMessageByNode other) {
        this.map = new HashMap<>(other.map);
    }

    public LocalPartitionStateMessageByNode(Map<String, LocalPartitionStateMessage> map) {
        this.map = new HashMap<>(map);
    }

    /** Returns collection of local partition states. */
    public Collection<LocalPartitionStateMessage> values() {
        return map.values();
    }

    /** Returns set of map entries. */
    public Set<Entry<String, LocalPartitionStateMessage>> entrySet() {
        return map.entrySet();
    }

    /** Puts node to state mapping. */
    public void put(String nodeName, LocalPartitionStateMessage state) {
        map.put(nodeName, state);
    }
}
