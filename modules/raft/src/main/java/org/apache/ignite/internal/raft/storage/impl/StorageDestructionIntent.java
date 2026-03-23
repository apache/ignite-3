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

package org.apache.ignite.internal.raft.storage.impl;

import org.apache.ignite.internal.tostring.S;

/** Intent to destroy raft node's group storages. */
public class StorageDestructionIntent {
    private final boolean isVolatile;
    private final String raftNodeId;
    private final String groupName;

    /** Constructor. */
    public StorageDestructionIntent(String raftNodeId, String groupName, boolean isVolatile) {
        this.raftNodeId = raftNodeId;
        this.groupName = groupName;
        this.isVolatile = isVolatile;
    }

    public String nodeId() {
        return raftNodeId;
    }

    public boolean isVolatile() {
        return isVolatile;
    }

    @Override
    public String toString() {
        return S.toString(StorageDestructionIntent.class, this);
    }

    public String groupName() {
        return groupName;
    }
}
