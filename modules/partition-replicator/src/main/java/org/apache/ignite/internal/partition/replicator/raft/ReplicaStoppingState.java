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

package org.apache.ignite.internal.partition.replicator.raft;

/**
 * Holds state about whether a replica is being stopped.
 */
public class ReplicaStoppingState {
    private volatile boolean replicaStopping;

    /** Returns whether the replica is being stopped. */
    public boolean isReplicaStopping() {
        return replicaStopping;
    }

    /** Marks the replica as being stopped. */
    void markReplicaStopping() {
        this.replicaStopping = true;
    }
}
