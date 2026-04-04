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

package org.apache.ignite.internal.raft.client;

/**
 * Strategy for selecting the target peer for a request.
 */
enum TargetPeerStrategy {
    /**
     * Most operations - targets the leader, waits for leader if not available.
     * On all peers exhausted, waits for leader notification.
     */
    LEADER,

    /**
     * Operations that don't need a leader (e.g., refreshLeader).
     * Any peer works, fails immediately when all peers exhausted.
     */
    RANDOM,

    /**
     * Operations that must go to a specific peer (e.g., snapshot).
     * Never retries to other peers, only retries to the same peer on transient errors.
     */
    SPECIFIC
}
