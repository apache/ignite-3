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

package org.apache.ignite.internal.metastorage.impl;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.IndexWithTerm;

/**
 * Entry point for tasks related to maintenance of the Metastorage Raft group.
 */
public interface MetastorageGroupMaintenance {
    /**
     * Returns a future that will be completed with information about index and term of the Metastorage Raft group.
     *
     * <p>This method is special in the following regard: it can be called before the component gets started. The returned
     * future will be completed after the component start.
     */
    CompletableFuture<IndexWithTerm> raftNodeIndex();

    /**
     * Makes this node a leader of the Metastorage group (with the voting set containing of just this node).
     *
     * @param termBeforeChange Term that Metastorage on the target node has before we make it become the leader.
     * @param targetVotingSet Voting set members which we want to achieve (becoming a leader is just the first step in doing so).
     * @return Future which completes when new leader becomes available.
     */
    CompletableFuture<Void> becomeLonelyLeader(long termBeforeChange, Set<String> targetVotingSet);
}
