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

package org.apache.ignite.internal.raft.rebalance;

import org.apache.ignite.internal.raft.service.RaftGroupService;

/**
 * Wrapper for RaftGroupService and term.
 */
public class RaftWithTerm {

    private final RaftGroupService raftClient;
    private final long term;

    /**
     * Creates a new instance of RaftWithTerm.
     *
     * @param raftClient The RaftGroupService.
     * @param term The term.
     */
    public RaftWithTerm(RaftGroupService raftClient, long term) {
        this.raftClient = raftClient;
        this.term = term;
    }

    /**
     * Returns the RaftGroupService.
     *
     * @return The RaftGroupService.
     */
    public RaftGroupService raftClient() {
        return raftClient;
    }

    /**
     * Returns the term.
     *
     * @return The term.
     */
    public long term() {
        return term;
    }

}
