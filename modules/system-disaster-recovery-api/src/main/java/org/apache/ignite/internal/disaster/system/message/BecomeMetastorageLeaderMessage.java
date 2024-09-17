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

package org.apache.ignite.internal.disaster.system.message;

import java.util.Set;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * A command to make a node become a Metastorage Raft group leader.
 */
@Transferable(SystemDisasterRecoveryMessageGroup.BECOME_METASTORAGE_LEADER)
public interface BecomeMetastorageLeaderMessage extends NetworkMessage {
    /** Term that Metastorage on the target node has before we make it become the leader. */
    long termBeforeChange();

    /** Voting set members (aka peers) which we want to achieve (becoming a leader is just the first step in doing so). */
    Set<String> targetVotingSet();
}
