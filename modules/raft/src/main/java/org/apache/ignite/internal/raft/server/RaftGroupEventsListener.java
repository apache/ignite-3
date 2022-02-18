/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft.server;

import java.util.List;
import org.apache.ignite.raft.jraft.entity.PeerId;

/**
 * Listener for group membership and other events.
 */
public interface RaftGroupEventsListener {
    /**
     * Invoked, when new leader is elected (if it is the first leader of group ever - will be invoked too).
     */
    public void onLeaderElected();

    /**
     * Invoked on the leader, when new peers' configuration applied to raft group.
     *
     * @param peers list of peers, which was applied by raft group membership configuration.
     */
    public void onNewPeersConfigurationApplied(List<PeerId> peers);
}
