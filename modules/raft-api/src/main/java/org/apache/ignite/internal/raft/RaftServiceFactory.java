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

package org.apache.ignite.internal.raft;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/**
 * Factory that can be used to create customized Raft service.
 */
public interface RaftServiceFactory<T extends RaftGroupService> {
    /**
     * Creates Raft group service.
     *
     * @param groupId Group id.
     * @param peersAndLearners Peers configuration.
     * @param raftConfiguration Raft configuration.
     * @param raftClientExecutor Client executor.
     * @param commandsMarshaller Marshaller that should be used to serialize commands.
     * @return Future that contains client when completes.
     */
    CompletableFuture<T> startRaftGroupService(
            ReplicationGroupId groupId,
            PeersAndLearners peersAndLearners,
            RaftConfiguration raftConfiguration,
            ScheduledExecutorService raftClientExecutor,
            Marshaller commandsMarshaller
    );
}
