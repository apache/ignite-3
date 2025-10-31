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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.compute.JobsCommon.getIgniteImplFromOldVersionCompute;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SnapshotRequest;

@SuppressWarnings("TestOnlyProblems")
public class TruncateRaftLogCommand implements ComputeJob<String, Void> {
    @Override
    public CompletableFuture<Void> executeAsync(JobExecutionContext context, String groupId) {
        try {
            IgniteImpl igniteImpl = getIgniteImplFromOldVersionCompute(context.ignite());

            SnapshotRequest request = new RaftMessagesFactory().snapshotRequest()
                    .groupId(groupId)
                    .peerId(igniteImpl.name())
                    .build();

            MessagingService messagingService = igniteImpl.raftManager().messagingService();

            // Version 3.0.0 doesn't have "forced" snapshot, so we have to request it twice.
            return messagingService.invoke(igniteImpl.name(), request, 10000L)
                    .thenCompose(ignored -> messagingService.invoke(igniteImpl.name(), request, 10000L))
                    .thenAccept((ignored) -> {});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}