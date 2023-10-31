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

package org.apache.ignite.internal.table.distributed.replicator;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ChainingRaftGroupServiceTest extends BaseIgniteAbstractTest {
    @Mock
    private RaftGroupService realRaftClient;

    @InjectMocks
    private ChainingRaftGroupService chainingRaftClient;

    @Mock
    private Command command1;
    @Mock
    private Command command2;

    @Test
    void subsequentRunStartsAfterPreviousRunFinishes() {
        CompletableFuture<Object> runFuture1 = new CompletableFuture<>();
        CompletableFuture<Object> runFuture2 = new CompletableFuture<>();

        when(realRaftClient.run(any())).thenReturn(runFuture1, runFuture2);

        chainingRaftClient.run(command1);
        chainingRaftClient.run(command2);

        verify(realRaftClient).run(command1);
        verify(realRaftClient, never()).run(command2);

        runFuture1.complete(null);

        verify(realRaftClient).run(command2);
    }

    @Test
    void failingRunDoesNotBlockSubsequentRunsExecution() {
        CompletableFuture<Object> runFuture1 = new CompletableFuture<>();
        CompletableFuture<Object> runFuture2 = new CompletableFuture<>();

        when(realRaftClient.run(any())).thenReturn(runFuture1, runFuture2);

        chainingRaftClient.run(command1);
        chainingRaftClient.run(command2);

        runFuture1.completeExceptionally(new Exception("Oops"));

        verify(realRaftClient).run(command2);
    }
}
