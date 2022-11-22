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

package org.apache.ignite.internal.table.distributed;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link PartitionMover} class.
 */
class PartitionMoverTest {
    /**
     * Tests that {@link RaftGroupServiceImpl#changePeersAsync} was retried after some exceptions.
     */
    @Test
    public void testChangePeersAsyncRetryLogic() {
        RaftGroupService raftService = mock(RaftGroupService.class);

        when(raftService.changePeersAsync(any(), any(), anyLong()))
                .thenReturn(failedFuture(new RuntimeException()))
                .thenReturn(failedFuture(new IOException()))
                .thenReturn(completedFuture(null));

        var partitionMover = new PartitionMover(new IgniteSpinBusyLock(), () -> raftService);

        List<Peer> peers = List.of(new Peer("peer1"), new Peer("peer2"));
        List<Peer> learners = List.of(new Peer("learner1"), new Peer("learner2"), new Peer("learner3"));
        long term = 123;

        assertThat(partitionMover.movePartition(peers, learners, term), willCompleteSuccessfully());

        verify(raftService, times(3)).changePeersAsync(eq(peers), eq(learners), eq(term));
    }

    @Test
    public void testComponentStop() {
        var lock = new IgniteSpinBusyLock();

        var partitionMover = new PartitionMover(lock, () -> mock(RaftGroupService.class));

        lock.block();

        assertThrowsWithCause(() -> partitionMover.movePartition(List.of(), List.of(), 1), NodeStoppingException.class);
    }

    @Test
    public void testComponentAsyncStop() {
        var lock = new IgniteSpinBusyLock();

        RaftGroupService raftService = mock(RaftGroupService.class);

        when(raftService.changePeersAsync(any(), any(), anyLong()))
                .then(invocation -> CompletableFuture.runAsync(lock::block));

        var partitionMover = new PartitionMover(lock, () -> raftService);

        assertThat(partitionMover.movePartition(List.of(), List.of(), 1), willThrowWithCauseOrSuppressed(NodeStoppingException.class));
    }
}
