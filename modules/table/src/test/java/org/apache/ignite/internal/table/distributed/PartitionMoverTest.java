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
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.distributionzones.rebalance.PartitionMover;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link PartitionMover} class.
 */
class PartitionMoverTest extends BaseIgniteAbstractTest {
    private static final long TERM = 123;

    private static final PeersAndLearners PEERS_AND_LEARNERS = PeersAndLearners.fromConsistentIds(
            Set.of("peer1", "peer2"),
            Set.of("learner1", "learner2", "learner3")
    );

    /**
     * Tests that {@link RaftGroupServiceImpl#changePeersAsync} was retried after some exceptions.
     */
    @Test
    public void testChangePeersAsyncRetryLogic() {
        RaftGroupService raftService = mock(RaftGroupService.class);

        when(raftService.changePeersAsync(any(), anyLong()))
                .thenReturn(failedFuture(new RuntimeException()))
                .thenReturn(failedFuture(new IOException()))
                .thenReturn(nullCompletedFuture());

        var partitionMover = new PartitionMover(new IgniteSpinBusyLock(), () -> completedFuture(raftService));

        assertThat(partitionMover.movePartition(PEERS_AND_LEARNERS, TERM), willCompleteSuccessfully());

        verify(raftService, times(3)).changePeersAsync(eq(PEERS_AND_LEARNERS), eq(TERM));
    }

    @Test
    public void testComponentStop() {
        var lock = new IgniteSpinBusyLock();

        RaftGroupService raftService = mock(RaftGroupService.class);

        var partitionMover = new PartitionMover(lock, () -> completedFuture(raftService));

        lock.block();

        assertThrowsWithCause(() -> partitionMover.movePartition(PEERS_AND_LEARNERS, TERM), NodeStoppingException.class);
    }

    @Test
    public void testComponentAsyncStop() {
        var lock = new IgniteSpinBusyLock();

        RaftGroupService raftService = mock(RaftGroupService.class);

        when(raftService.changePeersAsync(any(), anyLong()))
                .then(invocation -> CompletableFuture.runAsync(lock::block));

        var partitionMover = new PartitionMover(lock, () -> completedFuture(raftService));

        assertThat(partitionMover.movePartition(PEERS_AND_LEARNERS, TERM), willThrowWithCauseOrSuppressed(NodeStoppingException.class));
    }
}
