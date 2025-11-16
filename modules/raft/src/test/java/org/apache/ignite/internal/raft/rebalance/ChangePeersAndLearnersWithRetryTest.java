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
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link ChangePeersAndLearnersWithRetry} class.
 */
@ExtendWith(ExecutorServiceExtension.class)
class ChangePeersAndLearnersWithRetryTest extends BaseIgniteAbstractTest {
    private static final long TERM = 123;

    private static final PeersAndLearners PEERS_AND_LEARNERS = PeersAndLearners.fromConsistentIds(
            Set.of("peer1", "peer2"),
            Set.of("learner1", "learner2", "learner3")
    );

    @InjectExecutorService
    private ScheduledExecutorService rebalanceScheduler;

    /**
     * Tests that {@link RaftGroupServiceImpl#changePeersAndLearnersAsync} was retried after some exceptions.
     */
    @Test
    public void testChangePeersAndLearnersAsyncRetryLogic() {
        RaftGroupService raftService = mock(RaftGroupService.class);

        when(raftService.changePeersAndLearnersAsync(any(), anyLong(), anyLong()))
                .thenReturn(failedFuture(new RuntimeException()))
                .thenReturn(failedFuture(new IOException()))
                .thenReturn(nullCompletedFuture());

        var changePeersAndLearnersWithRetry =
                new ChangePeersAndLearnersWithRetry(new IgniteSpinBusyLock(), rebalanceScheduler, () -> completedFuture(raftService));

        assertThat(changePeersAndLearnersWithRetry.execute(
                PEERS_AND_LEARNERS,
                Configuration.NO_SEQUENCE_TOKEN,
                raft -> completedFuture(new RaftWithTerm(raft, TERM))
        ), willCompleteSuccessfully());

        verify(raftService, times(3))
                .changePeersAndLearnersAsync(eq(PEERS_AND_LEARNERS), eq(TERM), eq(Configuration.NO_SEQUENCE_TOKEN));
    }

    @Test
    public void testComponentStop() {
        var lock = new IgniteSpinBusyLock();

        RaftGroupService raftService = mock(RaftGroupService.class);

        var changePeersAndLearnersWithRetry =
                new ChangePeersAndLearnersWithRetry(lock, rebalanceScheduler, () -> completedFuture(raftService));

        lock.block();

        assertThrowsWithCause(() ->
                        changePeersAndLearnersWithRetry.execute(
                                PEERS_AND_LEARNERS,
                                Configuration.NO_SEQUENCE_TOKEN,
                                raft -> completedFuture(new RaftWithTerm(raft, TERM))
                        ),
                NodeStoppingException.class);
    }

    @Test
    public void testComponentAsyncStop() {
        var lock = new IgniteSpinBusyLock();

        RaftGroupService raftService = mock(RaftGroupService.class);

        when(raftService.changePeersAndLearnersAsync(any(), anyLong(), anyLong()))
                .then(invocation -> CompletableFuture.runAsync(lock::block));

        var changePeersAndLearnersWithRetry =
                new ChangePeersAndLearnersWithRetry(lock, rebalanceScheduler, () -> completedFuture(raftService));

        assertThat(changePeersAndLearnersWithRetry.execute(
                        PEERS_AND_LEARNERS,
                        Configuration.NO_SEQUENCE_TOKEN,
                        raft -> completedFuture(new RaftWithTerm(raft, TERM))
                ),
                willThrowWithCauseOrSuppressed(NodeStoppingException.class));
    }
}
