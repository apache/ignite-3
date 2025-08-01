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

package org.apache.ignite.internal.metastorage.server;

import static org.apache.ignite.internal.metastorage.MetaStorageManager.LATEST_REVISION;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker.TrackingToken;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

/** For {@link ReadOperationForCompactionTracker} testing. */
public class ReadOperationForCompactionTrackerTest {
    private final ReadOperationForCompactionTracker tracker = new ReadOperationForCompactionTracker();

    @Test
    void testCollectEmpty() {
        assertTrue(tracker.collect(0).isDone());
        assertTrue(tracker.collect(1).isDone());
        assertTrue(tracker.collect(2).isDone());
    }

    @Test
    void testTrackAndUntrack() {
        assertThrows(CompactedException.class, () -> tracker.track(0, () -> 0L, () -> 0L));

        TrackingToken token1 = tracker.track(1, () -> 0L, () -> 0L);

        token1.close();
    }

    @Test
    void testTrackUntrackAndCollect() {
        TrackingToken token1 = tracker.track(1, () -> 0L, () -> 0L);
        TrackingToken token2 = tracker.track(2, () -> 0L, () -> 0L);

        assertTrue(tracker.collect(0).isDone());

        CompletableFuture<Void> collectFuture1 = tracker.collect(1);
        CompletableFuture<Void> collectFuture2 = tracker.collect(2);

        assertFalse(collectFuture1.isDone());
        assertFalse(collectFuture2.isDone());

        token1.close();
        assertTrue(collectFuture1.isDone());
        assertFalse(collectFuture2.isDone());

        token2.close();
        assertTrue(collectFuture1.isDone());
        assertTrue(collectFuture2.isDone());
    }

    /**
     * Tests that concurrent update and compaction doesn't break reading the "latest" revision of data.
     */
    @Test
    void testTrackLatestRevision() {
        for (int i = 0; i < 10_000; i++) {
            AtomicLong latestRevision = new AtomicLong(2);
            AtomicLong compactedRevision = new AtomicLong(1);

            AtomicReference<TrackingToken> token = new AtomicReference<>();

            IgniteTestUtils.runRace(
                    () -> token.set(tracker.track(LATEST_REVISION, latestRevision::get, compactedRevision::get)),
                    () -> {
                        latestRevision.set(3);
                        compactedRevision.set(2);

                        // This one is tricky. If we identify that there are no tracked futures after the moment of setting compaction
                        // revision to "2", it should mean that concurrent tracking should end up creating the future for revision "3".
                        // Here we validate that there are no data races for such a scenario, by re-checking the list of tracked futures.
                        // If something has been added there by a concurrent thread, it should soon be completed before a retry.
                        if (tracker.collect(2).isDone()) {
                            assertThat(tracker.collect(2), willSucceedFast());
                        }
                    }
            );

            assertFalse(tracker.collect(3).isDone());

            token.get().close();

            assertTrue(tracker.collect(3).isDone());
        }
    }

    /**
     * Tests that concurrent compaction either leads to {@link CompactedException}, or just works. There should be no leaks or races.
     */
    @Test
    void testConcurrentCompact() {
        for (int i = 0; i < 10_000; i++) {
            AtomicLong latestRevision = new AtomicLong(2);
            AtomicLong compactedRevision = new AtomicLong(1);

            AtomicReference<TrackingToken> token = new AtomicReference<>();

            IgniteTestUtils.runRace(
                    () -> {
                        try {
                            token.set(tracker.track(2, latestRevision::get, compactedRevision::get));
                        } catch (CompactedException ignore) {
                            // No-op.
                        }
                    },
                    () -> {
                        latestRevision.set(3);
                        compactedRevision.set(2);

                        // This one is tricky. If we identify that there are no tracked futures after the moment of setting compaction
                        // revision to "2", it should mean that concurrent tracking should end up end up throwing CompactedException.
                        // Here we validate that there are no data races for such a scenario, by re-checking the list of tracked futures.
                        // If something has been added there by a concurrent thread, it should soon be completed before failing.
                        if (tracker.collect(2).isDone()) {
                            assertThat(tracker.collect(2), willSucceedFast());
                        }
                    }
            );

            if (token.get() == null) {
                // CompactedException case.
                assertTrue(tracker.collect(2).isDone());
            } else {
                // Successful tracking case.
                assertFalse(tracker.collect(2).isDone());

                token.get().close();

                assertTrue(tracker.collect(2).isDone());
            }
        }
    }
}
