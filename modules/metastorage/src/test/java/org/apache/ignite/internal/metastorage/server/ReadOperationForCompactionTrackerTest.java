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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
        UUID readOperation0 = UUID.randomUUID();
        UUID readOperation1 = UUID.randomUUID();

        long operationRevision0 = 1;
        long operationRevision1 = 2;

        assertDoesNotThrow(() -> tracker.track(readOperation0, operationRevision0));
        assertDoesNotThrow(() -> tracker.track(readOperation0, operationRevision1));
        assertDoesNotThrow(() -> tracker.track(readOperation1, operationRevision0));
        assertDoesNotThrow(() -> tracker.track(readOperation1, operationRevision1));

        assertDoesNotThrow(() -> tracker.untrack(readOperation0, operationRevision0));
        assertDoesNotThrow(() -> tracker.untrack(readOperation0, operationRevision1));
        assertDoesNotThrow(() -> tracker.untrack(readOperation1, operationRevision0));
        assertDoesNotThrow(() -> tracker.untrack(readOperation1, operationRevision1));

        // Let's check that after untrack we can do track again for the previous arguments.
        assertDoesNotThrow(() -> tracker.track(readOperation0, operationRevision0));
        assertDoesNotThrow(() -> tracker.untrack(readOperation0, operationRevision0));
    }

    @Test
    void testTrackUntrackAndCollect() {
        UUID readOperation0 = UUID.randomUUID();
        UUID readOperation1 = UUID.randomUUID();

        long operationRevision0 = 1;
        long operationRevision1 = 2;

        tracker.track(readOperation0, operationRevision0);
        tracker.track(readOperation1, operationRevision0);

        assertTrue(tracker.collect(0).isDone());

        CompletableFuture<Void> collectFuture1 = tracker.collect(1);
        assertFalse(collectFuture1.isDone());

        tracker.untrack(readOperation0, operationRevision0);
        assertFalse(collectFuture1.isDone());

        tracker.untrack(readOperation1, operationRevision0);
        assertTrue(collectFuture1.isDone());

        tracker.track(readOperation0, operationRevision1);
        tracker.track(readOperation1, operationRevision1);

        assertTrue(tracker.collect(0).isDone());
        assertTrue(tracker.collect(1).isDone());

        CompletableFuture<Void> collectFuture2 = tracker.collect(2);
        assertFalse(collectFuture2.isDone());

        tracker.untrack(readOperation1, operationRevision1);
        assertFalse(collectFuture2.isDone());

        tracker.untrack(readOperation0, operationRevision1);
        assertTrue(collectFuture2.isDone());
    }
}
