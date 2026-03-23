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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SORTED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.dirtyFullPageId;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.junit.jupiter.api.Test;

/** For {@link CheckpointPages} testing. */
public class CheckpointPagesTest {
    @Test
    void testContains() {
        CheckpointPages checkpointPages = createCheckpointPages(new DirtyFullPageId(0, 0, 1), new DirtyFullPageId(1, 0, 1));

        assertTrue(checkpointPages.contains(new DirtyFullPageId(0, 0, 1)));
        assertTrue(checkpointPages.contains(new DirtyFullPageId(1, 0, 1)));

        assertFalse(checkpointPages.contains(new DirtyFullPageId(2, 0, 1)));
        assertFalse(checkpointPages.contains(new DirtyFullPageId(3, 0, 1)));
    }

    @Test
    void testSize() {
        CheckpointPages checkpointPages = createCheckpointPages(dirtyFullPageId(0, 0), dirtyFullPageId(1, 0));

        assertEquals(2, checkpointPages.size());
    }

    @Test
    void testRemoveOnCheckpoint() {
        CheckpointPages checkpointPages = createCheckpointPages(dirtyFullPageId(0, 0), dirtyFullPageId(1, 0), dirtyFullPageId(2, 0));

        assertTrue(checkpointPages.removeOnCheckpoint(dirtyFullPageId(0, 0)));
        assertFalse(checkpointPages.contains(new DirtyFullPageId(0, 0, 1)));
        assertEquals(2, checkpointPages.size());

        assertFalse(checkpointPages.removeOnCheckpoint(dirtyFullPageId(0, 0)));
        assertFalse(checkpointPages.contains(new DirtyFullPageId(0, 0, 1)));
        assertEquals(2, checkpointPages.size());

        assertTrue(checkpointPages.removeOnCheckpoint(dirtyFullPageId(1, 0)));
        assertFalse(checkpointPages.contains(new DirtyFullPageId(0, 0, 1)));
        assertEquals(1, checkpointPages.size());
    }

    @Test
    void testRemoveOnPageReplacement() throws Exception {
        var checkpointProgress = new CheckpointProgressImpl(10);

        CheckpointPages checkpointPages = createCheckpointPages(checkpointProgress, dirtyFullPageId(0, 0), dirtyFullPageId(1, 0));

        // Let's make sure that the check will not complete until the dirty page sorting phase completes.
        checkpointProgress.transitTo(LOCK_RELEASED);

        CompletableFuture<Boolean> removeOnPageReplacementFuture = runAsync(
                () -> checkpointPages.removeOnPageReplacement(dirtyFullPageId(0, 0))
        );
        assertThat(removeOnPageReplacementFuture, willTimeoutFast());

        checkpointProgress.transitTo(PAGES_SORTED);
        assertThat(removeOnPageReplacementFuture, willBe(true));
        assertFalse(checkpointPages.contains(dirtyFullPageId(0, 0)));
        assertEquals(1, checkpointPages.size());

        assertFalse(checkpointPages.removeOnPageReplacement(dirtyFullPageId(0, 0)));
        assertFalse(checkpointPages.contains(dirtyFullPageId(0, 0)));
        assertEquals(1, checkpointPages.size());

        assertTrue(checkpointPages.removeOnPageReplacement(dirtyFullPageId(1, 0)));
        assertFalse(checkpointPages.contains(dirtyFullPageId(1, 0)));
        assertEquals(0, checkpointPages.size());
    }

    @Test
    void testRemoveOnPageReplacementErrorOnWaitPageSortingPhase() {
        var checkpointProgress = new CheckpointProgressImpl(10);

        CheckpointPages checkpointPages = createCheckpointPages(checkpointProgress);

        checkpointProgress.fail(new Exception("from test"));

        assertThrows(
                Exception.class,
                () -> checkpointPages.removeOnPageReplacement(dirtyFullPageId(0, 0)),
                "from test"
        );
    }

    private static CheckpointPages createCheckpointPages(DirtyFullPageId... pageIds) {
        var checkpointProgress = new CheckpointProgressImpl(10);

        checkpointProgress.transitTo(PAGES_SORTED);

        return createCheckpointPages(checkpointProgress, pageIds);
    }

    private static CheckpointPages createCheckpointPages(CheckpointProgressImpl checkpointProgress, DirtyFullPageId... pageIds) {
        var set = new HashSet<DirtyFullPageId>(pageIds.length);

        Collections.addAll(set, pageIds);

        return new CheckpointPages(set, checkpointProgress);
    }
}
