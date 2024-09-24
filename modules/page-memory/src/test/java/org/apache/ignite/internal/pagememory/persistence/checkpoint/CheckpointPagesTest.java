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
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.fullPageId;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.junit.jupiter.api.Test;

/** For {@link CheckpointPages} testing. */
public class CheckpointPagesTest {
    @Test
    void testContains() {
        CheckpointPages checkpointPages = createCheckpointPages(fullPageId(0, 0), fullPageId(1, 0));

        assertTrue(checkpointPages.contains(new FullPageId(0, 0)));
        assertTrue(checkpointPages.contains(new FullPageId(1, 0)));

        assertFalse(checkpointPages.contains(new FullPageId(2, 0)));
        assertFalse(checkpointPages.contains(new FullPageId(3, 0)));
    }

    @Test
    void testSize() {
        CheckpointPages checkpointPages = createCheckpointPages(fullPageId(0, 0), fullPageId(1, 0));

        assertEquals(2, checkpointPages.size());
    }

    @Test
    void testRemove() {
        CheckpointPages checkpointPages = createCheckpointPages(fullPageId(0, 0), fullPageId(1, 0), fullPageId(2, 0));

        assertTrue(checkpointPages.remove(fullPageId(0, 0)));
        assertFalse(checkpointPages.contains(new FullPageId(0, 0)));
        assertEquals(2, checkpointPages.size());

        assertFalse(checkpointPages.remove(fullPageId(0, 0)));
        assertFalse(checkpointPages.contains(new FullPageId(0, 0)));
        assertEquals(2, checkpointPages.size());

        assertTrue(checkpointPages.remove(fullPageId(1, 0)));
        assertFalse(checkpointPages.contains(new FullPageId(0, 0)));
        assertEquals(1, checkpointPages.size());
    }

    @Test
    void testAdd() {
        CheckpointPages checkpointPages = createCheckpointPages();

        assertTrue(checkpointPages.add(fullPageId(0, 0)));
        assertTrue(checkpointPages.contains(fullPageId(0, 0)));
        assertEquals(1, checkpointPages.size());

        assertFalse(checkpointPages.add(fullPageId(0, 0)));
        assertTrue(checkpointPages.contains(fullPageId(0, 0)));
        assertEquals(1, checkpointPages.size());

        assertTrue(checkpointPages.add(fullPageId(0, 1)));
        assertTrue(checkpointPages.contains(fullPageId(0, 1)));
        assertEquals(2, checkpointPages.size());
    }

    @Test
    void testAllowToReplaceSuccessfully() throws Exception {
        var checkpointProgress = new CheckpointProgressImpl(10);

        CheckpointPages checkpointPages = createCheckpointPages(checkpointProgress, fullPageId(0, 0), fullPageId(1, 0));

        // Let's make sure that the check will not complete until the dirty page sorting phase completes.
        checkpointProgress.transitTo(LOCK_RELEASED);

        CompletableFuture<Boolean> allowToReplaceFuture = runAsync(() -> checkpointPages.allowToReplace(fullPageId(0, 0)));
        assertThat(allowToReplaceFuture, willTimeoutFast());

        checkpointProgress.transitTo(PAGES_SORTED);
        assertThat(allowToReplaceFuture, willBe(true));
        assertTrue(checkpointPages.contains(fullPageId(0, 0)));

        assertTrue(checkpointPages.allowToReplace(fullPageId(1, 0)));
        assertTrue(checkpointPages.contains(fullPageId(1, 0)));
    }

    @Test
    void testAllowToReplaceErrorOnWaitPageSortingPhase() {
        var checkpointProgress = new CheckpointProgressImpl(10);

        CheckpointPages checkpointPages = createCheckpointPages(checkpointProgress);

        checkpointProgress.fail(new Exception("from test"));

        assertThrows(
                Exception.class,
                () -> checkpointPages.allowToReplace(fullPageId(0, 0)),
                "from test"
        );
    }

    @Test
    void testAllowToReplaceUnsuccessfully() throws Exception {
        var checkpointProgress = new CheckpointProgressImpl(10);
        checkpointProgress.transitTo(PAGES_SORTED);

        CheckpointPages checkpointPages = createCheckpointPages(checkpointProgress, fullPageId(0, 0), fullPageId(1, 0));

        // Case of a page that never existed.
        assertFalse(checkpointPages.allowToReplace(fullPageId(1, 1)));

        // Case of a page that no longer exists.
        checkpointPages.remove(fullPageId(1, 0));
        assertFalse(checkpointPages.allowToReplace(fullPageId(1, 0)));

        // Case after intention to start the fsync phase.
        checkpointProgress.stopBlockingFsyncOnPageReplacement();
        assertFalse(checkpointPages.allowToReplace(fullPageId(0, 0)));
    }

    @Test
    void testFinishReplaceSuccessfully() throws Exception {
        var checkpointProgress = new CheckpointProgressImpl(10);
        checkpointProgress.transitTo(PAGES_SORTED);

        CheckpointPages checkpointPages = createCheckpointPages(checkpointProgress, fullPageId(0, 0));

        checkpointPages.allowToReplace(fullPageId(0, 0));

        CompletableFuture<Void> stopBlockingFsyncOnPageReplacementFuture = checkpointProgress.stopBlockingFsyncOnPageReplacement();
        assertFalse(stopBlockingFsyncOnPageReplacementFuture.isDone());

        checkpointPages.finishReplace(fullPageId(0, 0), null);
        assertThat(stopBlockingFsyncOnPageReplacementFuture, willCompleteSuccessfully());
    }

    @Test
    void testFinishReplaceWithError() throws Exception {
        var checkpointProgress = new CheckpointProgressImpl(10);
        checkpointProgress.transitTo(PAGES_SORTED);

        CheckpointPages checkpointPages = createCheckpointPages(checkpointProgress, fullPageId(0, 0));

        checkpointPages.allowToReplace(fullPageId(0, 0));

        CompletableFuture<Void> stopBlockingFsyncOnPageReplacementFuture = checkpointProgress.stopBlockingFsyncOnPageReplacement();
        assertFalse(stopBlockingFsyncOnPageReplacementFuture.isDone());

        checkpointPages.finishReplace(fullPageId(0, 0), new Exception("from test"));
        assertThat(stopBlockingFsyncOnPageReplacementFuture, willThrow(Exception.class, "from test"));
    }

    private static CheckpointPages createCheckpointPages(FullPageId... pageIds) {
        var checkpointProgress = new CheckpointProgressImpl(10);

        checkpointProgress.transitTo(PAGES_SORTED);

        return createCheckpointPages(checkpointProgress, pageIds);
    }

    private static CheckpointPages createCheckpointPages(CheckpointProgressImpl checkpointProgress, FullPageId... pageIds) {
        var set = new HashSet<FullPageId>(pageIds.length);

        Collections.addAll(set, pageIds);

        return new CheckpointPages(set, checkpointProgress);
    }
}
