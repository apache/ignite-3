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

import static java.lang.System.nanoTime;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SORTED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.SCHEDULED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * For {@link CheckpointProgressImpl} testing.
 */
public class CheckpointProgressImplTest {
    @Test
    void testId() {
        assertNotNull(new CheckpointProgressImpl(0).id());
    }

    @Test
    void testReason() {
        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);

        assertNull(progressImpl.reason());

        String reason = "test";

        progressImpl.reason(reason);

        assertEquals(reason, progressImpl.reason());
    }

    @Test
    void testNextCheckpointNanos() {
        long startNanos = nanoTime();

        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(10);

        long endNanos = nanoTime();

        assertThat(
                progressImpl.nextCheckpointNanos() - startNanos,
                greaterThanOrEqualTo(0L)
        );

        assertThat(
                endNanos + 10 - progressImpl.nextCheckpointNanos(),
                greaterThanOrEqualTo(0L)
        );
    }

    @Test
    void testCounters() {
        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);

        assertEquals(0, progressImpl.currentCheckpointPagesCount());

        assertEquals(0, progressImpl.writtenPagesCounter().get());
        assertEquals(0, progressImpl.syncedPagesCounter().get());
        assertEquals(0, progressImpl.evictedPagesCounter().get());
        assertEquals(0, progressImpl.syncedFilesCounter().get());

        progressImpl.writtenPagesCounter().incrementAndGet();
        progressImpl.syncedPagesCounter().incrementAndGet();
        progressImpl.evictedPagesCounter().incrementAndGet();
        progressImpl.syncedFilesCounter().incrementAndGet();

        progressImpl.initCounters(100500);

        assertEquals(100500, progressImpl.currentCheckpointPagesCount());

        assertEquals(0, progressImpl.writtenPagesCounter().get());
        assertEquals(0, progressImpl.syncedPagesCounter().get());
        assertEquals(0, progressImpl.evictedPagesCounter().get());
        assertEquals(0, progressImpl.syncedFilesCounter().get());

        progressImpl.writtenPagesCounter().incrementAndGet();
        progressImpl.syncedPagesCounter().incrementAndGet();
        progressImpl.evictedPagesCounter().incrementAndGet();
        progressImpl.syncedFilesCounter().incrementAndGet();

        progressImpl.clearCounters();

        assertEquals(0, progressImpl.currentCheckpointPagesCount());

        assertEquals(0, progressImpl.writtenPagesCounter().get());
        assertEquals(0, progressImpl.syncedPagesCounter().get());
        assertEquals(0, progressImpl.evictedPagesCounter().get());
        assertEquals(0, progressImpl.syncedFilesCounter().get());

        progressImpl.currentCheckpointPagesCount(42);

        assertEquals(42, progressImpl.currentCheckpointPagesCount());
    }

    @Test
    void testGreaterOrEqualTo() {
        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);

        assertTrue(progressImpl.greaterOrEqualTo(SCHEDULED));
        assertFalse(progressImpl.greaterOrEqualTo(LOCK_TAKEN));
        assertFalse(progressImpl.greaterOrEqualTo(PAGES_SNAPSHOT_TAKEN));
        assertFalse(progressImpl.greaterOrEqualTo(LOCK_RELEASED));
        assertFalse(progressImpl.greaterOrEqualTo(PAGES_SORTED));
        assertFalse(progressImpl.greaterOrEqualTo(FINISHED));

        progressImpl.transitTo(LOCK_TAKEN);

        assertTrue(progressImpl.greaterOrEqualTo(SCHEDULED));
        assertTrue(progressImpl.greaterOrEqualTo(LOCK_TAKEN));
        assertFalse(progressImpl.greaterOrEqualTo(PAGES_SNAPSHOT_TAKEN));
        assertFalse(progressImpl.greaterOrEqualTo(LOCK_RELEASED));
        assertFalse(progressImpl.greaterOrEqualTo(PAGES_SORTED));
        assertFalse(progressImpl.greaterOrEqualTo(FINISHED));

        progressImpl.transitTo(PAGES_SNAPSHOT_TAKEN);

        assertTrue(progressImpl.greaterOrEqualTo(SCHEDULED));
        assertTrue(progressImpl.greaterOrEqualTo(LOCK_TAKEN));
        assertTrue(progressImpl.greaterOrEqualTo(PAGES_SNAPSHOT_TAKEN));
        assertFalse(progressImpl.greaterOrEqualTo(LOCK_RELEASED));
        assertFalse(progressImpl.greaterOrEqualTo(PAGES_SORTED));
        assertFalse(progressImpl.greaterOrEqualTo(FINISHED));

        progressImpl.transitTo(LOCK_RELEASED);

        assertTrue(progressImpl.greaterOrEqualTo(SCHEDULED));
        assertTrue(progressImpl.greaterOrEqualTo(LOCK_TAKEN));
        assertTrue(progressImpl.greaterOrEqualTo(PAGES_SNAPSHOT_TAKEN));
        assertTrue(progressImpl.greaterOrEqualTo(LOCK_RELEASED));
        assertFalse(progressImpl.greaterOrEqualTo(PAGES_SORTED));
        assertFalse(progressImpl.greaterOrEqualTo(FINISHED));

        progressImpl.transitTo(PAGES_SORTED);

        assertTrue(progressImpl.greaterOrEqualTo(SCHEDULED));
        assertTrue(progressImpl.greaterOrEqualTo(LOCK_TAKEN));
        assertTrue(progressImpl.greaterOrEqualTo(PAGES_SNAPSHOT_TAKEN));
        assertTrue(progressImpl.greaterOrEqualTo(LOCK_RELEASED));
        assertTrue(progressImpl.greaterOrEqualTo(PAGES_SORTED));
        assertFalse(progressImpl.greaterOrEqualTo(FINISHED));

        progressImpl.transitTo(FINISHED);

        assertTrue(progressImpl.greaterOrEqualTo(SCHEDULED));
        assertTrue(progressImpl.greaterOrEqualTo(LOCK_TAKEN));
        assertTrue(progressImpl.greaterOrEqualTo(PAGES_SNAPSHOT_TAKEN));
        assertTrue(progressImpl.greaterOrEqualTo(LOCK_RELEASED));
        assertTrue(progressImpl.greaterOrEqualTo(PAGES_SORTED));
        assertTrue(progressImpl.greaterOrEqualTo(FINISHED));
    }

    @Test
    void testInProgress() {
        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);

        assertFalse(progressImpl.inProgress());

        progressImpl.transitTo(LOCK_TAKEN);
        assertFalse(progressImpl.inProgress());

        progressImpl.transitTo(LOCK_RELEASED);
        assertTrue(progressImpl.inProgress());

        progressImpl.transitTo(PAGES_SORTED);
        assertTrue(progressImpl.inProgress());

        progressImpl.transitTo(PAGES_SORTED);
        assertTrue(progressImpl.inProgress());

        progressImpl.transitTo(FINISHED);
        assertFalse(progressImpl.inProgress());
    }

    @Test
    void testFail() {
        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);

        CompletableFuture<?> future = progressImpl.futureFor(PAGES_SNAPSHOT_TAKEN);

        Exception failReason = new Exception("test");

        progressImpl.fail(failReason);

        assertEquals(FINISHED, progressImpl.state());

        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(100, TimeUnit.MILLISECONDS));

        assertSame(failReason, exception.getCause());
    }

    @Test
    void testFutureFor() {
        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);

        CompletableFuture<?> future0 = progressImpl.futureFor(SCHEDULED);

        assertNotNull(future0);
        assertTrue(future0.isDone());
        assertSame(future0, progressImpl.futureFor(SCHEDULED));

        CompletableFuture<?> future1 = progressImpl.futureFor(LOCK_TAKEN);

        assertNotNull(future1);
        assertFalse(future1.isDone());
        assertSame(future1, progressImpl.futureFor(LOCK_TAKEN));

        CompletableFuture<?> future2 = progressImpl.futureFor(PAGES_SNAPSHOT_TAKEN);

        assertNotNull(future2);
        assertFalse(future2.isDone());
        assertSame(future2, progressImpl.futureFor(PAGES_SNAPSHOT_TAKEN));

        CompletableFuture<?> future3 = progressImpl.futureFor(LOCK_RELEASED);

        assertNotNull(future3);
        assertFalse(future3.isDone());
        assertSame(future3, progressImpl.futureFor(LOCK_RELEASED));

        CompletableFuture<?> future4 = progressImpl.futureFor(PAGES_SORTED);

        assertNotNull(future4);
        assertFalse(future4.isDone());
        assertSame(future4, progressImpl.futureFor(PAGES_SORTED));

        CompletableFuture<?> future5 = progressImpl.futureFor(FINISHED);

        assertNotNull(future5);
        assertFalse(future5.isDone());
        assertSame(future5, progressImpl.futureFor(FINISHED));

        progressImpl.transitTo(LOCK_RELEASED);

        assertDoesNotThrow(() -> future0.get(100, TimeUnit.MILLISECONDS));
        assertDoesNotThrow(() -> future1.get(100, TimeUnit.MILLISECONDS));
        assertDoesNotThrow(() -> future2.get(100, TimeUnit.MILLISECONDS));
        assertDoesNotThrow(() -> future3.get(100, TimeUnit.MILLISECONDS));

        assertFalse(future4.isDone());
        assertFalse(future5.isDone());

        assertSame(future0, progressImpl.futureFor(SCHEDULED));
        assertSame(future1, progressImpl.futureFor(LOCK_TAKEN));
        assertSame(future2, progressImpl.futureFor(PAGES_SNAPSHOT_TAKEN));
        assertSame(future3, progressImpl.futureFor(LOCK_RELEASED));
        assertSame(future4, progressImpl.futureFor(PAGES_SORTED));
        assertSame(future5, progressImpl.futureFor(FINISHED));
    }

    @Test
    void testTransitTo() {
        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);

        assertEquals(SCHEDULED, progressImpl.state());

        // Transit to LOCK_TAKEN.

        progressImpl.transitTo(LOCK_TAKEN);
        assertEquals(LOCK_TAKEN, progressImpl.state());

        progressImpl.transitTo(SCHEDULED);
        assertEquals(LOCK_TAKEN, progressImpl.state());

        // Transit to PAGE_SNAPSHOT_TAKEN.

        progressImpl.transitTo(PAGES_SNAPSHOT_TAKEN);
        assertEquals(PAGES_SNAPSHOT_TAKEN, progressImpl.state());

        progressImpl.transitTo(LOCK_TAKEN);
        assertEquals(PAGES_SNAPSHOT_TAKEN, progressImpl.state());

        progressImpl.transitTo(SCHEDULED);
        assertEquals(PAGES_SNAPSHOT_TAKEN, progressImpl.state());

        // Transit to LOCK_RELEASED.

        progressImpl.transitTo(LOCK_RELEASED);
        assertEquals(LOCK_RELEASED, progressImpl.state());

        progressImpl.transitTo(PAGES_SNAPSHOT_TAKEN);
        assertEquals(LOCK_RELEASED, progressImpl.state());

        progressImpl.transitTo(LOCK_TAKEN);
        assertEquals(LOCK_RELEASED, progressImpl.state());

        progressImpl.transitTo(SCHEDULED);
        assertEquals(LOCK_RELEASED, progressImpl.state());

        // Transit to PAGES_SORTED.

        progressImpl.transitTo(PAGES_SORTED);
        assertEquals(PAGES_SORTED, progressImpl.state());

        progressImpl.transitTo(LOCK_RELEASED);
        assertEquals(PAGES_SORTED, progressImpl.state());

        progressImpl.transitTo(PAGES_SNAPSHOT_TAKEN);
        assertEquals(PAGES_SORTED, progressImpl.state());

        progressImpl.transitTo(LOCK_TAKEN);
        assertEquals(PAGES_SORTED, progressImpl.state());

        progressImpl.transitTo(SCHEDULED);
        assertEquals(PAGES_SORTED, progressImpl.state());

        // Transit to FINISHED.

        progressImpl.transitTo(FINISHED);
        assertEquals(FINISHED, progressImpl.state());

        progressImpl.transitTo(PAGES_SORTED);
        assertEquals(FINISHED, progressImpl.state());

        progressImpl.transitTo(LOCK_RELEASED);
        assertEquals(FINISHED, progressImpl.state());

        progressImpl.transitTo(PAGES_SNAPSHOT_TAKEN);
        assertEquals(FINISHED, progressImpl.state());

        progressImpl.transitTo(LOCK_TAKEN);
        assertEquals(FINISHED, progressImpl.state());

        progressImpl.transitTo(SCHEDULED);
        assertEquals(FINISHED, progressImpl.state());
    }

    @Test
    void testPagesToWrite() {
        CheckpointProgressImpl progressImpl = new CheckpointProgressImpl(0);

        assertNull(progressImpl.pagesToWrite());

        progressImpl.pagesToWrite(CheckpointDirtyPages.EMPTY);

        assertSame(CheckpointDirtyPages.EMPTY, progressImpl.pagesToWrite());

        progressImpl.pagesToWrite(null);

        assertNull(progressImpl.pagesToWrite());
    }
}
