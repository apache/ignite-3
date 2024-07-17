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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.junit.jupiter.api.Test;

/**
 * For {@link CheckpointPages} testing.
 */
public class CheckpointPagesTest {
    @Test
    void testContains() {
        CheckpointPages checkpointPages = new CheckpointPages(
                Set.of(new FullPageId(0, 0), new FullPageId(1, 0)),
                nullCompletedFuture()
        );

        assertTrue(checkpointPages.contains(new FullPageId(0, 0)));
        assertTrue(checkpointPages.contains(new FullPageId(1, 0)));

        assertFalse(checkpointPages.contains(new FullPageId(2, 0)));
        assertFalse(checkpointPages.contains(new FullPageId(3, 0)));
    }

    @Test
    void testSize() {
        CheckpointPages checkpointPages = new CheckpointPages(
                Set.of(new FullPageId(0, 0), new FullPageId(1, 0)),
                nullCompletedFuture()
        );

        assertEquals(2, checkpointPages.size());
    }

    @Test
    void testMarkAsSaved() {
        CheckpointPages checkpointPages = new CheckpointPages(
                new HashSet<>(Set.of(new FullPageId(0, 0), new FullPageId(1, 0), new FullPageId(2, 0))),
                nullCompletedFuture()
        );

        assertTrue(checkpointPages.markAsSaved(new FullPageId(0, 0)));
        assertFalse(checkpointPages.contains(new FullPageId(0, 0)));
        assertEquals(2, checkpointPages.size());

        assertFalse(checkpointPages.markAsSaved(new FullPageId(0, 0)));
        assertFalse(checkpointPages.contains(new FullPageId(0, 0)));
        assertEquals(2, checkpointPages.size());

        assertTrue(checkpointPages.markAsSaved(new FullPageId(1, 0)));
        assertFalse(checkpointPages.contains(new FullPageId(0, 0)));
        assertEquals(1, checkpointPages.size());
    }

    @Test
    void testAllowToSave() throws Exception {
        Set<FullPageId> pages = Set.of(new FullPageId(0, 0), new FullPageId(1, 0), new FullPageId(2, 0));

        CheckpointPages checkpointPages = new CheckpointPages(pages, nullCompletedFuture());

        assertTrue(checkpointPages.allowToSave(new FullPageId(0, 0)));
        assertTrue(checkpointPages.allowToSave(new FullPageId(1, 0)));
        assertTrue(checkpointPages.allowToSave(new FullPageId(2, 0)));

        assertFalse(checkpointPages.allowToSave(new FullPageId(3, 0)));

        IgniteInternalCheckedException exception = assertThrows(
                IgniteInternalCheckedException.class,
                () -> new CheckpointPages(pages, failedFuture(new Exception("test"))).allowToSave(new FullPageId(0, 0))
        );

        assertThat(exception.getCause(), instanceOf(Exception.class));
        assertThat(exception.getCause().getMessage(), equalTo("test"));

        exception = assertThrows(
                IgniteInternalCheckedException.class,
                () -> {
                    CompletableFuture<Object> future = new CompletableFuture<>();

                    future.cancel(true);

                    new CheckpointPages(pages, future).allowToSave(new FullPageId(0, 0));
                }
        );

        assertThat(exception.getCause(), instanceOf(CancellationException.class));
    }
}
