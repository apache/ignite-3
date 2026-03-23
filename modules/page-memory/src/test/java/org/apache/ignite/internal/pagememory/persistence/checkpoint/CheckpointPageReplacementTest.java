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

import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.dirtyFullPageId;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/** For {@link CheckpointPageReplacement} testing. */
public class CheckpointPageReplacementTest {
    @Test
    void testBlock() {
        var checkpointPageReplacement = new CheckpointPageReplacement();

        assertDoesNotThrow(() -> checkpointPageReplacement.block(dirtyFullPageId(0, 0)));
        assertDoesNotThrow(() -> checkpointPageReplacement.block(dirtyFullPageId(0, 1)));

        checkpointPageReplacement.stopBlocking();
    }

    @Test
    void testUnblock() {
        var checkpointPageReplacement = new CheckpointPageReplacement();

        checkpointPageReplacement.block(dirtyFullPageId(0, 0));
        checkpointPageReplacement.block(dirtyFullPageId(0, 1));
        checkpointPageReplacement.block(dirtyFullPageId(0, 2));
        checkpointPageReplacement.block(dirtyFullPageId(0, 3));
        checkpointPageReplacement.block(dirtyFullPageId(0, 4));
        checkpointPageReplacement.block(dirtyFullPageId(0, 5));

        assertDoesNotThrow(() -> checkpointPageReplacement.unblock(dirtyFullPageId(0, 0), null));
        assertDoesNotThrow(() -> checkpointPageReplacement.unblock(dirtyFullPageId(0, 1), new Throwable("from test 0")));
        assertDoesNotThrow(() -> checkpointPageReplacement.unblock(dirtyFullPageId(0, 2), new Throwable("from test 1")));

        checkpointPageReplacement.stopBlocking();

        assertDoesNotThrow(() -> checkpointPageReplacement.unblock(dirtyFullPageId(0, 3), null));
        assertDoesNotThrow(() -> checkpointPageReplacement.unblock(dirtyFullPageId(0, 4), new Throwable("from test 0")));
        assertDoesNotThrow(() -> checkpointPageReplacement.unblock(dirtyFullPageId(0, 5), new Throwable("from test 1")));
    }

    @Test
    void testStopBlocking() {
        var checkpointPageReplacement = new CheckpointPageReplacement();

        checkpointPageReplacement.block(dirtyFullPageId(0, 0));
        checkpointPageReplacement.block(dirtyFullPageId(0, 1));

        CompletableFuture<Void> stopBlockingFuture = checkpointPageReplacement.stopBlocking();
        assertFalse(stopBlockingFuture.isDone());

        checkpointPageReplacement.unblock(dirtyFullPageId(0, 0), null);
        assertFalse(stopBlockingFuture.isDone());

        checkpointPageReplacement.unblock(dirtyFullPageId(0, 1), null);
        assertTrue(stopBlockingFuture.isDone());

        assertTrue(checkpointPageReplacement.stopBlocking().isDone());
    }

    @Test
    void testStopBlockingError() {
        var checkpointPageReplacement = new CheckpointPageReplacement();

        checkpointPageReplacement.block(dirtyFullPageId(0, 0));
        checkpointPageReplacement.block(dirtyFullPageId(0, 1));

        CompletableFuture<Void> stopBlockingFuture = checkpointPageReplacement.stopBlocking();
        assertFalse(stopBlockingFuture.isDone());

        checkpointPageReplacement.unblock(dirtyFullPageId(0, 0), new RuntimeException("from test 1"));
        assertThat(stopBlockingFuture, willThrow(RuntimeException.class, "from test 1"));

        checkpointPageReplacement.unblock(dirtyFullPageId(0, 1), new TimeoutException("from test 2"));
        assertThat(stopBlockingFuture, willThrow(RuntimeException.class, "from test 1"));

        assertThat(checkpointPageReplacement.stopBlocking(), willThrow(RuntimeException.class, "from test 1"));
    }

    @Test
    void testStopBlockingNoPageReplacement() {
        assertTrue(new CheckpointPageReplacement().stopBlocking().isDone());
    }
}
