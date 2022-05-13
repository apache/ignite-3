/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.ignite.lang.IgniteLogger;
import org.junit.jupiter.api.Test;

/**
 * For {@link Checkpointer} testing.
 */
public class CheckpointerTest {
    private final IgniteLogger log = IgniteLogger.forClass(CheckpointerTest.class);

    @Test
    void testStartAndStop() throws Exception {
        Checkpointer checkpointer = new Checkpointer(
                log, "test",
                null,
                null,
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                1,
                () -> 1_000
        );

        assertNull(checkpointer.runner());

        assertThat(checkpointer.runner(), nullValue());

        checkpointer.start();

        assertTrue(waitForCondition(() -> checkpointer.runner() != null, 10, 100));

        checkpointer.stop();

        assertTrue(checkpointer.isShutdownNow());
        assertTrue(checkpointer.isCancelled());
        assertTrue(checkpointer.isDone());
    }

    @Test
    void testScheduleCheckpoint() {
        Checkpointer checkpointer = new Checkpointer(
                log, "test",
                null,
                null,
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                1,
                () -> 100
        );

        assertNull(checkpointer.currentProgress());

        // TODO: continue
    }
}
