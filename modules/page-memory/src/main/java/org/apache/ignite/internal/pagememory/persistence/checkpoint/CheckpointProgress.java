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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Represents information of progress of a current checkpoint and allows obtaining future to wait for a particular checkpoint state.
 */
public interface CheckpointProgress {
    /**
     * Returns checkpoint ID.
     */
    UUID id();

    /**
     * Returns description of the reason of the current checkpoint.
     */
    @Nullable String reason();

    /**
     * Return {@code true} If checkpoint already started but have not finished yet.
     */
    boolean inProgress();

    /**
     * Returns future which can be used for detection when current checkpoint reaches the specific state.
     */
    CompletableFuture<Void> futureFor(CheckpointState state);

    /**
     * Returns number of dirty pages in current checkpoint. If checkpoint is not running, returns {@code 0}.
     */
    int currentCheckpointPagesCount();

    /**
     * Returns the sorted dirty pages to be written on the checkpoint, {@code null} if there were no dirty pages, or they have already been
     * written.
     */
    @Nullable CheckpointDirtyPages pagesToWrite();
}
