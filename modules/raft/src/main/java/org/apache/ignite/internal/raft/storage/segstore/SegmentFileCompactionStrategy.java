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

package org.apache.ignite.internal.raft.storage.segstore;

import org.jetbrains.annotations.Nullable;

/**
 * Strategy for selecting which segment file to compact during garbage collection.
 *
 * <p>Each implementation is fully responsible for discovering and evaluating compaction candidates. This includes
 * determining which files are eligible (e.g. skipping the currently-written segment file, skipping files that have not
 * yet been checkpointed).
 */
@FunctionalInterface
interface SegmentFileCompactionStrategy {
    /**
     * Selects the next segment file to compact.
     *
     * @return File properties of the selected candidate, or {@code null} if no compaction should be performed right now.
     */
    @Nullable FileProperties selectSegmentFileForCompaction();
}
