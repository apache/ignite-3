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

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Strategy for selecting which segment files to compact during garbage collection.
 *
 * <p>Each implementation is fully responsible for discovering and ordering compaction candidates. This includes
 * determining which files are eligible (e.g. skipping files that have not yet been checkpointed) and ranking them
 * by priority.
 *
 * <p>The GC consumes candidates from the stream in order, compacting each one, and stops as soon as the log size
 * drops below the soft limit or the stream is exhausted.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
interface SegmentFileCompactionStrategy {
    /**
     * Returns an ordered stream of segment file candidates for compaction. The stream must be closed by the caller.
     */
    Stream<FileProperties> selectCandidates() throws IOException;
}
