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

package org.apache.ignite.internal.metastorage;

/**
 * Listener which receives and handles the metastorage compaction revision update after watches have been started.
 *
 * <p>It is guaranteed that it will <b>not</b> be invoked in parallel with the handling of watch events or metastore revision update
 * events, and it will also grow monotonously without duplicates.</p>
 */
@FunctionalInterface
public interface CompactionRevisionUpdateListener {
    /**
     * Invoked when the metastorage compaction revision has been updated in the WatchEvent queue.
     *
     * @param compactionRevision New metastorage compaction revision.
     */
    void onUpdate(long compactionRevision);
}
