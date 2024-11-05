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

package org.apache.ignite.internal.metastorage.server;

import org.apache.ignite.internal.metastorage.Revisions;

/** Listener update of {@link Revisions current metastorage revisions}, needed only for metastorage recovery. */
@FunctionalInterface
public interface RecoveryRevisionsListener {
    /**
     * Invoked when one of the {@link Revisions current metastorage revisions} is updated.
     *
     * <p>Until the method completes its execution, no update or compaction of metastorage will occur, so the method should complete its
     * execution as soon as possible.</p>
     */
    void onUpdate(Revisions currentRevisions);
}
