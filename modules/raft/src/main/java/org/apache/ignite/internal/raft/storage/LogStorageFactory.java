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

package org.apache.ignite.internal.raft.storage;

import java.util.Set;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;

/** Log storage factory interface. */
// TODO https://issues.apache.org/jira/browse/IGNITE-22766
public interface LogStorageFactory extends LogSyncer, IgniteComponent {
    /**
     * Creates a log storage.
     *
     * @param uri Log storage URI.
     * @param raftOptions Raft options.
     * @return Log storage.
     */
    LogStorage createLogStorage(String uri, RaftOptions raftOptions);

    /**
     * Destroys a log storage (that is, removes it from the disk).
     *
     * @param uri Log storage URI.
     */
    void destroyLogStorage(String uri);

    /**
     * Obtains group IDs for storage of all Raft groups existing on disk.
     *
     * <p>This method should only be called when the log storage is not accessed otherwise (so no Raft groups can appear or be destroyed
     * in parallel with this call).
     */
    Set<String> raftNodeStorageIdsOnDisk();
}
