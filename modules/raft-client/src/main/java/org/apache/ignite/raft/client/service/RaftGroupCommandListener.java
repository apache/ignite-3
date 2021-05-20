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

package org.apache.ignite.raft.client.service;

import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;

/**
 * A listener for replication group commands.
 */
public interface RaftGroupCommandListener {
    /**
     * @param iterator Read command iterator.
     */
    void onRead(Iterator<CommandClosure<ReadCommand>> iterator);

    /**
     * @param iterator Write command iterator.
     */
    void onWrite(Iterator<CommandClosure<WriteCommand>> iterator);

    /**
     * The callback to save current snapshot.
     * @param path Snapshot directory to store data.
     * @param doneClo The closure to call on finish. Pass TRUE if snapshot was taken normally, FALSE otherwise.
     */
    public void onSnapshotSave(String path, Consumer<Boolean> doneClo);

    /**
     * The callback to load a snapshot.
     * @param path Snapshot directory.
     * @return {@code True} if the snapshot was saved successfully.
     */
    boolean onSnapshotLoad(String path);
}
