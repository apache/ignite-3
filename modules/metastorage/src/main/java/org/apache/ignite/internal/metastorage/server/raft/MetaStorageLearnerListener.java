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

package org.apache.ignite.internal.metastorage.server.raft;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;

/**
 * Meta Storage Raft group listener for learner nodes. These nodes ignore read and cursor-related commands.
 */
public class MetaStorageLearnerListener implements RaftGroupListener {
    private final KeyValueStorage storage;

    private final MetaStorageWriteHandler writeHandler;

    public MetaStorageLearnerListener(KeyValueStorage storage) {
        this.storage = storage;
        this.writeHandler = new MetaStorageWriteHandler(storage);
    }

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        throw new UnsupportedOperationException("Reads should not be sent to learners");
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iter) {
        while (iter.hasNext()) {
            CommandClosure<WriteCommand> clo = iter.next();

            if (!writeHandler.handleWriteCommand(clo)) {
                // Ignore all commands that are not handled by the writeHandler.
                clo.result(null);
            }
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.snapshot(path).whenComplete((unused, throwable) -> doneClo.accept(throwable));
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        storage.restoreSnapshot(path);
        return true;
    }

    @Override
    public void onShutdown() {
    }
}
