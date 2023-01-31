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

package org.apache.ignite.raft.server.snasphot;


import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.raft.TestWriteCommand;

/**
 * The RAFT state machine counts applied write commands and stores the result into {@link java.util.concurrent.atomic.AtomicLong} that is
 * passed through constructor.
 * {@see org.apache.ignite.raft.locked.TestWriteCommand}
 */
public class UpdateCountRaftListener implements RaftGroupListener {
    /** The logger. */
    private static IgniteLogger LOG = Loggers.forClass(UpdateCountRaftListener.class);

    /** Counter of received updates. */
    private final AtomicInteger counter;

    /** Storage to persist a state of the listener on snapshot. */
    private final Map<Path, Integer> snapshotDataStorage;

    /**
     * The constructor.
     *
     * @param counter             Counter to store amount of updates.
     * @param snapshotDataStorage Storage for a snapshot.
     */
    public UpdateCountRaftListener(AtomicInteger counter, Map<Path, Integer> snapshotDataStorage) {
        this.counter = counter;
        this.snapshotDataStorage = snapshotDataStorage;
    }

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        throw new UnsupportedOperationException("Read command is not supported for the RAFT listener.");
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            assert clo.command() instanceof TestWriteCommand;

            int current = counter.incrementAndGet();

            LOG.info("Increment value [curVal={}]", current);

            clo.result(null);
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        snapshotDataStorage.put(path, counter.get());

        doneClo.accept(null);
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        counter.set(snapshotDataStorage.getOrDefault(path, 0));

        return true;
    }

    @Override
    public void onShutdown() {
    }
}
