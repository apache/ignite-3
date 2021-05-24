/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.server;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;
import org.apache.ignite.raft.jraft.counter.snapshot.CounterSnapshotFile;
import org.apache.ignite.raft.jraft.util.Utils;

/**
 * TODO asch support for batch updates.
 */
public class CounterCommandListener implements RaftGroupCommandListener {
    /** */
    private static final IgniteLogger LOG = IgniteLogger.forClass(CounterCommandListener.class);

    /** */
    private AtomicLong counter = new AtomicLong();

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            assert clo.command() instanceof GetValueCommand;

            clo.result(counter.get());
        }
    }

    /** {@inheritDoc} */
    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            IncrementAndGetCommand cmd0 = (IncrementAndGetCommand) clo.command();

            clo.result(counter.addAndGet(cmd0.delta()));
        }
    }

    /** {@inheritDoc} */
    @Override public void onSnapshotSave(String path, Consumer<Boolean> doneClo) {
        final long currVal = this.counter.get();

        // TODO asch use executor.
        Utils.runInThread(() -> {
            final CounterSnapshotFile snapshot = new CounterSnapshotFile(path + File.separator + "data");
            if (snapshot.save(currVal))
                doneClo.accept(Boolean.TRUE);
            else
                doneClo.accept(Boolean.FALSE);
        });
    }

    /** {@inheritDoc} */
    @Override public boolean onSnapshotLoad(String path) {
        final CounterSnapshotFile snapshot = new CounterSnapshotFile(path + File.separator + "data");
        try {
            this.counter.set(snapshot.load());
            return true;
        }
        catch (final IOException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }
    }

    /**
     * @return Current value.
     */
    public long value() {
        return counter.get();
    }
}
